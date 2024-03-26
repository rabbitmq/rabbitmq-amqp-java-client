// Copyright (c) 2024 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.
package com.rabbitmq.model.amqp;

import static com.rabbitmq.model.amqp.UriUtils.encodeHttpParameter;
import static com.rabbitmq.model.amqp.UriUtils.encodePathSegment;
import static com.rabbitmq.model.amqp.Utils.newThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.rabbitmq.model.Management;
import com.rabbitmq.model.ModelException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpManagement implements Management {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpManagement.class);

  private static final String MANAGEMENT_NODE_ADDRESS = "/management";
  private static final String REPLY_TO = "$me";

  private static final String GET = "GET";
  private static final String POST = "POST";
  private static final String PUT = "PUT";
  private static final String DELETE = "DELETE";
  private static final String CODE_200 = "200";
  private static final String CODE_201 = "201";
  private static final String CODE_204 = "204";

  private final Session session;
  private final Sender sender;
  private final Receiver receiver;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Duration rpcTimeout = Duration.ofSeconds(10);
  private final ConcurrentMap<UUID, OutstandingRequest> outstandingRequests =
      new ConcurrentHashMap<>();
  private final Thread receiveLoop;
  private final TopologyListener topologyListener;

  AmqpManagement(AmqpManagementParameters parameters) {
    try {
      this.session = parameters.connection().nativeConnection().openSession();
      String linkPairName = "management-link-pair";
      Map<String, Object> properties = Collections.singletonMap("paired", Boolean.TRUE);
      this.sender =
          session.openSender(
              MANAGEMENT_NODE_ADDRESS,
              new SenderOptions()
                  .deliveryMode(DeliveryMode.AT_MOST_ONCE)
                  .linkName(linkPairName)
                  .properties(properties));

      this.receiver =
          session.openReceiver(
              MANAGEMENT_NODE_ADDRESS,
              new ReceiverOptions()
                  .deliveryMode(DeliveryMode.AT_MOST_ONCE)
                  .linkName(linkPairName)
                  .properties(properties)
                  .creditWindow(100));

      this.sender.openFuture().get(this.rpcTimeout.toMillis(), MILLISECONDS);
      this.receiver.openFuture().get(this.rpcTimeout.toMillis(), MILLISECONDS);
      Runnable receiveTask =
          () -> {
            try {
              while (!Thread.currentThread().isInterrupted()) {
                Delivery delivery = receiver.receive(100, MILLISECONDS);
                if (delivery != null) {
                  Object correlationId = delivery.message().correlationId();
                  if (correlationId instanceof UUID) {
                    OutstandingRequest request = outstandingRequests.remove(correlationId);
                    if (request != null) {
                      request.complete(delivery.message());
                    } else {
                      LOGGER.info("Could not find outstanding request {}", correlationId);
                    }
                  } else {
                    LOGGER.info("Could not correlate inbound message with managemement request");
                  }
                }
              }
            } catch (ClientConnectionRemotelyClosedException
                | ClientLinkRemotelyClosedException e) {
              // receiver is closed
            } catch (ClientException e) {
              java.util.function.Consumer<String> log =
                  this.closed.get() ? m -> LOGGER.debug(m, e) : m -> LOGGER.warn(m, e);
              log.accept("Error while polling AMQP receiver");
            }
          };
      this.receiveLoop =
          newThread(
              "rabbitmq-amqp-management-consumer-" + ID_SEQUENCE.getAndIncrement(), receiveTask);
      this.receiveLoop.start();
      this.topologyListener =
          parameters.topologyListener() == null
              ? TopologyListener.NO_OP
              : parameters.topologyListener();
    } catch (Exception e) {
      throw new ModelException(e);
    }
  }

  @Override
  public QueueSpecification queue() {
    return new AmqpQueueSpecification(this);
  }

  @Override
  public QueueDeletion queueDeletion() {
    return name -> {
      this.topologyListener.queueDeleted(name);
      Map<String, Object> responseBody = delete(queueLocation(name), CODE_200);
      if (!responseBody.containsKey("message_count")) {
        throw new ModelException("Response body should contain message_count");
      }
    };
  }

  @Override
  public ExchangeSpecification exchange() {
    return new AmqpExchangeSpecification(this);
  }

  @Override
  public ExchangeDeletion exchangeDeletion() {
    return name -> {
      this.topologyListener.exchangeDeleted(name);
      this.delete(exchangeLocation(name), CODE_204);
    };
  }

  @Override
  public BindingSpecification binding() {
    return new AmqpBindingManagement.AmqpBindingSpecification(this);
  }

  @Override
  public UnbindSpecification unbind() {
    return new AmqpBindingManagement.AmqpUnbindSpecification(this);
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.releaseResources();
      this.receiver.close();
      this.sender.close();
      this.session.close();
    }
  }

  void releaseResources() {
    if (this.receiveLoop != null) {
      this.receiveLoop.interrupt();
    }
  }

  boolean isOpen() {
    return !this.closed.get();
  }

  void declareQueue(String name, Map<String, Object> body) {
    this.declare(body, queueLocation(name), List.of(CODE_200, CODE_201));
  }

  void declareExchange(String name, Map<String, Object> body) {
    this.declare(body, exchangeLocation(name), List.of(CODE_204));
  }

  private Map<String, Object> declare(
      Map<String, Object> body, String target, Collection<String> expectedResponseCodes) {
    return this.declare(body, target, PUT, expectedResponseCodes);
  }

  private Map<String, Object> declare(
      Map<String, Object> body,
      String target,
      String operation,
      Collection<String> expectedResponseCodes) {
    UUID requestId = messageId();
    try {
      Message<?> request =
          Message.create(body).messageId(requestId).to(target).subject(operation).replyTo(REPLY_TO);

      OutstandingRequest outstandingRequest = this.request(request);
      outstandingRequest.block();

      checkResponse(outstandingRequest.response(), requestId, expectedResponseCodes);
      return outstandingRequest.responseBodyAsMap();
    } catch (ClientException e) {
      throw new ModelException("Error on PUT operation: " + target, e);
    }
  }

  OutstandingRequest request(Message<?> request) throws ClientException {
    OutstandingRequest outstandingRequest = new OutstandingRequest(this.rpcTimeout);
    this.outstandingRequests.put((UUID) request.messageId(), outstandingRequest);
    this.sender.send(request);
    return outstandingRequest;
  }

  private Map<String, Object> delete(String target, String expectedResponseCode) {
    UUID requestId = messageId();
    try {
      Message<?> request =
          Message.create((Map<?, ?>) null)
              .messageId(requestId)
              .to(target)
              .subject(DELETE)
              .replyTo(REPLY_TO);

      OutstandingRequest outstandingRequest = request(request);
      outstandingRequest.block();
      checkResponse(outstandingRequest.response(), requestId, List.of(expectedResponseCode));
      return outstandingRequest.responseBodyAsMap();
    } catch (ClientException e) {
      throw new ModelException("Error on DELETE operation: " + target, e);
    }
  }

  private static UUID messageId() {
    return UUID.randomUUID();
  }

  private static String queueLocation(String q) {
    return "/queues/" + encodePathSegment(q);
  }

  private static String exchangeLocation(String e) {
    return "/exchanges/" + encodePathSegment(e);
  }

  private static void checkResponse(
      Message<?> response, UUID requestId, Collection<String> expectedResponseCodes)
      throws ClientException {
    if (!requestId.equals(response.correlationId())) {
      throw new ModelException("Unexpected correlation ID");
    }
    if (!expectedResponseCodes.contains(response.subject())) {
      throw new ModelException(
          "Unexpected response code: %s instead of %s",
          response.subject(), String.join(" or ", expectedResponseCodes));
    }
  }

  void bind(Map<String, Object> body) {
    declare(body, "/bindings", POST, List.of(CODE_204));
  }

  void unbind(
      String destinationField,
      String source,
      String destination,
      String key,
      Map<String, Object> arguments) {
    if (arguments == null || arguments.isEmpty()) {
      String target =
          "/bindings/"
              + "src="
              + encodePathSegment(source)
              + ";"
              + destinationField
              + "="
              + encodePathSegment(destination)
              + ";key="
              + encodePathSegment(key)
              + ";args=";
      delete(target, CODE_204);
    } else {
      List<Map<String, Object>> bindings;
      String target = bindingsTarget(destinationField, source, destination, key);
      try {
        bindings = get(target).responseBodyAsList();
      } catch (ClientException e) {
        throw new ModelException("Error on GET operation: " + target, e);
      }
      matchBinding(bindings, key, arguments).ifPresent(location -> delete(location, CODE_204));
    }
  }

  private static Optional<String> matchBinding(
      List<Map<String, Object>> bindings, String key, Map<String, Object> arguments) {
    Optional<String> uri;
    if (!bindings.isEmpty()) {
      uri =
          bindings.stream()
              .filter(
                  binding -> {
                    String bindingKey = (String) binding.get("binding_key");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> bindingArguments =
                        (Map<String, Object>) binding.get("arguments");
                    if (key == null && bindingKey == null
                        || key != null && key.equals(bindingKey)) {
                      return arguments == null && bindingArguments == null
                          || arguments != null && arguments.equals(bindingArguments);
                    }
                    return false;
                  })
              .map(b -> b.get("location").toString())
              .findFirst();
    } else {
      uri = Optional.empty();
    }
    return uri;
  }

  private OutstandingRequest get(String target) throws ClientException {
    UUID requestId = messageId();
    Message<?> request =
        Message.create((Map<?, ?>) null)
            .messageId(requestId)
            .to(target)
            .subject(GET)
            .replyTo(REPLY_TO);

    OutstandingRequest outstandingRequest = request(request);
    outstandingRequest.block();
    checkResponse(outstandingRequest.response(), requestId, List.of(CODE_200));
    return outstandingRequest;
  }

  private String bindingsTarget(
      String destinationField, String source, String destination, String key) {
    return "/bindings?src="
        + encodeHttpParameter(source)
        + "&"
        + destinationField
        + "="
        + encodeHttpParameter(destination)
        + "&key="
        + encodeHttpParameter(key);
  }

  private static class OutstandingRequest {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicReference<Message<?>> response = new AtomicReference<>();
    private final Duration timeout;

    private OutstandingRequest(Duration timeout) {
      this.timeout = timeout;
    }

    void block() {
      boolean completed;
      try {
        completed = this.latch.await(timeout.toMillis(), MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ModelException("Interrupted while waiting for management response");
      }
      if (!completed) {
        throw new ModelException("Could not get management response in %d ms", timeout.toMillis());
      }
    }

    void complete(Message<?> response) {
      this.response.set(response);
      this.latch.countDown();
    }

    Message<?> response() {
      return this.response.get();
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> responseBodyAsMap() throws ClientException {
      return (Map<K, V>) this.response.get().body();
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> responseBodyAsList() throws ClientException {
      return (List<T>) this.response.get().body();
    }
  }

  TopologyListener recovery() {
    return this.topologyListener;
  }
}
