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
package com.rabbitmq.amqp.client.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.rabbitmq.amqp.client.Management;
import com.rabbitmq.amqp.client.ModelException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
  private static final int CODE_200 = 200;
  private static final int CODE_201 = 201;
  private static final int CODE_204 = 204;
  private static final int CODE_409 = 409;

  private final AmqpConnection connection;
  private final Long id;
  private volatile Session session;
  private volatile Sender sender;
  private volatile Receiver receiver;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Duration rpcTimeout = Duration.ofSeconds(10);
  private final ConcurrentMap<UUID, OutstandingRequest> outstandingRequests =
      new ConcurrentHashMap<>();
  private volatile Future<?> receiveLoop;
  private final TopologyListener topologyListener;
  private final Supplier<String> nameSupplier;

  AmqpManagement(AmqpManagementParameters parameters) {
    this.id = ID_SEQUENCE.getAndIncrement();
    this.connection = parameters.connection();
    this.topologyListener =
        parameters.topologyListener() == null
            ? TopologyListener.NO_OP
            : parameters.topologyListener();
    this.nameSupplier = parameters.nameSupplier();
  }

  @Override
  public QueueSpecification queue() {
    checkAvailable();
    return new AmqpQueueSpecification(this);
  }

  @Override
  public QueueSpecification queue(String name) {
    checkAvailable();
    return this.queue().name(name);
  }

  @Override
  public QueueInfo queueInfo(String name) {
    checkAvailable();
    try {
      Map<String, Object> queueInfo = get(queueLocation(name)).responseBodyAsMap();
      return new DefaultQueueInfo(queueInfo);
    } catch (ClientException e) {
      throw new ModelException("Error while fetching queue '%s' information", name);
    }
  }

  @Override
  public QueueDeletion queueDeletion() {
    checkAvailable();
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
    checkAvailable();
    return new AmqpExchangeSpecification(this);
  }

  @Override
  public ExchangeSpecification exchange(String name) {
    checkAvailable();
    return this.exchange().name(name);
  }

  @Override
  public ExchangeDeletion exchangeDeletion() {
    checkAvailable();
    return name -> {
      this.topologyListener.exchangeDeleted(name);
      this.delete(exchangeLocation(name), CODE_204);
    };
  }

  @Override
  public BindingSpecification binding() {
    checkAvailable();
    return new AmqpBindingManagement.AmqpBindingSpecification(this);
  }

  @Override
  public UnbindSpecification unbind() {
    checkAvailable();
    return new AmqpBindingManagement.AmqpUnbindSpecification(this);
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true) && this.initialized.get()) {
      this.releaseResources();
      try {
        this.receiver.close();
      } catch (Exception e) {
        LOGGER.debug("Error while closing management receiver: {}", e.getMessage());
      }
      try {
        this.sender.close();
      } catch (Exception e) {
        LOGGER.debug("Error while closing management sender: {}", e.getMessage());
      }
      try {
        this.session.close();
      } catch (Exception e) {
        LOGGER.debug("Error while closing management session: {}", e.getMessage());
      }
    }
  }

  void init() {
    if (this.initialized.compareAndSet(false, true)) {
      try {
        this.session = this.connection.nativeConnection().openSession();
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
        this.receiveLoop = this.connection.executorService().submit(receiveTask);
      } catch (Exception e) {
        throw new ModelException(e);
      }
    }
  }

  void releaseResources() {
    if (this.receiveLoop != null) {
      this.receiveLoop.cancel(true);
    }
    this.initialized.set(false);
  }

  QueueInfo declareQueue(String name, Map<String, Object> body) {
    if (name == null || name.isBlank()) {
      QueueInfo info = null;
      while (info == null) {
        name = this.nameSupplier.get();
        Response<Map<String, Object>> response =
            this.declare(body, queueLocation(name), CODE_200, CODE_201, CODE_409);
        if (response.code() == CODE_201) {
          info = new DefaultQueueInfo(response.body());
        }
      }
      return info;
    } else {
      return new DefaultQueueInfo(
          this.declare(body, queueLocation(name), CODE_200, CODE_201).body());
    }
  }

  void declareExchange(String name, Map<String, Object> body) {
    this.declare(body, exchangeLocation(name), CODE_204);
  }

  private Response<Map<String, Object>> declare(
      Map<String, Object> body, String target, int... expectedResponseCodes) {
    return this.declare(body, target, PUT, expectedResponseCodes);
  }

  private Response<Map<String, Object>> declare(
      Map<String, Object> body, String target, String operation, int... expectedResponseCodes) {
    checkAvailable();
    UUID requestId = messageId();
    try {
      Message<?> request =
          Message.create(body).messageId(requestId).to(target).subject(operation).replyTo(REPLY_TO);

      OutstandingRequest outstandingRequest = this.request(request);
      outstandingRequest.block();

      checkResponse(outstandingRequest, requestId, expectedResponseCodes);
      return outstandingRequest.mapResponse();
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

  private Map<String, Object> delete(String target, int expectedResponseCode) {
    checkAvailable();
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
      checkResponse(outstandingRequest, requestId, expectedResponseCode);
      return outstandingRequest.responseBodyAsMap();
    } catch (ClientException e) {
      throw new ModelException("Error on DELETE operation: " + target, e);
    }
  }

  private static UUID messageId() {
    return UUID.randomUUID();
  }

  private static String queueLocation(String q) {
    return "/queues/" + UriUtils.encodePathSegment(q);
  }

  private static String exchangeLocation(String e) {
    return "/exchanges/" + UriUtils.encodePathSegment(e);
  }

  private static void checkResponse(
      OutstandingRequest request, UUID requestId, int... expectedResponseCodes)
      throws ClientException {
    Message<?> response = request.responseMessage();
    if (!requestId.equals(response.correlationId())) {
      throw new ModelException("Unexpected correlation ID");
    }
    int responseCode = request.mapResponse().code();
    if (IntStream.of(expectedResponseCodes).noneMatch(c -> c == responseCode)) {
      throw new ModelException(
          "Unexpected response code: %d instead of %s",
          responseCode,
          IntStream.of(expectedResponseCodes)
              .mapToObj(String::valueOf)
              .collect(Collectors.joining(", ")));
    }
  }

  void bind(Map<String, Object> body) {
    declare(body, "/bindings", POST, CODE_204);
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
              + UriUtils.encodePathSegment(source)
              + ";"
              + destinationField
              + "="
              + UriUtils.encodePathSegment(destination)
              + ";key="
              + UriUtils.encodePathSegment(key)
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
    checkAvailable();
    UUID requestId = messageId();
    Message<?> request =
        Message.create((Map<?, ?>) null)
            .messageId(requestId)
            .to(target)
            .subject(GET)
            .replyTo(REPLY_TO);

    OutstandingRequest outstandingRequest = request(request);
    outstandingRequest.block();
    checkResponse(outstandingRequest, requestId, CODE_200);
    return outstandingRequest;
  }

  private String bindingsTarget(
      String destinationField, String source, String destination, String key) {
    return "/bindings?src="
        + UriUtils.encodeHttpParameter(source)
        + "&"
        + destinationField
        + "="
        + UriUtils.encodeHttpParameter(destination)
        + "&key="
        + UriUtils.encodeHttpParameter(key);
  }

  private static class OutstandingRequest {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicReference<Message<?>> responseMessage = new AtomicReference<>();
    private final AtomicReference<Response<?>> response = new AtomicReference<>();
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

    void complete(Message<?> response) throws ClientException {
      this.responseMessage.set(response);
      this.response.set(new Response<>(Integer.parseInt(response.subject()), response.body()));
      this.latch.countDown();
    }

    Message<?> responseMessage() {
      return this.responseMessage.get();
    }

    @SuppressWarnings("unchecked")
    private <K, V> Response<Map<K, V>> mapResponse() {
      return (Response<Map<K, V>>) this.response.get();
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> responseBodyAsMap() throws ClientException {
      return (Map<K, V>) this.responseMessage.get().body();
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> responseBodyAsList() throws ClientException {
      return (List<T>) this.responseMessage.get().body();
    }
  }

  TopologyListener recovery() {
    return this.topologyListener;
  }

  private static class DefaultQueueInfo implements QueueInfo {

    private final String name;
    private final boolean durable;
    private final boolean autoDelete;
    private final boolean exclusive;
    private final QueueType type;
    private final Map<String, Object> arguments;
    private final String leader;
    private final List<String> replicas;
    private final long messageCount;
    private final int consumerCount;

    @SuppressWarnings("unchecked")
    private DefaultQueueInfo(Map<String, Object> response) {
      this.name = (String) response.get("name");
      this.durable = (Boolean) response.get("durable");
      this.autoDelete = (Boolean) response.get("auto_delete");
      this.exclusive = (Boolean) response.get("exclusive");
      this.type = QueueType.valueOf(((String) response.get("type")).toUpperCase(Locale.ENGLISH));
      this.arguments = Collections.unmodifiableMap((Map<String, Object>) response.get("arguments"));
      this.leader = (String) response.get("leader");
      String[] replicas = (String[]) response.get("replicas");
      if (replicas == null || replicas.length == 0) {
        this.replicas = Collections.emptyList();
      } else {
        this.replicas = List.of(replicas);
      }
      this.messageCount = ((Number) response.get("message_count")).longValue();
      this.consumerCount = ((Number) response.get("consumer_count")).intValue();
    }

    @Override
    public String name() {
      return this.name;
    }

    @Override
    public boolean durable() {
      return this.durable;
    }

    @Override
    public boolean autoDelete() {
      return this.autoDelete;
    }

    @Override
    public boolean exclusive() {
      return this.exclusive;
    }

    @Override
    public QueueType type() {
      return this.type;
    }

    @Override
    public Map<String, Object> arguments() {
      return this.arguments;
    }

    @Override
    public String leader() {
      return this.leader;
    }

    @Override
    public List<String> replicas() {
      return this.replicas;
    }

    @Override
    public long messageCount() {
      return this.messageCount;
    }

    @Override
    public int consumerCount() {
      return this.consumerCount;
    }

    @Override
    public String toString() {
      return "DefaultQueueInfo{"
          + "name='"
          + name
          + '\''
          + ", durable="
          + durable
          + ", autoDelete="
          + autoDelete
          + ", exclusive="
          + exclusive
          + ", type="
          + type
          + ", arguments="
          + arguments
          + ", leader='"
          + leader
          + '\''
          + ", replicas="
          + replicas
          + ", messageCount="
          + messageCount
          + ", consumerCount="
          + consumerCount
          + '}';
    }
  }

  private void checkAvailable() {
    if (this.closed.get()) {
      throw new ModelException("Management is closed");
    } else if (!this.initialized.get()) {
      throw new ModelException("Management is not available");
    }
  }

  @Override
  public String toString() {
    return this.connection.toString() + "-" + this.id;
  }

  private static class Response<T> {

    private final int code;
    private final T body;

    private Response(int code, T body) {
      this.code = code;
      this.body = body;
    }

    int code() {
      return this.code;
    }

    T body() {
      return this.body;
    }
  }
}
