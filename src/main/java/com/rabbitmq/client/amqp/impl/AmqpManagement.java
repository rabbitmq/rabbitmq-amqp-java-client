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
package com.rabbitmq.client.amqp.impl;

import static com.rabbitmq.client.amqp.impl.AmqpManagement.State.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Management;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientSessionRemotelyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Follow the <a
 * href="https://github.com/oasis-tcs/amqp-specs/blob/master/http-over-amqp-v1.0-wd06a.docx">HTTP
 * Semantics and Content over AMQP Version 1.0</a> extension specification.
 *
 * @see <a
 *     href="https://github.com/oasis-tcs/amqp-specs/blob/master/http-over-amqp-v1.0-wd06a.docx">HTTP
 *     Semantics and Content over AMQP Version 1.0</a>
 */
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
  private static final int CODE_404 = 404;
  private static final int CODE_409 = 409;

  private final AmqpConnection connection;
  private final Long id;
  private volatile Session session;
  private volatile Sender sender;
  private volatile Receiver receiver;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Duration rpcTimeout = Duration.ofSeconds(10);
  private final ConcurrentMap<UUID, OutstandingRequest> outstandingRequests =
      new ConcurrentHashMap<>();
  private volatile Future<?> receiveLoop;
  private final TopologyListener topologyListener;
  private final Supplier<String> nameSupplier;
  private final AtomicReference<State> state = new AtomicReference<>(CREATED);
  private volatile boolean initializing = false;
  private final Lock initializationLock = new ReentrantLock();
  private final Duration receiveLoopIdleTimeout;
  private final Lock instanceLock = new ReentrantLock();

  AmqpManagement(AmqpManagementParameters parameters) {
    this.id = ID_SEQUENCE.getAndIncrement();
    this.connection = parameters.connection();
    this.topologyListener =
        parameters.topologyListener() == null
            ? TopologyListener.NO_OP
            : parameters.topologyListener();
    this.nameSupplier = parameters.nameSupplier();
    this.receiveLoopIdleTimeout =
        parameters.receiveLoopIdleTimeout() == null
            ? Duration.ofSeconds(20)
            : parameters.receiveLoopIdleTimeout();
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
      throw new AmqpException("Error while fetching queue '%s' information", name);
    }
  }

  @Override
  public QueueDeletion queueDeletion() {
    checkAvailable();
    return name -> {
      Map<String, Object> responseBody = delete(queueLocation(name), CODE_200);
      this.topologyListener.queueDeleted(name);
      if (!responseBody.containsKey("message_count")) {
        throw new AmqpException("Response body should contain message_count");
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
      this.delete(exchangeLocation(name), CODE_204);
      this.topologyListener.exchangeDeleted(name);
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
    if (this.initializing) {
      throw new AmqpException.AmqpResourceInvalidStateException(
          "Management is initializing, retry closing later.");
    }
    if (this.closed.compareAndSet(false, true)) {
      this.state(CLOSED);
      this.releaseResources();
      if (this.receiver != null) {
        try {
          this.receiver.close();
        } catch (Exception e) {
          LOGGER.debug("Error while closing management receiver: {}", e.getMessage());
        }
      }
      if (this.sender != null) {
        try {
          this.sender.close();
        } catch (Exception e) {
          LOGGER.debug("Error while closing management sender: {}", e.getMessage());
        }
      }
      if (this.session != null) {
        try {
          this.session.close();
        } catch (Exception e) {
          LOGGER.debug("Error while closing management session: {}", e.getMessage());
        }
      }
    }
  }

  void init() {
    if (this.state() != OPEN) {
      if (!this.initializing) {
        try {
          initializationLock.lock();
          if (!this.initializing && this.state() != OPEN) {
            this.initializing = true;
            LOGGER.debug("Initializing management ({}).", this);
            this.state(UNAVAILABLE);
            try {
              if (this.receiveLoop != null) {
                this.receiveLoop.cancel(true);
                this.receiveLoop = null;
              }
              LOGGER.debug("Creating management session ({}).", this);
              this.session = this.connection.nativeConnection().openSession();
              String linkPairName = "management-link-pair";
              Map<String, Object> properties = Collections.singletonMap("paired", Boolean.TRUE);
              LOGGER.debug("Creating management sender ({}).", this);
              this.sender =
                  session.openSender(
                      MANAGEMENT_NODE_ADDRESS,
                      new SenderOptions()
                          .deliveryMode(DeliveryMode.AT_MOST_ONCE)
                          .linkName(linkPairName)
                          .properties(properties));

              LOGGER.debug("Creating management receiver ({}).", this);
              this.receiver =
                  session.openReceiver(
                      MANAGEMENT_NODE_ADDRESS,
                      new ReceiverOptions()
                          .deliveryMode(DeliveryMode.AT_MOST_ONCE)
                          .linkName(linkPairName)
                          .properties(properties)
                          .creditWindow(100));

              this.sender.openFuture().get(this.rpcTimeout.toMillis(), MILLISECONDS);
              LOGGER.debug("Management sender created ({}).", this);
              this.receiver.openFuture().get(this.rpcTimeout.toMillis(), MILLISECONDS);
              LOGGER.debug("Management receiver created ({}).", this);
              this.state(OPEN);
              this.initializing = false;
            } catch (Exception e) {
              throw new AmqpException(e);
            }
          }
        } finally {
          initializationLock.unlock();
        }
      }
    }
  }

  private Runnable receiveTask() {
    return () -> {
      try {
        Duration waitDuration = Duration.ofMillis(100);
        long idleTime = 0;
        while (!Thread.currentThread().isInterrupted()) {
          Delivery delivery = receiver.receive(waitDuration.toMillis(), MILLISECONDS);
          if (delivery != null) {
            idleTime = 0;
            Object correlationId = delivery.message().correlationId();
            if (correlationId instanceof UUID) {
              OutstandingRequest request = outstandingRequests.remove(correlationId);
              if (request != null) {
                request.complete(delivery.message());
              } else {
                LOGGER.info("Could not find outstanding request {}", correlationId);
              }
            } else {
              LOGGER.info("Could not correlate inbound message with management request");
            }
          } else {
            idleTime += waitDuration.toMillis();
            if (idleTime > receiveLoopIdleTimeout.toMillis()) {
              LOGGER.debug(
                  "Management receive loop has been idle for more than {}, finishing it.",
                  this.receiveLoopIdleTimeout);
              this.receiveLoop = null;
              return;
            }
          }
        }
      } catch (ClientConnectionRemotelyClosedException | ClientLinkRemotelyClosedException e) {
        // receiver is closed
      } catch (ClientSessionRemotelyClosedException e) {
        this.state(UNAVAILABLE);
        LOGGER.info("Management session closed in receive loop: {} ({})", e.getMessage(), this);
        AmqpException exception = ExceptionUtils.convert(e);
        this.failRequests(exception);
        if (exception instanceof AmqpException.AmqpSecurityException) {
          LOGGER.debug(
              "Recovering AMQP management because the failure was a security exception ({}).",
              this);
          this.init();
        }
      } catch (ClientException e) {
        java.util.function.Consumer<String> log =
            this.closed.get() ? m -> LOGGER.debug(m, e) : m -> LOGGER.info(m, e);
        log.accept("Error while polling AMQP receiver");
      }
    };
  }

  private void failRequests(AmqpException exception) {
    Iterator<Map.Entry<UUID, OutstandingRequest>> iterator =
        this.outstandingRequests.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<UUID, OutstandingRequest> request = iterator.next();
      LOGGER.info("Failing management request {}", request.getKey());
      request.getValue().fail(exception);
      iterator.remove();
    }
  }

  void releaseResources() {
    this.markUnavailable();
    if (this.receiveLoop != null) {
      this.receiveLoop.cancel(true);
      this.receiveLoop = null;
    }
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
      Message<?> request = Message.create(body).to(target).subject(operation);

      OutstandingRequest outstandingRequest = this.request(request, requestId);
      outstandingRequest.block();

      checkResponse(outstandingRequest, requestId, expectedResponseCodes);
      return outstandingRequest.mapResponse();
    } catch (ClientException e) {
      throw new AmqpException("Error on PUT operation: " + target, e);
    }
  }

  OutstandingRequest request(Message<?> request, UUID requestId) throws ClientException {
    // HTTP over AMQP 1.0 extension specification, 5.1:
    // To associate a response with a request, the correlation-id value of the response properties
    // MUST be set to the message-id value of the request properties.
    request.messageId(requestId).replyTo(REPLY_TO);
    OutstandingRequest outstandingRequest = new OutstandingRequest(this.rpcTimeout);
    LOGGER.debug("Enqueueing request {}", requestId);
    this.outstandingRequests.put(requestId, outstandingRequest);
    LOGGER.debug("Sending request {}", requestId);
    this.sender.send(request);
    Future<?> loop = this.receiveLoop;
    if (loop == null) {
      this.instanceLock.lock();
      try {
        loop = this.receiveLoop;
        if (loop == null) {
          Runnable receiveTask = receiveTask();
          LOGGER.debug("Starting management receive loop ({}).", this);
          this.receiveLoop = this.connection.environment().executorService().submit(receiveTask);
          LOGGER.debug("Management initialized ({}).", this);
        }
      } finally {
        this.instanceLock.unlock();
      }
    }
    return outstandingRequest;
  }

  private Map<String, Object> delete(String target, int expectedResponseCode) {
    checkAvailable();
    UUID requestId = messageId();
    try {
      Message<?> request = Message.create((Map<?, ?>) null).to(target).subject(DELETE);

      OutstandingRequest outstandingRequest = request(request, requestId);
      outstandingRequest.block();
      checkResponse(outstandingRequest, requestId, expectedResponseCode);
      return outstandingRequest.responseBodyAsMap();
    } catch (ClientException e) {
      throw new AmqpException("Error on DELETE operation: " + target, e);
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
      throw new AmqpException("Unexpected correlation ID");
    }
    int responseCode = request.mapResponse().code();
    if (IntStream.of(expectedResponseCodes).noneMatch(c -> c == responseCode)) {
      if (responseCode == CODE_404) {
        throw new AmqpException.AmqpEntityDoesNotExistException("Entity does not exist");
      } else {
        throw new AmqpException(
            "Unexpected response code: %d instead of %s",
            responseCode,
            IntStream.of(expectedResponseCodes)
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(", ")));
      }
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
              + UriUtils.encodeNonUnreserved(source)
              + ";"
              + destinationField
              + "="
              + UriUtils.encodeNonUnreserved(destination)
              + ";key="
              + UriUtils.encodeNonUnreserved(key)
              + ";args=";
      delete(target, CODE_204);
    } else {
      List<Map<String, Object>> bindings;
      String target = bindingsTarget(destinationField, source, destination, key);
      try {
        bindings = get(target).responseBodyAsList();
      } catch (ClientException e) {
        throw new AmqpException("Error on GET operation: " + target, e);
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
    Message<?> request = Message.create((Map<?, ?>) null).to(target).subject(GET);

    OutstandingRequest outstandingRequest = request(request, requestId);
    outstandingRequest.block();
    checkResponse(outstandingRequest, requestId, CODE_200);
    return outstandingRequest;
  }

  private String bindingsTarget(
      String destinationField, String source, String destination, String key) {
    return "/bindings?src="
        + UriUtils.encodeParameter(source)
        + "&"
        + destinationField
        + "="
        + UriUtils.encodeParameter(destination)
        + "&key="
        + UriUtils.encodeParameter(key);
  }

  private static class OutstandingRequest {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicReference<Message<?>> responseMessage = new AtomicReference<>();
    private final AtomicReference<Response<?>> response = new AtomicReference<>();
    private final AtomicReference<AmqpException> exception = new AtomicReference<>();
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
        throw new AmqpException("Interrupted while waiting for management response");
      }
      if (this.exception.get() != null) {
        throw this.exception.get();
      }
      if (!completed) {
        throw new AmqpException("Could not get management response in %d ms", timeout.toMillis());
      }
    }

    void complete(Message<?> response) throws ClientException {
      this.responseMessage.set(response);
      this.response.set(new Response<>(Integer.parseInt(response.subject()), response.body()));
      this.latch.countDown();
    }

    void fail(AmqpException e) {
      this.exception.set(e);
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

  // for testing
  boolean hasReceiveLoop() {
    return this.receiveLoop != null;
  }

  private static class DefaultQueueInfo implements QueueInfo {

    private final String name;
    private final boolean durable;
    private final boolean autoDelete;
    private final boolean exclusive;
    private final QueueType type;
    private final Map<String, Object> arguments;
    private final String leader;
    private final List<String> members;
    private final long messageCount;
    private final int consumerCount;

    @SuppressWarnings("unchecked")
    private DefaultQueueInfo(Map<String, Object> response) {
      this.name = (String) response.get("name");
      this.durable = (Boolean) response.get("durable");
      this.autoDelete = (Boolean) response.get("auto_delete");
      this.exclusive = (Boolean) response.get("exclusive");
      this.type = QueueType.valueOf(((String) response.get("type")).toUpperCase(Locale.ENGLISH));
      this.arguments = Map.copyOf((Map<String, Object>) response.get("arguments"));
      this.leader = (String) response.get("leader");
      String[] members = (String[]) response.get("replicas");
      if (members == null || members.length == 0) {
        this.members = Collections.emptyList();
      } else {
        this.members = List.of(members);
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
    @SuppressWarnings("removal")
    public List<String> replicas() {
      return this.members();
    }

    @Override
    public List<String> members() {
      return this.members;
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
          + members
          + ", messageCount="
          + messageCount
          + ", consumerCount="
          + consumerCount
          + '}';
    }
  }

  private void checkAvailable() {
    if (this.state() == CLOSED) {
      throw new AmqpException.AmqpResourceClosedException("Management is closed");
    } else if (this.state() != OPEN) {
      throw new AmqpException.AmqpResourceInvalidStateException(
          "Management is not open, current state is %s", this.state().name());
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

  private State state() {
    return this.state.get();
  }

  private void state(State state) {
    this.state.set(state);
  }

  void markUnavailable() {
    this.state(UNAVAILABLE);
  }

  enum State {
    CREATED,
    OPEN,
    UNAVAILABLE,
    CLOSED
  }
}
