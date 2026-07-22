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

import static com.rabbitmq.client.amqp.Resource.State.CLOSED;
import static com.rabbitmq.client.amqp.Resource.State.CLOSING;
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.impl.ExceptionUtils.convert;
import static com.rabbitmq.client.amqp.impl.Tuples.pair;
import static com.rabbitmq.client.amqp.impl.Utils.supportDirectReplyTo;
import static com.rabbitmq.client.amqp.impl.Utils.supportFilterExpressions;
import static com.rabbitmq.client.amqp.impl.Utils.supportSetToken;
import static com.rabbitmq.client.amqp.impl.Utils.supportSqlFilterExpressions;
import static java.lang.System.nanoTime;
import static java.time.Duration.ofNanos;

import com.rabbitmq.client.amqp.Address;
import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.ConnectionSettings;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.ObservationCollector;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.PublisherBuilder;
import com.rabbitmq.client.amqp.Requester;
import com.rabbitmq.client.amqp.RequesterBuilder;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.Responder;
import com.rabbitmq.client.amqp.ResponderBuilder;
import com.rabbitmq.client.amqp.impl.Tuples.Pair;
import com.rabbitmq.client.amqp.impl.Utils.RunnableWithException;
import com.rabbitmq.client.amqp.impl.Utils.StopWatch;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import com.rabbitmq.client.amqp.oauth2.CredentialsManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DisconnectionEvent;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AmqpConnection extends ResourceBase implements Connection {

  private static final int DEFAULT_NUM_THREADS = Math.max(1, Utils.AVAILABLE_PROCESSORS);

  /** Connection-related issues */
  private static final Predicate<Exception> CONNECTION_EXCEPTION_PREDICATE =
      e -> e instanceof AmqpException.AmqpConnectionException;

  /**
   * Issues related to underlying resources.
   *
   * <p>E.g. the connection used for enforcing affinity gets closed, the management is marked as
   * unavailable and throws an invalid state exception when it is called. The recovery process
   * should restart.
   */
  private static final Predicate<Exception> RESOURCE_INVALID_STATE_PREDICATE =
      e ->
          e instanceof AmqpException.AmqpResourceInvalidStateException
              && !(e instanceof AmqpException.AmqpResourceClosedException);

  static final Predicate<Exception> RECOVERY_PREDICATE =
      CONNECTION_EXCEPTION_PREDICATE.or(RESOURCE_INVALID_STATE_PREDICATE);

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConnection.class);
  private final long id;
  private final AmqpEnvironment environment;
  private volatile AmqpManagement management;
  private final TopologyListener topologyListener;
  private final EntityRecovery entityRecovery;
  private final DefaultConnectionSettings<?> connectionSettings;
  private final Supplier<SessionHandler> sessionHandlerSupplier;
  private final ConnectionUtils.AffinityContext affinity;
  private final ConnectionSettings.AffinityStrategy affinityStrategy;
  private final String name;
  private final boolean filterExpressionsSupported,
      setTokenSupported,
      sqlFilterExpressionsSupported,
      directReplyToSupported;
  private final boolean privateDispatchingExecutor;
  private final CredentialsManager.Registration credentialsRegistration;
  private final AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration;
  private final ConnectionState.ConnectionStateClient connectionStateClient;
  private volatile org.apache.qpid.protonj2.client.Connection nativeConnection;
  private volatile Address connectionAddress;
  private volatile String connectionNodename;
  private volatile Session nativeSession;
  private volatile ConsumerWorkService consumerWorkService;
  private volatile Executor dispatchingExecutor;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  AmqpConnection(AmqpConnectionBuilder builder) {
    super(builder.listeners(), builder.environment().executorService());
    this.id = ID_SEQUENCE.getAndIncrement();
    this.environment = builder.environment();
    this.name =
        builder.name() == null ? this.environment.toString() + "-" + this.id : builder.name();
    this.connectionSettings = builder.connectionSettings().consolidate();
    this.sessionHandlerSupplier =
        builder.isolateResources()
            ? () -> new SessionHandler.SingleSessionSessionHandler(this)
            : () -> new SessionHandler.ConnectionNativeSessionSessionHandler(this);
    BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> disconnectHandler;
    this.recoveryConfiguration = builder.recoveryConfiguration();

    Pair<TopologyListener, EntityRecovery> topologyInfra = createTopologyInfrastructure(builder);
    this.topologyListener = topologyInfra.v1();
    this.entityRecovery = topologyInfra.v2();

    Executor de =
        builder.dispatchingExecutor() == null
            ? environment.dispatchingExecutorService()
            : builder.dispatchingExecutor();

    if (de == null) {
      this.privateDispatchingExecutor = true;
    } else {
      this.privateDispatchingExecutor = false;
      this.dispatchingExecutor = de;
    }

    this.connectionStateClient =
        new ConnectionState.ConnectionStateClient(
            this.environment.connectionStateEventLoop(), this);

    if (this.recoveryConfiguration.activated()) {
      disconnectHandler = createDisconnectHandler(this.connectionStateClient.epoch());
    } else {
      disconnectHandler =
          (c, e) -> {
            AmqpException failureCause = convert(e.failureCause(), "Connection disconnected");
            this.close(failureCause);
          };
    }
    if (this.connectionSettings.affinity().activated()) {
      this.affinity =
          new ConnectionUtils.AffinityContext(
              this.connectionSettings.affinity().queue(),
              this.connectionSettings.affinity().operation());
      this.affinityStrategy = connectionSettings.affinity().strategy();
    } else {
      this.affinity = null;
      this.affinityStrategy = null;
    }
    this.management = createManagement();
    CredentialsManager credentialsManager = builder.credentialsManager();
    this.credentialsRegistration =
        credentialsManager.register(
            this.name(),
            (username, password) -> {
              State state = this.state();
              if (state == OPEN) {
                LOGGER.debug("Setting new token for connection {}", this.name);
                long start = nanoTime();
                ((AmqpManagement) management()).setToken(password);
                LOGGER.debug(
                    "Set new token for connection {} in {} ms",
                    this.name,
                    ofNanos(nanoTime() - start).toMillis());
              } else {
                LOGGER.debug(
                    "Could not set new token for connection {} because its state is {}",
                    this.name(),
                    state);
              }
            });
    LOGGER.debug("Opening native connection for connection '{}'...", this.name());
    NativeConnectionWrapper ncw =
        ConnectionUtils.enforceAffinity(
            addrs -> {
              NativeConnectionWrapper wrapper =
                  connect(this.connectionSettings, this.name(), disconnectHandler, addrs);
              this.nativeConnection = wrapper.connection();
              return wrapper;
            },
            this.management,
            this.affinity,
            this.environment.affinityCache(),
            this.affinityStrategy,
            ConnectionUtils.NO_RETRY_STRATEGY,
            this.name());
    this.sync(ncw);
    String brokerVersion = brokerVersion(this.nativeConnection);
    this.filterExpressionsSupported = supportFilterExpressions(brokerVersion);
    this.setTokenSupported = supportSetToken(brokerVersion);
    this.sqlFilterExpressionsSupported = supportSqlFilterExpressions(brokerVersion);
    this.directReplyToSupported = supportDirectReplyTo(brokerVersion);
    LOGGER.debug("Opened connection '{}' on node '{}'.", this.name(), this.connectionNodename());
    this.connectionStateClient.executeInLoop(() -> this.updateState(OPEN));
    this.environment.metricsCollector().openConnection();
  }

  private static void checkBroker(org.apache.qpid.protonj2.client.Connection connection)
      throws ClientException {
    String product = brokerProduct(connection);
    if (!"rabbitmq".equalsIgnoreCase(product)) {
      LOGGER.warn(
          "Connected to another broker than RabbitMQ ('{}'), the library may not behave as expected",
          product);
    }
  }

  private static void checkBrokerVersion(org.apache.qpid.protonj2.client.Connection connection)
      throws ClientException {
    String version = (String) connection.properties().get("version");
    if (version == null) {
      throw new AmqpException("No broker version set in connection properties");
    }
    if (!Utils.is4_0_OrMore(version)) {
      throw new AmqpException("The AMQP client library requires RabbitMQ 4.0 or more");
    }
  }

  private static String brokerVersion(org.apache.qpid.protonj2.client.Connection connection) {
    try {
      return (String) connection.properties().get("version");
    } catch (ClientException e) {
      throw convert(e);
    }
  }

  private static String brokerProduct(org.apache.qpid.protonj2.client.Connection connection) {
    try {
      return (String) connection.properties().get("product");
    } catch (ClientException e) {
      throw convert(e);
    }
  }

  private static String extractNode(org.apache.qpid.protonj2.client.Connection connection)
      throws ClientException {
    String node = (String) connection.properties().get("node");
    if (node == null) {
      throw new AmqpException("The broker node name is not available");
    }
    return node;
  }

  @Override
  public Management management() {
    checkOpen();
    return this.managementNoCheck();
  }

  Management managementNoCheck() {
    this.management.init();
    return this.management;
  }

  AmqpManagement createManagement() {
    return new AmqpManagement(
        new AmqpManagementParameters(this).topologyListener(this.topologyListener));
  }

  @Override
  public PublisherBuilder publisherBuilder() {
    checkOpen();
    return new AmqpPublisherBuilder(this);
  }

  // internal API

  @Override
  public ConsumerBuilder consumerBuilder() {
    checkOpen();
    return new AmqpConsumerBuilder(this);
  }

  @Override
  public RequesterBuilder requesterBuilder() {
    return new RequestResponseSupport.AmqpRequesterBuilder(this);
  }

  @Override
  public ResponderBuilder responderBuilder() {
    return new RequestResponseSupport.AmqpResponderBuilder(this);
  }

  @Override
  public ConnectionInfo connectionInfo() {
    checkOpen();
    return new AmqpConnectionInfo();
  }

  @Override
  public void close() {
    this.close(null);
  }

  private NativeConnectionWrapper connect(
      DefaultConnectionSettings<?> connectionSettings,
      String name,
      BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> disconnectHandler,
      List<Address> addresses) {

    ConnectionOptions connectionOptions = new ConnectionOptions();
    credentialsRegistration.connect(new TokenConnectionCallback(connectionOptions));
    connectionOptions.virtualHost("vhost:" + connectionSettings.virtualHost());
    connectionOptions.saslOptions().addAllowedMechanism(connectionSettings.saslMechanism());
    connectionOptions.idleTimeout(
        connectionSettings.idleTimeout().toMillis(), TimeUnit.MILLISECONDS);
    connectionOptions.disconnectedHandler(disconnectHandler);
    if (name == null) {
      connectionOptions.properties(ClientProperties.DEFAULT_CLIENT_PROPERTIES);
    } else {
      Map<String, Object> props = new LinkedHashMap<>(ClientProperties.DEFAULT_CLIENT_PROPERTIES);
      props.put("connection_name", name);
      connectionOptions.properties(Map.copyOf(props));
    }
    if (connectionSettings.tlsEnabled()) {
      DefaultConnectionSettings.DefaultTlsSettings<?> tlsSettings =
          connectionSettings.tlsSettings();
      connectionOptions.sslEnabled(true);
      SslOptions sslOptions = connectionOptions.sslOptions();
      sslOptions.sslContextOverride(tlsSettings.sslContext());
      sslOptions.verifyHost(tlsSettings.isHostnameVerification());
    }
    Address address = connectionSettings.selectAddress(addresses);
    if (connectionSettings.useWebSocket()) {
      LOGGER.trace("Using WebSocket for connection '{}'", this.name());
      connectionOptions
          .transportOptions()
          .useWebSockets(true)
          .webSocketPath(connectionSettings.webSocketPath());
    }
    connectionOptions
        .transportOptions()
        .readBytesConsumer(this.environment().readBytesConsumer())
        .writtenBytesConsumer(this.environment().writtenBytesConsumer());
    StopWatch stopWatch = new StopWatch();
    try {
      LOGGER.trace("Connecting '{}' to {}...", this.name(), address);
      org.apache.qpid.protonj2.client.Connection connection =
          this.environment.client().connect(address.host(), address.port(), connectionOptions);
      LOGGER.debug("Created native connection instance for '{}'", this.name());
      ExceptionUtils.wrapGet(connection.openFuture());
      LOGGER.debug("Connection attempt '{}' succeeded", this.name());
      checkBroker(connection);
      checkBrokerVersion(connection);
      Session session = connection.openSession(Utils.sessionOptions());
      return new NativeConnectionWrapper(connection, session, extractNode(connection), address);
    } catch (ClientException e) {
      throw convert(e);
    } finally {
      LOGGER.debug("Connection attempt for '{}' took {}", this.name(), stopWatch.stop());
    }
  }

  String brokerVersion() {
    return brokerVersion(this.nativeConnection);
  }

  String brokerProduct() {
    return brokerProduct(this.nativeConnection);
  }

  Pair<TopologyListener, EntityRecovery> createTopologyInfrastructure(
      AmqpConnectionBuilder builder) {
    TopologyListener topologyListener;
    EntityRecovery entityRecovery;
    if (builder.recoveryConfiguration().topology()) {
      RecordingTopologyListener rtl =
          new RecordingTopologyListener(
              "topology-listener-connection-" + this.name(), this.environment.recoveryEventLoop());
      entityRecovery = new EntityRecovery(this, rtl);
      topologyListener = rtl;
    } else {
      topologyListener = TopologyListener.NO_OP;
      entityRecovery = null;
    }
    topologyListener =
        builder.topologyListener() == null
            ? topologyListener
            : TopologyListener.compose(List.of(builder.topologyListener(), topologyListener));
    return pair(topologyListener, entityRecovery);
  }

  private BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>
      createDisconnectHandler(long attemptEpoch) {
    return (conn, event) -> {
      this.connectionStateClient.handleDisconnect(attemptEpoch, event);
    };
  }

  private CompletableFuture<NativeConnectionWrapper> recoverNativeConnection(
      AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration,
      String connectionName,
      long attemptEpoch) {
    return AsyncRetry.asyncRetry(
            () ->
                ConnectionUtils.enforceAffinity(
                    addrs -> {
                      NativeConnectionWrapper wrapper =
                          connect(
                              this.connectionSettings,
                              connectionName,
                              createDisconnectHandler(attemptEpoch),
                              addrs);
                      // Temporarily assign the native connection so AmqpManagement can use it to
                      // open its session and query the broker for queue leader information
                      // during the loop.
                      this.nativeConnection = wrapper.connection();
                      return wrapper;
                    },
                    this.management,
                    this.affinity,
                    this.environment.affinityCache(),
                    this.affinityStrategy,
                    new ConnectionUtils.RetryStrategy() {
                      @Override
                      public <T> T maybeRetry(Supplier<T> task) {
                        return RetryUtils.callAndMaybeRetry(
                            task::get,
                            // no need to retry if the connection is closed
                            // the affinity task will fail and AsyncRetry will take care
                            // of retrying later
                            e -> RECOVERY_PREDICATE.negate().test(e),
                            Duration.ofMillis(10),
                            5,
                            "Connection affinity operation");
                      }
                    },
                    connectionName))
        .description("Recovering native connection for '%s'.", connectionName)
        .delayPolicy(recoveryConfiguration.backOffDelayPolicy())
        .retry(RECOVERY_PREDICATE)
        .scheduler(this.scheduledExecutorService())
        .build();
  }

  private void recoverTopology() throws InterruptedException {
    if (this.entityRecovery != null) {
      Utils.throwIfInterrupted();
      this.entityRecovery.recover();
    }
  }

  private void recoverConsumers() throws InterruptedException {
    List<AmqpConsumer> consumersToRecover = this.connectionStateClient.consumers();
    if (consumersToRecover.isEmpty()) {
      LOGGER.debug("No consumers to recover");
    } else {
      LOGGER.debug("{} consumer(s) to recover", consumersToRecover.size());
      List<AmqpConsumer> failedConsumers = new ArrayList<>();
      for (AmqpConsumer consumer : consumersToRecover) {
        Utils.throwIfInterrupted();
        try {
          LOGGER.debug("Recovering consumer {} (queue '{}')", consumer.id(), consumer.queue());
          consumer.recoverAfterConnectionFailure();
          consumer.state(OPEN);
          LOGGER.debug("Recovered consumer {} (queue '{}')", consumer.id(), consumer.queue());
        } catch (AmqpException.AmqpConnectionException ex) {
          LOGGER.warn(
              "Connection error while trying to recover consumer {} (queue '{}'), restarting recovery",
              consumer.id(),
              consumer.queue(),
              ex);
          throw ex;
        } catch (AmqpException.AmqpResourceClosedException ex) {
          if (ExceptionUtils.noRunningStreamMemberOnNode(ex)) {
            LOGGER.warn(
                "Could not recover consumer {} (queue '{}') because there is "
                    + "running stream member on the node, restarting recovery",
                consumer.id(),
                consumer.queue(),
                ex);
            throw new AmqpException.AmqpConnectionException(
                "No running stream member on the node", ex);
          } else {
            LOGGER.warn(
                "Error while trying to recover consumer {} (queue '{}')",
                consumer.id(),
                consumer.queue(),
                ex);
            failedConsumers.add(consumer);
          }
        } catch (Exception ex) {
          LOGGER.warn(
              "Error while trying to recover consumer {} (queue '{}')",
              consumer.id(),
              consumer.queue(),
              ex);
          failedConsumers.add(consumer);
        }
      }
      failedConsumers.forEach(AmqpConsumer::close);
    }
  }

  private void recoverPublishers() throws InterruptedException {
    List<AmqpPublisher> publishersToRecover = this.connectionStateClient.publishers();
    if (publishersToRecover.isEmpty()) {
      LOGGER.debug("No publishers to recover");
    } else {
      LOGGER.debug("{} publisher(s) to recover", publishersToRecover.size());
      List<AmqpPublisher> failedPublishers = new ArrayList<>();
      for (AmqpPublisher publisher : publishersToRecover) {
        Utils.throwIfInterrupted();
        try {
          LOGGER.debug(
              "Recovering publisher {} (address '{}')", publisher.id(), publisher.address());
          publisher.recoverAfterConnectionFailure();
          publisher.state(OPEN);
          LOGGER.debug(
              "Recovered publisher {} (address '{}')", publisher.id(), publisher.address());
        } catch (AmqpException.AmqpConnectionException ex) {
          LOGGER.warn(
              "Connection error while trying to recover publisher {} (address '{}'), restarting recovery",
              publisher.id(),
              publisher.address(),
              ex);
          throw ex;
        } catch (Exception ex) {
          LOGGER.warn(
              "Error while trying to recover publisher {} (address '{}')",
              publisher.id(),
              publisher.address(),
              ex);
          failedPublishers.add(publisher);
        }
      }
      failedPublishers.forEach(AmqpPublisher::close);
    }
  }

  private void closeManagement() {
    this.management.close();
  }

  Session nativeSession() {
    return nativeSession(true);
  }

  Session nativeSession(boolean check) {
    if (check) {
      checkOpen();
    }
    Session result = this.nativeSession;
    if (result == null) {
      throw new AmqpException.AmqpResourceInvalidStateException("Native session is not available");
    }
    return result;
  }

  org.apache.qpid.protonj2.client.Connection nativeConnection() {
    return this.nativeConnection;
  }

  AmqpEnvironment environment() {
    return this.environment;
  }

  ScheduledExecutorService scheduledExecutorService() {
    return this.environment.scheduledExecutorService();
  }

  ConsumerWorkService consumerWorkService() {
    checkOpen();

    // Fast-path read
    ConsumerWorkService result = this.consumerWorkService;
    if (result != null) {
      return result;
    }

    // Slow-path initialization serialized in the Event Loop
    return this.connectionStateClient.executeInLoop(
        () -> {
          if (this.state() == CLOSED || this.state() == CLOSING) {
            throw new AmqpException.AmqpResourceClosedException("Connection is closed");
          }
          if (this.consumerWorkService == null) {
            if (this.privateDispatchingExecutor) {
              this.dispatchingExecutor =
                  Executors.newFixedThreadPool(
                      DEFAULT_NUM_THREADS, Utils.threadFactory("dispatching-" + this.name() + "-"));
            }
            this.consumerWorkService = new WorkPoolConsumerWorkService(this.dispatchingExecutor);
          }
          return this.consumerWorkService;
        });
  }

  Clock clock() {
    return this.environment.clock();
  }

  MetricsCollector metricsCollector() {
    return this.environment.metricsCollector();
  }

  BackOffDelayPolicy recoveryBackOffDelayPolicy() {
    return this.recoveryConfiguration.backOffDelayPolicy();
  }

  ObservationCollector observationCollector() {
    return this.environment.observationCollector();
  }

  SessionHandler createSessionHandler() {
    return this.sessionHandlerSupplier.get();
  }

  Publisher createPublisher(AmqpPublisherBuilder builder) {
    // TODO copy the builder properties to create the publisher
    checkOpen();
    AmqpPublisher publisher = new AmqpPublisher(builder);
    boolean registered = this.connectionStateClient.registerPublisher(publisher);
    if (!registered) {
      publisher.close();
      throw new AmqpException.AmqpResourceClosedException(
          "Connection was closed during publisher creation");
    }
    return publisher;
  }

  void removePublisher(AmqpPublisher publisher) {
    this.connectionStateClient.removePublisher(publisher);
  }

  Consumer createConsumer(AmqpConsumerBuilder builder) {
    checkOpen();
    // TODO copy the builder properties to create the consumer
    AmqpConsumer consumer = new AmqpConsumer(builder);
    boolean registered = this.connectionStateClient.registerConsumer(consumer);
    if (!registered) {
      consumer.close();
      throw new AmqpException.AmqpResourceClosedException(
          "Connection was closed during consumer creation");
    }
    this.topologyListener.consumerCreated(consumer.id(), builder.queue());
    return consumer;
  }

  void removeConsumer(AmqpConsumer consumer) {
    this.connectionStateClient.removeConsumer(consumer);
    this.topologyListener.consumerDeleted(consumer.id(), consumer.queue());
  }

  Requester createRequester(RequestResponseSupport.AmqpRequesterBuilder builder) {
    checkOpen();
    Requester requester = new AmqpRequester(builder);
    boolean registered = this.connectionStateClient.registerRequester(requester);
    if (!registered) {
      requester.close();
      throw new AmqpException.AmqpResourceClosedException(
          "Connection was closed during requester creation");
    }
    return requester;
  }

  void removeRequester(Requester requester) {
    this.connectionStateClient.removeRequester(requester);
  }

  Responder createResponder(RequestResponseSupport.AmqpResponderBuilder builder) {
    checkOpen();
    Responder responder = new AmqpResponder(builder);
    boolean registered = this.connectionStateClient.registerResponder(responder);
    if (!registered) {
      responder.close();
      throw new AmqpException.AmqpResourceClosedException(
          "Connection was closed during responder recreation");
    }
    return responder;
  }

  void removeResponder(Responder responder) {
    this.connectionStateClient.removeResponder(responder);
  }

  Address connectionAddress() {
    return this.connectionAddress;
  }

  String connectionNodename() {
    return this.connectionNodename;
  }

  String name() {
    return this.name == null ? "<no-name>" : this.name;
  }

  ConnectionUtils.AffinityContext affinity() {
    return this.affinity;
  }

  boolean filterExpressionsSupported() {
    return this.filterExpressionsSupported;
  }

  boolean sqlFilterExpressionsSupported() {
    return this.sqlFilterExpressionsSupported;
  }

  boolean setTokenSupported() {
    return this.setTokenSupported;
  }

  boolean directReplyToSupported() {
    return this.directReplyToSupported;
  }

  long id() {
    return this.id;
  }

  void close(Throwable cause) {
    if (this.closed.compareAndSet(false, true)) {
      LOGGER.debug("Closing connection {}", this);
      try {
        this.connectionStateClient.markClosed(cause);
        this.connectionStateClient.executeInLoop(() -> this.state(CLOSING, cause));
      } catch (IllegalStateException e) {
        LOGGER.debug("Error in event loop: {}", e.getMessage());
      }
      this.credentialsRegistration.close();
      this.environment.removeConnection(this);
      BiConsumer<String, RunnableWithException> safeClose =
          (label, action) -> {
            try {
              action.run();
            } catch (Exception e) {
              LOGGER.info(
                  "Error during connection '{}' closing ({}): {}", this, label, e.getMessage());
            }
          };
      if (this.topologyListener instanceof AutoCloseable) {
        safeClose.accept(
            "topology listener", () -> ((AutoCloseable) this.topologyListener).close());
      }
      safeClose.accept("management", this.management::destroy);

      for (Requester requester : this.connectionStateClient.requesters()) {
        safeClose.accept("requester", requester::close);
      }
      for (Responder responder : this.connectionStateClient.responders()) {
        safeClose.accept("responder", responder::close);
      }
      for (AmqpPublisher publisher : this.connectionStateClient.publishers()) {
        safeClose.accept("publisher", () -> publisher.close(cause));
      }
      for (AmqpConsumer consumer : this.connectionStateClient.consumers()) {
        safeClose.accept("consumer", () -> consumer.close(cause));
      }
      if (this.consumerWorkService != null) {
        try {
          this.consumerWorkService.close();
        } catch (Exception e) {
          LOGGER.info(
              "Error while closing consumer work service for connection '{}': {}",
              this.name(),
              e.getMessage());
        }
      }
      if (this.privateDispatchingExecutor) {
        Executor es = this.dispatchingExecutor;
        if (es instanceof ExecutorService) {
          try {
            ((ExecutorService) es).shutdownNow();
          } catch (Exception e) {
            LOGGER.info(
                "Error while shutting down dispatching executor service for connection '{}': {}",
                this.name(),
                e.getMessage());
          }
        }
      }
      try {
        org.apache.qpid.protonj2.client.Connection nc = this.nativeConnection;
        if (nc != null) {
          nc.close();
        }
      } catch (Exception e) {
        LOGGER.warn("Error while closing native connection", e);
      }
      this.connectionStateClient.executeInLoop(() -> this.state(CLOSED, cause));
      safeClose.accept("state", this.connectionStateClient::close);
      this.environment.metricsCollector().closeConnection();
      LOGGER.debug("Connection {} has been closed", this);
    }
  }

  @Override
  public String toString() {
    return this.name();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AmqpConnection that = (AmqpConnection) o;
    return id == that.id;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  void updateState(Resource.State state) {
    this.state(state);
  }

  static class NativeConnectionWrapper {

    private final org.apache.qpid.protonj2.client.Connection connection;
    private final Session session;
    private final String nodename;
    private final Address address;

    NativeConnectionWrapper(
        org.apache.qpid.protonj2.client.Connection connection,
        Session session,
        String nodename,
        Address address) {
      this.connection = connection;
      this.session = session;
      this.nodename = nodename;
      this.address = address;
    }

    String nodename() {
      return this.nodename;
    }

    Address address() {
      return this.address;
    }

    org.apache.qpid.protonj2.client.Connection connection() {
      return this.connection;
    }

    Session session() {
      return this.session;
    }
  }

  private static class TokenConnectionCallback
      implements CredentialsManager.AuthenticationCallback {

    private final ConnectionOptions options;

    private TokenConnectionCallback(ConnectionOptions options) {
      this.options = options;
    }

    @Override
    public void authenticate(String username, String password) {
      options.user(username).password(password);
    }
  }

  private class AmqpConnectionInfo implements ConnectionInfo {

    @Override
    public String brokerVersion() {
      return AmqpConnection.this.brokerVersion();
    }

    @Override
    public String brokerProductName() {
      return AmqpConnection.this.brokerProduct();
    }

    @Override
    public String brokerNode() {
      return AmqpConnection.this.connectionNodename();
    }

    @Override
    public String host() {
      Address address = AmqpConnection.this.connectionAddress();
      return address != null ? address.host() : null;
    }

    @Override
    public int port() {
      Address address = AmqpConnection.this.connectionAddress();
      return address != null ? address.port() : -1;
    }

    @Override
    public String name() {
      return AmqpConnection.this.name();
    }
  }

  // connection-state-related callbacks

  void releaseManagementResources(AmqpException e) {
    if (this.management != null) {
      this.management.releaseResources(e);
    }
  }

  void updateState(Resource.State state, Throwable failureCause) {
    this.state(state, failureCause);
  }

  void resetNativeResources() {
    this.nativeConnection = null;
    this.nativeSession = null;
    this.connectionAddress = null;
  }

  void dispatchNativeRecovery(long attemptEpoch) {
    if (this.closed.get()) {
      LOGGER.debug("Discarding recovery dispatching, connection '{}' is closed.", this.name());
      return;
    }

    LOGGER.debug("Scheduling connection attempt for '{}'.", this.name());

    CompletableFuture<NativeConnectionWrapper> ncwFuture =
        recoverNativeConnection(this.recoveryConfiguration, this.name(), attemptEpoch);

    ncwFuture
        .thenAcceptAsync(
            ncw -> {
              // Phase 1 complete. Hand the new socket back to the Event Loop.
              this.connectionStateClient.handleNativeRecoverySuccess(ncw, attemptEpoch);
            },
            this.environment.executorService())
        .exceptionally(
            t -> {
              if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
              }
              LOGGER.warn(
                  "Native connection recovery for '{}' failed fundamentally: {}",
                  this.name(),
                  t.getMessage());
              this.close(t);
              return null;
            });
  }

  void sync(NativeConnectionWrapper wrapper) {
    this.connectionAddress = wrapper.address();
    this.connectionNodename = wrapper.nodename();
    this.nativeConnection = wrapper.connection();
    this.nativeSession = wrapper.session();
  }

  void dispatchTopologyRecovery(long attemptEpoch) {
    if (this.closed.get()) {
      LOGGER.debug(
          "Discarding topology recovery dispatching, connection '{}' is closed.", this.name());
      return;
    }

    try {
      boolean managementPreviouslyClosed = this.management.isClosed();
      LOGGER.debug("Recovering topology of connection '{}'...", this.name());

      // Safely resurrect management via its Event Loop
      this.managementNoCheck();

      this.recoverTopology();
      this.recoverConsumers();
      this.recoverPublishers();

      LOGGER.debug("Recovered topology of connection '{}'.", this.name());

      if (managementPreviouslyClosed) {
        LOGGER.debug("Management was closed before recovery, closing it again");
        try {
          this.closeManagement();
        } catch (Exception e) {
          LOGGER.info("Error while (re)closing management after recovery");
        }
      }

      // Phase 2 complete. Report success to the Event Loop.
      this.connectionStateClient.handleTopologyRecoverySuccess(attemptEpoch);

    } catch (Exception ex) {
      AmqpException amqpException = ExceptionUtils.convert(ex);
      if (ex instanceof InterruptedException) {
        LOGGER.debug("Topology recovery interrupted for connection '{}'", this.name());
        Thread.currentThread().interrupt();
        this.close(amqpException);
      } else {
        LOGGER.warn(
            "Error while recovering topology for connection '{}': {}",
            this.name(),
            ex.getMessage());
        // Phase 2 failed mid-flight. Report failure to the Event Loop.
        this.connectionStateClient.handleTopologyRecoveryFailure(attemptEpoch);
      }
    }
  }
}
