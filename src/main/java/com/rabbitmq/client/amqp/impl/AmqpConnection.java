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

import static com.rabbitmq.client.amqp.Resource.State.*;
import static com.rabbitmq.client.amqp.impl.Utils.supportFilterExpressions;
import static com.rabbitmq.client.amqp.impl.Utils.supportSetToken;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.ObservationCollector;
import com.rabbitmq.client.amqp.impl.Utils.StopWatch;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DisconnectionEvent;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AmqpConnection extends ResourceBase implements Connection {

  private static final Predicate<Exception> RECOVERY_PREDICATE =
      t -> t instanceof AmqpException.AmqpConnectionException;

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConnection.class);
  private final long id;
  private final AmqpEnvironment environment;
  private final AmqpManagement management;
  private volatile org.apache.qpid.protonj2.client.Connection nativeConnection;
  private volatile Address connectionAddress;
  private volatile String connectionNodename;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile Session nativeSession;
  private final List<AmqpPublisher> publishers = new CopyOnWriteArrayList<>();
  private final List<AmqpConsumer> consumers = new CopyOnWriteArrayList<>();
  private final List<RpcClient> rpcClients = new CopyOnWriteArrayList<>();
  private final List<RpcServer> rpcServers = new CopyOnWriteArrayList<>();
  private final TopologyListener topologyListener;
  private volatile EntityRecovery entityRecovery;
  private final AtomicBoolean recoveringConnection = new AtomicBoolean(false);
  private final DefaultConnectionSettings<?> connectionSettings;
  private final Supplier<SessionHandler> sessionHandlerSupplier;
  private final ConnectionUtils.AffinityContext affinity;
  private final ConnectionSettings.AffinityStrategy affinityStrategy;
  private final String name;
  private final Lock instanceLock = new ReentrantLock();
  private final boolean filterExpressionsSupported, setTokenSupported;
  private volatile ExecutorService dispatchingExecutorService;

  AmqpConnection(AmqpConnectionBuilder builder) {
    super(builder.listeners());
    this.id = ID_SEQUENCE.getAndIncrement();
    this.name = builder.name();
    this.environment = builder.environment();
    this.connectionSettings = builder.connectionSettings().consolidate();
    this.sessionHandlerSupplier =
        builder.isolateResources()
            ? () -> new SessionHandler.SingleSessionSessionHandler(this)
            : () -> new SessionHandler.ConnectionNativeSessionSessionHandler(this);
    BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> disconnectHandler;
    AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration =
        builder.recoveryConfiguration();

    this.topologyListener = createTopologyListener(builder);

    if (recoveryConfiguration.activated()) {
      disconnectHandler = recoveryDisconnectHandler(recoveryConfiguration, builder.name());
    } else {
      disconnectHandler =
          (c, e) -> {
            AmqpException failureCause =
                ExceptionUtils.convert(e.failureCause(), "Connection disconnected");
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
    LOGGER.debug("Opening native connection for connection '{}'...", this.name());
    NativeConnectionWrapper ncw =
        ConnectionUtils.enforceAffinity(
            addrs -> {
              NativeConnectionWrapper wrapper =
                  connect(this.connectionSettings, builder.name(), disconnectHandler, addrs);
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
    String brokerVesion = brokerVersion(this.nativeConnection);
    this.filterExpressionsSupported = supportFilterExpressions(brokerVesion);
    this.setTokenSupported = supportSetToken(brokerVesion);
    LOGGER.debug("Opened connection '{}' on node '{}'.", this.name(), this.connectionNodename());
    this.state(OPEN);
    this.environment.metricsCollector().openConnection();
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

  @Override
  public ConsumerBuilder consumerBuilder() {
    checkOpen();
    return new AmqpConsumerBuilder(this);
  }

  @Override
  public RpcClientBuilder rpcClientBuilder() {
    return new RpcSupport.AmqpRpcClientBuilder(this);
  }

  @Override
  public RpcServerBuilder rpcServerBuilder() {
    return new RpcSupport.AmqpRpcServerBuilder(this);
  }

  @Override
  public void close() {
    this.close(null);
  }

  // internal API

  private NativeConnectionWrapper connect(
      DefaultConnectionSettings<?> connectionSettings,
      String name,
      BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> disconnectHandler,
      List<Address> addresses) {

    ConnectionOptions connectionOptions = new ConnectionOptions();
    if (connectionSettings.credentialsProvider() instanceof UsernamePasswordCredentialsProvider) {
      UsernamePasswordCredentialsProvider credentialsProvider =
          (UsernamePasswordCredentialsProvider) connectionSettings.credentialsProvider();
      connectionOptions.user(credentialsProvider.getUsername());
      connectionOptions.password(credentialsProvider.getPassword());
    }
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
    StopWatch stopWatch = new StopWatch();
    try {
      LOGGER.trace("Connecting '{}' to {}...", this.name(), address);
      org.apache.qpid.protonj2.client.Connection connection =
          this.environment.client().connect(address.host(), address.port(), connectionOptions);
      LOGGER.debug("Created native connection instance for '{}'", this.name());
      ExceptionUtils.wrapGet(connection.openFuture());
      LOGGER.debug("Connection attempt '{}' succeeded", this.name());
      checkBrokerVersion(connection);
      return new NativeConnectionWrapper(connection, extractNode(connection), address);
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e);
    } finally {
      LOGGER.debug("Connection attempt for '{}' took {}", this.name(), stopWatch.stop());
    }
  }

  private void sync(NativeConnectionWrapper wrapper) {
    this.connectionAddress = wrapper.address();
    this.connectionNodename = wrapper.nodename();
    this.nativeConnection = wrapper.connection();
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
      throw ExceptionUtils.convert(e);
    }
  }

  String brokerVersion() {
    return brokerVersion(this.nativeConnection);
  }

  private static String extractNode(org.apache.qpid.protonj2.client.Connection connection)
      throws ClientException {
    String node = (String) connection.properties().get("node");
    if (node == null) {
      throw new AmqpException("The broker node name is not available");
    }
    return node;
  }

  TopologyListener createTopologyListener(AmqpConnectionBuilder builder) {
    TopologyListener topologyListener;
    if (builder.recoveryConfiguration().topology()) {
      RecordingTopologyListener rtl =
          new RecordingTopologyListener(
              "topology-listener-connection-" + this.name(), this.environment.recoveryEventLoop());
      this.entityRecovery = new EntityRecovery(this, rtl);
      topologyListener = rtl;
    } else {
      topologyListener = TopologyListener.NO_OP;
    }
    return builder.topologyListener() == null
        ? topologyListener
        : TopologyListener.compose(List.of(builder.topologyListener(), topologyListener));
  }

  private BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>
      recoveryDisconnectHandler(
          AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration, String name) {
    AtomicReference<BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>>
        resultReference = new AtomicReference<>();
    BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> result =
        (conn, event) -> {
          ClientIOException ioex = event.failureCause();
          LOGGER.debug(
              "Disconnect handler of '{}', error is the following: {}",
              this.name(),
              ioex.getMessage());
          if (this.state() == OPENING) {
            LOGGER.debug("Connection is still opening, disconnect handler skipped");
            // the broker is not available when opening the connection
            // nothing to do in this listener
            return;
          }
          if (this.recoveringConnection.get()) {
            LOGGER.debug(
                "Filtering recovery task scheduling, connection recovery of '{}' already in progress",
                this.name());
            return;
          }
          AmqpException exception = ExceptionUtils.convert(event.failureCause());
          LOGGER.debug("Converted native exception to {}", exception.getClass().getSimpleName());

          if (RECOVERY_PREDICATE.test(exception) && this.state() != OPENING) {
            LOGGER.debug(
                "Queueing recovery task for '{}', error is {}",
                this.name(),
                exception.getMessage());
            this.environment
                .executorService()
                .submit(
                    () -> {
                      if (!this.recoveringConnection.get()) {
                        recoverAfterConnectionFailure(
                            recoveryConfiguration, name, exception, resultReference);
                      }
                    });
          } else {
            LOGGER.debug(
                "Not recovering connection '{}' for error {}",
                this.name(),
                event.failureCause().getMessage());
            close(ExceptionUtils.convert(ioex));
          }
        };

    resultReference.set(result);
    return result;
  }

  private void recoverAfterConnectionFailure(
      AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration,
      String connectionName,
      Exception failureCause,
      AtomicReference<BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>>
          disconnectedHandlerReference) {
    LOGGER.info(
        "Connection '{}' to '{}' has been disconnected, trying to recover.",
        this.name(),
        this.currentConnectionLabel());
    LOGGER.debug("Notifying listeners of connection '{}'.", this.name());
    this.state(RECOVERING, failureCause);
    this.changeStateOfPublishers(RECOVERING, failureCause);
    this.changeStateOfConsumers(RECOVERING, failureCause);
    this.nativeConnection = null;
    this.nativeSession = null;
    this.connectionAddress = null;
    LOGGER.debug("Releasing management resource of connection '{}'.", this.name());
    this.releaseManagementResources();
    CompletableFuture<NativeConnectionWrapper> ncwFuture;
    if (this.recoveringConnection.compareAndSet(false, true)) {
      this.recoveringConnection.set(true);
      LOGGER.debug("Connection attempt for '{}'.", this.name());
      ncwFuture =
          recoverNativeConnection(
              recoveryConfiguration, connectionName, disconnectedHandlerReference);
    } else {
      LOGGER.debug("Connection '{}' already recovering, returning.", this.name());
      return;
    }

    ncwFuture
        .thenAccept(
            ncw -> {
              this.sync(ncw);
              LOGGER.debug("Reconnected '{}' to {}", this.name(), this.currentConnectionLabel());
              this.recoveringConnection.set(false);
              try {
                if (recoveryConfiguration.topology()) {
                  this.management.init();
                  LOGGER.debug("Recovering topology of connection '{}'...", this.name());
                  this.recoverTopology();
                  this.recoverConsumers();
                  this.recoverPublishers();
                  LOGGER.debug("Recovered topology of connection '{}'.", this.name());
                }
                LOGGER.info(
                    "Recovered connection '{}' to {}", this.name(), this.currentConnectionLabel());
                this.state(OPEN);
              } catch (Exception ex) {
                // likely InterruptedException or IO exception
                LOGGER.warn(
                    "Error while trying to recover topology for connection '{}': {}",
                    this.name(),
                    ex.getMessage());
                if (RECOVERY_PREDICATE.test(ex)) {
                  LOGGER.debug(
                      "Error during topology recoverable, queueing recovery task for '{}', error is {}",
                      this.name(),
                      ex.getMessage());
                  this.environment
                      .executorService()
                      .submit(
                          () -> {
                            if (!this.recoveringConnection.get()) {
                              recoverAfterConnectionFailure(
                                  recoveryConfiguration, name, ex, disconnectedHandlerReference);
                            }
                          });
                }
              }
            })
        .exceptionally(
            t -> {
              this.recoveringConnection.set(false);
              if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
              }
              this.close(t);
              return null;
            });
  }

  private CompletableFuture<NativeConnectionWrapper> recoverNativeConnection(
      AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration,
      String connectionName,
      AtomicReference<BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>>
          disconnectedHandlerReference) {
    return AsyncRetry.asyncRetry(
            () ->
                ConnectionUtils.enforceAffinity(
                    addrs -> {
                      NativeConnectionWrapper wrapper =
                          connect(
                              this.connectionSettings,
                              connectionName,
                              disconnectedHandlerReference.get(),
                              addrs);
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
                            e -> true,
                            Duration.ofMillis(10),
                            5,
                            "Connection affinity operation");
                      }
                    },
                    connectionName))
        .description("Trying to create native connection for '%s'.", connectionName)
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
    if (this.consumers.isEmpty()) {
      LOGGER.debug("No consumers to recover");
    } else {
      LOGGER.debug("{} consumer(s) to recover", this.consumers.size());
      List<AmqpConsumer> failedConsumers = new ArrayList<>();
      for (AmqpConsumer consumer : this.consumers) {
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
    if (this.publishers.isEmpty()) {
      LOGGER.debug("No publishers to recover");
    } else {
      LOGGER.debug("{} publisher(s) to recover", this.publishers.size());
      List<AmqpPublisher> failedPublishers = new ArrayList<>();
      for (AmqpPublisher publisher : this.publishers) {
        Utils.throwIfInterrupted();
        try {
          LOGGER.debug(
              "Recovering publisher {} (address '{}')", publisher.id(), publisher.address());
          publisher.recoverAfterConnectionFailure();
          publisher.state(OPEN);
          LOGGER.debug(
              "Recovered publisher {} (address '{}')", publisher.id(), publisher.address());
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

  private void releaseManagementResources() {
    if (this.management != null) {
      this.management.releaseResources();
    }
  }

  Session nativeSession() {
    return nativeSession(true);
  }

  Session nativeSession(boolean check) {
    if (check) {
      checkOpen();
    }

    Session result = this.nativeSession;
    if (result != null) {
      return result;
    }

    this.instanceLock.lock();
    try {
      if (this.nativeSession == null) {
        this.nativeSession = this.openSession(this.nativeConnection);
      }
      return this.nativeSession;
    } finally {
      this.instanceLock.unlock();
    }
  }

  private Session openSession(org.apache.qpid.protonj2.client.Connection connection) {
    try {
      return connection.openSession();
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e, "Error while opening session");
    }
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

  ExecutorService dispatchingExecutorService() {
    checkOpen();

    ExecutorService result = this.dispatchingExecutorService;
    if (result != null) {
      return result;
    }

    this.instanceLock.lock();
    try {
      if (this.dispatchingExecutorService == null) {
        this.dispatchingExecutorService =
            Executors.newSingleThreadExecutor(
                Utils.threadFactory("dispatching-" + this.name + "-"));
      }
      return this.dispatchingExecutorService;
    } finally {
      this.instanceLock.unlock();
    }
  }

  Clock clock() {
    return this.environment.clock();
  }

  MetricsCollector metricsCollector() {
    return this.environment.metricsCollector();
  }

  ObservationCollector observationCollector() {
    return this.environment.observationCollector();
  }

  SessionHandler createSessionHandler() {
    return this.sessionHandlerSupplier.get();
  }

  Publisher createPublisher(AmqpPublisherBuilder builder) {
    // TODO copy the builder properties to create the publisher
    AmqpPublisher publisher = new AmqpPublisher(builder);
    this.publishers.add(publisher);
    return publisher;
  }

  void removePublisher(AmqpPublisher publisher) {
    this.publishers.remove(publisher);
  }

  Consumer createConsumer(AmqpConsumerBuilder builder) {
    // TODO copy the builder properties to create the consumer
    AmqpConsumer consumer = new AmqpConsumer(builder);
    this.consumers.add(consumer);
    this.topologyListener.consumerCreated(consumer.id(), builder.queue());
    return consumer;
  }

  void removeConsumer(AmqpConsumer consumer) {
    this.consumers.remove(consumer);
    this.topologyListener.consumerDeleted(consumer.id(), consumer.queue());
  }

  RpcClient createRpcClient(RpcSupport.AmqpRpcClientBuilder builder) {
    RpcClient rpcClient = new AmqpRpcClient(builder);
    this.rpcClients.add(rpcClient);
    return rpcClient;
  }

  void removeRpcClient(RpcClient rpcClient) {
    this.rpcClients.remove(rpcClient);
  }

  RpcServer createRpcServer(RpcSupport.AmqpRpcServerBuilder builder) {
    RpcServer rpcServer = new AmqpRpcServer(builder);
    this.rpcServers.add(rpcServer);
    return rpcServer;
  }

  void removeRpcServer(RpcServer rpcServer) {
    this.rpcServers.remove(rpcServer);
  }

  private void changeStateOfPublishers(State newState, Throwable failure) {
    this.changeStateOfResources(this.publishers, newState, failure);
  }

  private void changeStateOfConsumers(State newState, Throwable failure) {
    this.changeStateOfResources(this.consumers, newState, failure);
  }

  private void changeStateOfResources(
      List<? extends ResourceBase> resources, State newState, Throwable failure) {
    resources.forEach(r -> r.state(newState, failure));
  }

  private String currentConnectionLabel() {
    if (this.connectionAddress == null) {
      return "<null>";
    } else {
      return this.connectionAddress.host() + ":" + this.connectionAddress.port();
    }
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

  boolean setTokenSupported() {
    return this.setTokenSupported;
  }

  long id() {
    return this.id;
  }

  private void close(Throwable cause) {
    if (this.closed.compareAndSet(false, true)) {
      this.state(CLOSING, cause);
      this.environment.removeConnection(this);
      if (this.topologyListener instanceof AutoCloseable) {
        try {
          ((AutoCloseable) this.topologyListener).close();
        } catch (Exception e) {
          LOGGER.info("Error while closing topology listener", e);
        }
      }
      this.closeManagement();
      for (RpcClient rpcClient : this.rpcClients) {
        rpcClient.close();
      }
      for (RpcServer rpcServer : this.rpcServers) {
        rpcServer.close();
      }
      for (AmqpPublisher publisher : this.publishers) {
        publisher.close(cause);
      }
      for (AmqpConsumer consumer : this.consumers) {
        consumer.close(cause);
      }
      try {
        this.dispatchingExecutorService.shutdownNow();
      } catch (Exception e) {
        LOGGER.info(
            "Error while shutting down dispatching executor service for connection '{}': {}",
            this.name(),
            e.getMessage());
      }
      try {
        org.apache.qpid.protonj2.client.Connection nc = this.nativeConnection;
        if (nc != null) {
          nc.close();
        }
      } catch (Exception e) {
        LOGGER.warn("Error while closing native connection", e);
      }
      this.state(CLOSED, cause);
      this.environment.metricsCollector().closeConnection();
    }
  }

  @Override
  public String toString() {
    return this.environment.toString() + "-" + this.id;
  }

  static class NativeConnectionWrapper {

    private final org.apache.qpid.protonj2.client.Connection connection;
    private final String nodename;
    private final Address address;

    NativeConnectionWrapper(
        org.apache.qpid.protonj2.client.Connection connection, String nodename, Address address) {
      this.connection = connection;
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
}
