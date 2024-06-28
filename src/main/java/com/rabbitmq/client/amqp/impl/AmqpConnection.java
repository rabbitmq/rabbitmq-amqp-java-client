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
import static java.util.Collections.singletonMap;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.ObservationCollector;
import com.rabbitmq.client.amqp.impl.Utils.StopWatch;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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

  private static final Predicate<Throwable> RECOVERY_PREDICATE =
      t -> t instanceof AmqpException.AmqpConnectionException;

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConnection.class);
  private final long id;
  private final AmqpEnvironment environment;
  private final AmqpManagement management;
  private volatile org.apache.qpid.protonj2.client.Connection nativeConnection;
  private volatile Address connectionAddress;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile Session nativeSession;
  private final List<AmqpPublisher> publishers = new CopyOnWriteArrayList<>();
  private final List<AmqpConsumer> consumers = new CopyOnWriteArrayList<>();
  private final List<RpcClient> rpcClients = new CopyOnWriteArrayList<>();
  private final List<RpcServer> rpcServers = new CopyOnWriteArrayList<>();
  private final TopologyListener topologyListener;
  private volatile EntityRecovery entityRecovery;
  private final Future<?> recoveryLoop;
  private final BlockingQueue<Runnable> recoveryRequestQueue;
  private final AtomicBoolean recoveringConnection = new AtomicBoolean(false);
  private final DefaultConnectionSettings<?> connectionSettings =
      DefaultConnectionSettings.instance();
  private Supplier<SessionHandler> sessionHandlerSupplier;

  AmqpConnection(AmqpConnectionBuilder builder) {
    super(builder.listeners());
    this.id = ID_SEQUENCE.getAndIncrement();
    this.environment = builder.environment();
    builder.connectionSettings().copyTo(this.connectionSettings);
    this.connectionSettings.consolidate();
    this.sessionHandlerSupplier =
        builder.isolateResources()
            ? () -> new SessionHandler.SingleSessionSessionHandler(this)
            : () -> new SessionHandler.ConnectionNativeSessionSessionHandler(this);
    BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> disconnectHandler;
    AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration =
        builder.recoveryConfiguration();

    this.topologyListener = createTopologyListener(builder);

    if (recoveryConfiguration.activated()) {
      this.recoveryRequestQueue = new ArrayBlockingQueue<>(10);
      this.recoveryLoop =
          this.executorService()
              .submit(
                  () -> {
                    while (!Thread.currentThread().isInterrupted()) {
                      try {
                        Runnable recoveryTask = this.recoveryRequestQueue.take();
                        recoveryTask.run();
                      } catch (InterruptedException e) {
                        return;
                      } catch (Exception e) {
                        LOGGER.warn("Error during connection recovery", e);
                      }
                    }
                  });
      disconnectHandler = recoveryDisconnectHandler(recoveryConfiguration, builder.name());
    } else {
      disconnectHandler =
          (c, e) -> {
            AmqpException failureCause =
                ExceptionUtils.convert(e.failureCause(), "Connection disconnected");
            this.close(failureCause);
          };
      this.recoveryRequestQueue = null;
      this.recoveryLoop = null;
    }
    this.nativeConnection = connect(this.connectionSettings, builder.name(), disconnectHandler);
    this.management = createManagement();
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

  protected AmqpManagement createManagement() {
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

  private org.apache.qpid.protonj2.client.Connection connect(
      DefaultConnectionSettings<?> connectionSettings,
      String name,
      BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>
          disconnectHandler) {

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
    if (name != null) {
      connectionOptions.properties(singletonMap("connection_name", name));
    }
    if (connectionSettings.tlsEnabled()) {
      DefaultConnectionSettings.DefaultTlsSettings<?> tlsSettings =
          connectionSettings.tlsSettings();
      connectionOptions.sslEnabled(true);
      SslOptions sslOptions = connectionOptions.sslOptions();
      sslOptions.sslContextOverride(tlsSettings.sslContext());
      sslOptions.verifyHost(tlsSettings.isHostnameVerification());
    }
    this.connectionAddress = connectionSettings.selectAddress();
    StopWatch stopWatch = new StopWatch();
    try {
      LOGGER.debug("Connecting...");
      org.apache.qpid.protonj2.client.Connection connection =
          this.environment
              .client()
              .connect(
                  this.connectionAddress.host(), this.connectionAddress.port(), connectionOptions);
      ExceptionUtils.wrapGet(connection.openFuture());
      LOGGER.debug("Connection attempt succeeded");
      checkBrokerVersion(connection);
      return connection;
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e);
    } finally {
      LOGGER.debug("Connection attempt took {}", stopWatch.stop());
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

  TopologyListener createTopologyListener(AmqpConnectionBuilder builder) {
    TopologyListener topologyListener;
    if (builder.recoveryConfiguration().topology()) {
      RecordingTopologyListener rtl = new RecordingTopologyListener(this.executorService());
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
          LOGGER.debug("Disconnect handler, error is the following:", ioex);
          if (this.state() == OPENING) {
            LOGGER.debug("Connection is still opening, disconnect handler skipped");
            // the broker is not available when opening the connection
            // nothing to do in this listener
            return;
          }
          if (this.recoveringConnection.get()) {
            LOGGER.debug("Filtering recovery task enqueueing, connection recovery in progress");
            return;
          }
          AmqpException exception = ExceptionUtils.convert(event.failureCause());
          LOGGER.debug("Converted native exception to {}", exception.getClass().getSimpleName());

          if (RECOVERY_PREDICATE.test(exception) && this.state() != OPENING) {
            LOGGER.debug("Queueing recovery task, error is {}", exception.getMessage());
            this.recoveryRequestQueue.add(
                () ->
                    recoverAfterConnectionFailure(
                        recoveryConfiguration, name, exception, resultReference));
          } else {
            LOGGER.debug(
                "Not recovering connection for error {}", event.failureCause().getMessage());
          }
        };

    resultReference.set(result);
    return result;
  }

  private void recoverAfterConnectionFailure(
      AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration,
      String connectionName,
      AmqpException failureCause,
      AtomicReference<BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>>
          disconnectedHandlerReference) {
    LOGGER.info("Connection to {} failed, trying to recover", this.currentConnectionLabel());
    this.state(RECOVERING, failureCause);
    this.changeStateOfPublishers(RECOVERING, failureCause);
    this.changeStateOfConsumers(RECOVERING, failureCause);
    this.nativeConnection = null;
    this.nativeSession = null;
    this.connectionAddress = null;
    this.releaseManagementResources();
    try {
      this.recoveringConnection.set(true);
      this.nativeConnection =
          recoverNativeConnection(
              recoveryConfiguration, connectionName, disconnectedHandlerReference);
    } catch (Exception ex) {
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      this.closed.set(true);
      this.changeStateOfPublishers(CLOSED, ex);
      this.changeStateOfConsumers(CLOSED, ex);
      this.state(CLOSED, ex);
      return;
    } finally {
      this.recoveringConnection.set(false);
    }

    try {
      if (recoveryConfiguration.topology()) {
        LOGGER.debug("Recovering topology");
        this.recoverTopology();
        this.recoverConsumers();
        this.recoverPublishers();
        LOGGER.debug("Recovered topology");
      }

      LOGGER.info("Recovered connection to {}", this.currentConnectionLabel());
      this.state(OPEN);
    } catch (Exception ex) {
      // likely InterruptedException or IO exception
      LOGGER.info("Error while trying to recover connection", ex);
    }
  }

  private org.apache.qpid.protonj2.client.Connection recoverNativeConnection(
      AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration,
      String connectionName,
      AtomicReference<BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>>
          disconnectedHandlerReference)
      throws InterruptedException {
    int attempt = 0;
    while (true) {
      Duration delay = recoveryConfiguration.backOffDelayPolicy().delay(attempt);
      if (BackOffDelayPolicy.TIMEOUT.equals(delay)) {
        throw new AmqpException("Recovery retry timed out");
      } else {
        try {
          Thread.sleep(delay.toMillis());
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          LOGGER.info("Thread interrupted while waiting during connection recovery");
          throw ex;
        }
      }

      try {
        org.apache.qpid.protonj2.client.Connection result =
            connect(this.connectionSettings, connectionName, disconnectedHandlerReference.get());
        result.openFuture().get();
        LOGGER.debug("Reconnected to {}", this.currentConnectionLabel());
        return result;
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.info("Thread interrupted while waiting for connection opening");
        throw ex;
      } catch (Exception ex) {
        LOGGER.info("Error while trying to recover connection", ex);
        if (!RECOVERY_PREDICATE.test(ex)) {
          LOGGER.info(
              "Stopping connection recovery, exception is not recoverable: {}", ex.getMessage());
          throw new AmqpException("Could not recover connection after fatal exception", ex);
        }
      }
    }
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
          LOGGER.debug("Recovering consumer {} (address '{}')", consumer.id(), consumer.address());
          consumer.recoverAfterConnectionFailure();
          consumer.state(OPEN);
          LOGGER.debug("Recovered consumer {} (address '{}')", consumer.id(), consumer.address());
        } catch (Exception ex) {
          LOGGER.warn(
              "Error while trying to recover consumer {} (address '{}')",
              consumer.id(),
              consumer.address(),
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
    if (result == null) {
      synchronized (this) {
        result = this.nativeSession;
        if (result == null) {
          if (check) {
            checkOpen();
          }
          this.nativeSession = result = this.openSession(this.nativeConnection);
        }
      }
    }
    return result;
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

  ExecutorService executorService() {
    return this.environment.executorService();
  }

  ScheduledExecutorService scheduledExecutorService() {
    return this.environment.scheduledExecutorService();
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
    this.topologyListener.consumerCreated(consumer.id(), consumer.address());
    return consumer;
  }

  void removeConsumer(AmqpConsumer consumer) {
    this.consumers.remove(consumer);
    this.topologyListener.consumerDeleted(consumer.id(), consumer.address());
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

  private void close(Throwable cause) {
    if (this.closed.compareAndSet(false, true)) {
      this.state(CLOSING, cause);
      if (this.recoveryLoop != null) {
        this.recoveryLoop.cancel(true);
      }
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
        publisher.close();
      }
      for (AmqpConsumer consumer : this.consumers) {
        consumer.close();
      }
      try {
        this.nativeConnection.close();
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
}
