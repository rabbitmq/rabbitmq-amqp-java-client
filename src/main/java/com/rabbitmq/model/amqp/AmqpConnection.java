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

import static com.rabbitmq.model.Resource.State.*;
import static com.rabbitmq.model.amqp.ExceptionUtils.convert;
import static com.rabbitmq.model.amqp.Utils.throwIfInterrupted;
import static java.util.Collections.singletonMap;

import com.rabbitmq.model.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DisconnectionEvent;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpConnection extends ResourceBase implements Connection {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConnection.class);
  private final AmqpEnvironment environment;
  private volatile AmqpManagement management;
  private final Lock managementLock = new ReentrantLock();
  private volatile org.apache.qpid.protonj2.client.Connection nativeConnection;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile Session nativeSession;
  private final List<AmqpPublisher> publishers = new CopyOnWriteArrayList<>();
  private final List<AmqpConsumer> consumers = new CopyOnWriteArrayList<>();
  private final TopologyListener topologyListener;
  private volatile EntityRecovery entityRecovery;
  private final Thread recoveryLoop;
  private final BlockingQueue<Runnable> recoveryRequestQueue;
  private final AtomicBoolean recoveringConnection = new AtomicBoolean(false);

  AmqpConnection(AmqpConnectionBuilder builder) {
    super(builder.listeners());
    this.environment = builder.environment();

    Utils.ConnectionParameters connectionParameters = this.environment.connectionParameters();
    BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> disconnectHandler;
    AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration =
        builder.recoveryConfiguration();

    this.topologyListener = createTopologyListener(builder);

    if (recoveryConfiguration.activated()) {
      this.recoveryRequestQueue = new ArrayBlockingQueue<>(10);
      this.recoveryLoop =
          Utils.newThread(
              "rabbitmq-amqp-connection-recovery",
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
      this.recoveryLoop.start();
      disconnectHandler =
          recoveryDisconnectHandler(recoveryConfiguration, connectionParameters, builder.name());
    } else {
      disconnectHandler =
          (c, e) -> {
            if (this.closed.compareAndSet(false, true)) {
              ModelException failureCause = convert(e.failureCause(), "Connection disconnected");
              this.state(CLOSING, failureCause);
              this.maybeReleaseManagementResources();
              this.state(CLOSED, failureCause);
            }
          };
      this.recoveryRequestQueue = null;
      this.recoveryLoop = null;
    }
    this.nativeConnection = connect(connectionParameters, builder.name(), disconnectHandler);
    this.state(OPEN);
  }

  @Override
  public Management management() {
    checkOpen();
    return managementNoCheck();
  }

  Management managementNoCheck() {
    try {
      this.managementLock.lock();
      if (this.management == null || !this.management.isOpen()) {
        this.management = createManagement();
      }
    } finally {
      this.managementLock.unlock();
    }
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
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.state(CLOSING);
      if (this.recoveryLoop != null) {
        this.recoveryLoop.interrupt();
      }
      this.maybeCloseManagement();
      for (AmqpPublisher publisher : this.publishers) {
        publisher.close();
      }
      try {
        this.nativeConnection.close();
      } catch (Exception e) {
        LOGGER.warn("Error while closing native connection", e);
      }
      this.state(CLOSED);
    }
  }

  // internal API

  private org.apache.qpid.protonj2.client.Connection connect(
      Utils.ConnectionParameters connectionParameters,
      String name,
      BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>
          disconnectHandler) {
    ConnectionOptions connectionOptions = new ConnectionOptions();
    connectionOptions.user(connectionParameters.username());
    connectionOptions.password(connectionParameters.password());
    connectionOptions.virtualHost("vhost:" + connectionParameters.virtualHost());
    // only the mechanisms supported in RabbitMQ
    connectionOptions.saslOptions().addAllowedMechanism("PLAIN").addAllowedMechanism("EXTERNAL");
    connectionOptions.disconnectedHandler(disconnectHandler);
    if (name != null) {
      connectionOptions.properties(singletonMap("connection_name", name));
    }
    try {
      org.apache.qpid.protonj2.client.Connection connection =
          this.environment
              .client()
              .connect(connectionParameters.host(), connectionParameters.port(), connectionOptions);
      return connection.openFuture().get();
    } catch (ClientException | ExecutionException e) {
      throw new ModelException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ModelException(e);
    }
  }

  TopologyListener createTopologyListener(AmqpConnectionBuilder builder) {
    TopologyListener topologyListener;
    if (builder.recoveryConfiguration().topology()) {
      RecordingTopologyListener rtl = new RecordingTopologyListener();
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
          AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration,
          Utils.ConnectionParameters connectionParameters,
          String name) {
    AtomicReference<BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>>
        resultReference = new AtomicReference<>();
    BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> result =
        (conn, event) -> {
          if (event.failureCause() instanceof ClientConnectionRemotelyClosedException
              && this.recoveringConnection.get()) {
            LOGGER.debug("Filtering recovery task enqueueing, connection recovery in progress");
          } else {
            LOGGER.debug("Queueing recovery task");
            this.recoveryRequestQueue.add(
                () ->
                    recoverAfterConnectionFailure(
                        recoveryConfiguration,
                        connectionParameters,
                        name,
                        event.failureCause(),
                        resultReference));
          }
        };

    resultReference.set(result);
    return result;
  }

  private void recoverAfterConnectionFailure(
      AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration,
      Utils.ConnectionParameters connectionParameters,
      String connectionName,
      ClientIOException nativeFailureCause,
      AtomicReference<BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>>
          disconnectedHandlerReference) {
    ModelException failureCause = convert(nativeFailureCause, "Connection disconnected");
    LOGGER.info("Connection to {} failed, trying to recover", connectionParameters.label());
    this.state(RECOVERING, failureCause);
    this.changeStateOfPublishers(RECOVERING, failureCause);
    this.changeStateOfConsumers(RECOVERING, failureCause);
    this.nativeConnection = null;
    this.nativeSession = null;
    this.maybeReleaseManagementResources();
    try {
      this.recoveringConnection.set(true);
      this.nativeConnection =
          recoverNativeConnection(
              recoveryConfiguration,
              connectionParameters,
              connectionName,
              disconnectedHandlerReference);
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


      LOGGER.info("Recovered connection to {}", connectionParameters.label());
      this.state(OPEN);
    } catch (Exception ex) {
      // likely InterruptedException or IO exception
      LOGGER.info("Error while trying to recover connection", ex);
    }
  }

  private org.apache.qpid.protonj2.client.Connection recoverNativeConnection(
      AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration,
      Utils.ConnectionParameters connectionParameters,
      String connectionName,
      AtomicReference<BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>>
          disconnectedHandlerReference)
      throws InterruptedException {
    int attempt = 0;
    while (true) {
      Duration delay = recoveryConfiguration.backOffDelayPolicy().delay(attempt);
      if (BackOffDelayPolicy.TIMEOUT.equals(delay)) {
        throw new ModelException("Recovery retry timed out");
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
            connect(connectionParameters, connectionName, disconnectedHandlerReference.get());
        result.openFuture().get();
        LOGGER.debug("Reconnected to {}", connectionParameters.label());
        return result;
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.info("Thread interrupted while waiting for connection opening");
        throw ex;
      } catch (Exception ex) {
        LOGGER.info("Error while trying to recover connection", ex);
        // TODO determine whether we should recover after the exception
      }
    }
  }

  private void recoverTopology() throws InterruptedException {
    if (this.entityRecovery != null) {
      throwIfInterrupted();
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
        throwIfInterrupted();
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
        throwIfInterrupted();
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

  private void maybeCloseManagement() {
    try {
      this.managementLock.lock();
      if (this.management != null) {
        this.management.close();
      }
    } finally {
      this.managementLock.unlock();
    }
  }

  private void maybeReleaseManagementResources() {
    // FIXME keep the management reference and make it recover
    // users can keep a reference to it
    try {
      this.managementLock.lock();
      if (this.management != null) {
        this.management.releaseResources();
        this.management = null;
      }
    } finally {
      this.managementLock.unlock();
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
      throw convert(e, "Error while opening session");
    }
  }

  org.apache.qpid.protonj2.client.Connection nativeConnection() {
    return this.nativeConnection;
  }

  ExecutorService executorService() {
    return this.environment.executorService();
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
}
