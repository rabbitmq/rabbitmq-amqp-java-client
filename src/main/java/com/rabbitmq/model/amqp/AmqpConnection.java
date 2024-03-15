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
import static java.util.Collections.singletonMap;

import com.rabbitmq.model.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DisconnectionEvent;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
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
  private List<AmqpPublisher> publishers = new CopyOnWriteArrayList<>();
  private List<AmqpConsumer> consumers = new CopyOnWriteArrayList<>();

  AmqpConnection(AmqpConnectionBuilder builder) {
    super(builder.listeners());
    this.environment = builder.environment();

    Utils.ConnectionParameters connectionParameters = this.environment.connectionParameters();
    BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> disconnectHandler;
    AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration =
        builder.recoveryConfiguration();
    if (recoveryConfiguration.activated()) {
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
    }
    this.nativeConnection = connect(connectionParameters, builder.name(), disconnectHandler);
    this.state(OPEN);
  }

  @Override
  public Management management() {
    checkOpen();
    try {
      this.managementLock.lock();
      if (this.management == null || !this.management.isOpen()) {
        this.management = new AmqpManagement(this);
      }
    } finally {
      this.managementLock.unlock();
    }
    return this.management;
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

  private BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>
      recoveryDisconnectHandler(
          AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration,
          Utils.ConnectionParameters connectionParameters,
          String name) {
    AtomicReference<BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>>
        resultReference = new AtomicReference<>();
    BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> result =
        (c, e) -> {
          // TODO use a dedicated thread for connection recovery
          // (we do not want to wait too long in a IO thread)
          // it should be dispatched as a task that could be stopped in #close()
          ModelException failureCause = convert(e.failureCause(), "Connection disconnected");
          if (this.compareAndSetState(OPEN, RECOVERING, failureCause)) {
            this.state(RECOVERING, failureCause);
            this.changeStateOfPublishers(RECOVERING);
            this.changeStateOfConsumers(RECOVERING);
            this.nativeConnection = null;
            this.nativeSession = null;
            this.maybeReleaseManagementResources();
            int attempt = 0;
            while (true) {
              try {
                Thread.sleep(recoveryConfiguration.backOffDelayPolicy().delay(attempt).toMillis());
              } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.info("Thread interrupted while waiting during connection recovery");
                this.closed.set(true);
                this.changeStateOfPublishers(CLOSED);
                this.changeStateOfConsumers(CLOSED);
                this.state(CLOSED, ex);
              }
              try {
                this.nativeConnection = connect(connectionParameters, name, resultReference.get());
                this.nativeConnection.openFuture().get();
                this.state(OPEN);

                List<AmqpConsumer> failedConsumers = new ArrayList<>();
                for (AmqpConsumer consumer : this.consumers) {
                  try {
                    consumer.recoverAfterConnectionFailure();
                    consumer.state(OPEN);
                  } catch (Exception ex) {
                    LOGGER.warn("Error while trying to recover consumer", ex);
                    failedConsumers.add(consumer);
                  }
                }
                failedConsumers.forEach(AmqpConsumer::close);

                List<AmqpPublisher> failedPublishers = new ArrayList<>();
                for (AmqpPublisher publisher : this.publishers) {
                  try {
                    publisher.recoverAfterConnectionFailure();
                    publisher.state(OPEN);
                  } catch (Exception ex) {
                    LOGGER.warn("Error while trying to recover publisher", ex);
                    failedPublishers.add(publisher);
                  }
                }
                failedPublishers.forEach(AmqpPublisher::close);
                break;
              } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.info("Thread interrupted while waiting for connection opening");
                this.closed.set(true);
                this.changeStateOfPublishers(CLOSED);
                this.state(CLOSED, ex);
              } catch (Exception ex) {
                LOGGER.info("Error while trying to recover connection", ex);
                // TODO determine whether we should recover after the exception
              }
            }
          }
        };
    resultReference.set(result);
    return result;
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
    checkOpen();
    Session result = this.nativeSession;
    if (result == null) {
      synchronized (this) {
        result = this.nativeSession;
        if (result == null) {
          checkOpen();
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
    return consumer;
  }

  void removeConsumer(AmqpConsumer consumer) {
    this.consumers.remove(consumer);
  }

  private void changeStateOfPublishers(State newState) {
    this.changeStateOfResources(this.publishers, newState);
  }

  private void changeStateOfConsumers(State newState) {
    this.changeStateOfResources(this.consumers, newState);
  }

  private void changeStateOfResources(List<? extends ResourceBase> resources, State newState) {
    resources.forEach(r -> r.state(newState));
  }
}
