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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DisconnectionEvent;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpConnection implements Connection {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConnection.class);
  private final AmqpEnvironment environment;
  private volatile AmqpManagement management;
  private final Lock managementLock = new ReentrantLock();
  private volatile org.apache.qpid.protonj2.client.Connection nativeConnection;
  private final AtomicReference<Resource.State> state = new AtomicReference<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final List<StateListener> listeners;

  AmqpConnection(AmqpConnectionBuilder builder) {
    this.environment = builder.environment();
    this.listeners = new ArrayList<>(builder.listeners());
    this.state(OPENING);

    Utils.ConnectionParameters connectionParameters = this.environment.connectionParameters();
    BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> disconnectHandler;
    AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration =
        builder.recoveryConfiguration();
    if (recoveryConfiguration.activated()) {
      disconnectHandler =
          recoverDisconnectHandler(recoveryConfiguration, connectionParameters, builder.name());
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
      recoverDisconnectHandler(
          AmqpConnectionBuilder.AmqpRecoveryConfiguration recoveryConfiguration,
          Utils.ConnectionParameters connectionParameters,
          String name) {
    AtomicReference<BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent>>
        resultReference = new AtomicReference<>();
    BiConsumer<org.apache.qpid.protonj2.client.Connection, DisconnectionEvent> result =
        (c, e) -> {
          // TODO consider using another thread for connection recovery
          // (we do not want to wait too long in a IO thread)
          // it should be dispatched as a task that could be stopped in #close()
          ModelException failureCause = convert(e.failureCause(), "Connection disconnected");
          this.state(RECOVERING, failureCause);
          this.nativeConnection = null;
          this.maybeReleaseManagementResources();
          int attempt = 0;
          while (true) {
            try {
              Thread.sleep(recoveryConfiguration.backOffDelayPolicy().delay(attempt).toMillis());
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              LOGGER.info("Thread interrupted while waiting during connection recovery");
              this.state(CLOSED, ex);
            }
            this.nativeConnection = connect(connectionParameters, name, resultReference.get());
            try {
              this.nativeConnection.openFuture().get();
              this.state(OPEN);
              break;
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              LOGGER.info("Thread interrupted while waiting for connection opening");
              this.state(CLOSED, ex);
            } catch (ExecutionException ex) {
              LOGGER.info("Error while trying to recover connection", ex);
              // TODO determine whether we should recover after the exception
            }
            break;
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

  org.apache.qpid.protonj2.client.Connection nativeConnection() {
    return this.nativeConnection;
  }

  ExecutorService executorService() {
    return this.environment.executorService();
  }

  private void state(Resource.State state) {
    this.state(state, null);
  }

  private void state(Resource.State state, Throwable failureCause) {
    Resource.State previousState = this.state.getAndSet(state);
    if (!this.listeners.isEmpty()) {
      Resource.Context context = new DefaultContext(this, failureCause, previousState, state);
      this.listeners.forEach(
          l -> {
            try {
              l.handle(context);
            } catch (Exception e) {
              LOGGER.warn("Error in resource listener", e);
            }
          });
    }
  }

  private void checkOpen() {
    if (this.state.get() != OPEN) {
      throw new ModelException(
          "Connection is not open, current state is %s", this.state.get().name());
    }
  }

  private static class DefaultContext implements Context {

    private final Resource resource;
    private final Throwable failureCause;
    private final State previousState;
    private final State currentState;

    private DefaultContext(
        Resource resource, Throwable failureCause, State previousState, State currentState) {
      this.resource = resource;
      this.failureCause = failureCause;
      this.previousState = previousState;
      this.currentState = currentState;
    }

    @Override
    public Resource resource() {
      return this.resource;
    }

    @Override
    public Throwable failureCause() {
      return this.failureCause;
    }

    @Override
    public State previousState() {
      return this.previousState;
    }

    @Override
    public State currentState() {
      return this.currentState;
    }
  }
}
