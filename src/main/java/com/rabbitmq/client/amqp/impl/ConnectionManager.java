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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

  private final AmqpEnvironment environment;
  private final Lock connectionsLock = new ReentrantLock();
  private final Set<AmqpConnection> connections = new HashSet<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  ConnectionManager(AmqpEnvironment environment) {
    this.environment = environment;
  }

  AmqpConnection connection(AmqpConnectionBuilder builder) {
    AmqpConnection connection = null;
    if (builder.connectionSettings().affinity().activated()
        && builder.connectionSettings().affinity().reuse()) {
      builder.connectionSettings().affinity().validate();

      ConnectionUtils.AffinityContext affinity =
          new ConnectionUtils.AffinityContext(
              builder.connectionSettings().affinity().queue(),
              builder.connectionSettings().affinity().operation());
      connection =
          doOnConnections(
              conns -> {
                return this.connections.stream()
                    .filter(c -> affinity.equals(c.affinity()))
                    .findAny()
                    .orElse(null);
              });
    }
    if (connection == null) {
      AmqpConnectionBuilder copy = new AmqpConnectionBuilder(this.environment);
      builder.copyTo(copy);
      connection = new AmqpConnection(copy);
    }
    AtomicReference<AmqpConnection> connectionReference = new AtomicReference<>(connection);
    doOnConnections(
        conns -> {
          conns.add(connectionReference.get());
        });
    return connection;
  }

  void remove(AmqpConnection connection) {
    doOnConnections(
        conns -> {
          if (!closed.get()) {
            conns.remove(connection);
          }
        });
  }

  void close() {
    if (this.closed.compareAndSet(false, true)) {
      for (AmqpConnection connection : this.connections) {
        try {
          connection.close();
        } catch (Exception e) {
          LOGGER.warn("Error while closing connection", e);
        }
      }
      this.connections.clear();
    }
  }

  private void doOnConnections(Consumer<Set<AmqpConnection>> operation) {
    this.connectionsLock.lock();
    try {
      operation.accept(this.connections);
    } finally {
      this.connectionsLock.unlock();
    }
  }

  private <T> T doOnConnections(Function<Set<AmqpConnection>, T> operation) {
    this.connectionsLock.lock();
    try {
      return operation.apply(this.connections);
    } finally {
      this.connectionsLock.unlock();
    }
  }
}
