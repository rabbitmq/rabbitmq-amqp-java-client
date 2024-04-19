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

import com.rabbitmq.model.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.qpid.protonj2.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpEnvironment implements Environment {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private final Logger LOGGER = LoggerFactory.getLogger(AmqpEnvironment.class);

  private final Client client;
  private final ExecutorService executorService;
  private final DefaultConnectionSettings<?> connectionSettings =
      DefaultConnectionSettings.instance();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final boolean internalExecutor;
  private final List<AmqpConnection> connections = Collections.synchronizedList(new ArrayList<>());
  private final long id;

  AmqpEnvironment(
      ExecutorService executorService, DefaultConnectionSettings<?> connectionSettings) {
    this.id = ID_SEQUENCE.getAndIncrement();
    connectionSettings.copyTo(this.connectionSettings);
    this.connectionSettings.consolidate();
    ClientOptions clientOptions = new ClientOptions();
    this.client = Client.create(clientOptions);

    if (executorService == null) {
      this.executorService = Utils.executorService("rabbitmq-amqp-environment-%d-", this.id);
      this.internalExecutor = true;
    } else {
      this.executorService = executorService;
      this.internalExecutor = false;
    }
  }

  DefaultConnectionSettings<?> connectionSettings() {
    return this.connectionSettings;
  }

  Client client() {
    return this.client;
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      for (AmqpConnection connection : this.connections) {
        try {
          connection.close();
        } catch (Exception e) {
          LOGGER.warn("Error while closing connection", e);
        }
      }
      this.client.close();
      if (this.internalExecutor) {
        this.executorService.shutdownNow();
      }
    }
  }

  @Override
  public ConnectionBuilder connectionBuilder() {
    return new AmqpConnectionBuilder(this);
  }

  ExecutorService executorService() {
    return this.executorService;
  }

  void addConnection(AmqpConnection connection) {
    this.connections.add(connection);
  }

  @Override
  public String toString() {
    return "rabbitmq-amqp-" + this.id;
  }
}