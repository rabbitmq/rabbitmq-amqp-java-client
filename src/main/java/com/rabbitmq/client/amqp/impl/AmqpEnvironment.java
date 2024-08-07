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

import com.rabbitmq.client.amqp.ConnectionBuilder;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.ObservationCollector;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import com.rabbitmq.client.amqp.metrics.NoOpMetricsCollector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.qpid.protonj2.client.*;

class AmqpEnvironment implements Environment {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private final Client client;
  private final ExecutorService executorService;
  private final DefaultConnectionSettings<?> connectionSettings =
      DefaultConnectionSettings.instance();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final boolean internalExecutor;
  private final ConnectionManager connectionManager = new ConnectionManager(this);
  private final long id;
  private final Clock clock = new Clock();
  private final ScheduledExecutorService scheduledExecutorService;
  private volatile ScheduledFuture<?> clockRefreshFuture;
  private final AtomicBoolean clockRefreshSet = new AtomicBoolean(false);
  private final MetricsCollector metricsCollector;
  private final ObservationCollector observationCollector;
  private ConnectionUtils.AffinityCache affinityCache = new ConnectionUtils.AffinityCache();

  AmqpEnvironment(
      ExecutorService executorService,
      DefaultConnectionSettings<?> connectionSettings,
      MetricsCollector metricsCollector,
      ObservationCollector observationCollector) {
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
    this.scheduledExecutorService =
        Executors.newScheduledThreadPool(0, Utils.defaultThreadFactory());
    this.metricsCollector =
        metricsCollector == null ? NoOpMetricsCollector.INSTANCE : metricsCollector;
    this.observationCollector =
        observationCollector == null ? Utils.NO_OP_OBSERVATION_COLLECTOR : observationCollector;
  }

  DefaultConnectionSettings<?> connectionSettings() {
    return this.connectionSettings;
  }

  Client client() {
    return this.client;
  }

  Clock clock() {
    if (this.clockRefreshSet.compareAndSet(false, true)) {
      this.clockRefreshFuture =
          this.scheduledExecutorService.scheduleAtFixedRate(
              this.clock::refresh, 1, 1, TimeUnit.SECONDS);
    }
    return this.clock;
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.connectionManager.close();
      this.client.close();
      if (this.internalExecutor) {
        this.executorService.shutdownNow();
      }
      if (this.clockRefreshFuture != null) {
        this.clockRefreshFuture.cancel(false);
      }
      this.scheduledExecutorService.shutdownNow();
    }
  }

  @Override
  public ConnectionBuilder connectionBuilder() {
    return new AmqpConnectionBuilder(this);
  }

  ExecutorService executorService() {
    return this.executorService;
  }

  ScheduledExecutorService scheduledExecutorService() {
    return this.scheduledExecutorService;
  }

  MetricsCollector metricsCollector() {
    return this.metricsCollector;
  }

  ObservationCollector observationCollector() {
    return this.observationCollector;
  }

  ConnectionUtils.AffinityCache affinityCache() {
    return this.affinityCache;
  }

  AmqpConnection connection(AmqpConnectionBuilder builder) {
    return this.connectionManager.connection(builder);
  }

  void removeConnection(AmqpConnection connection) {
    this.connectionManager.remove(connection);
  }

  @Override
  public String toString() {
    return "rabbitmq-amqp-" + this.id;
  }
}
