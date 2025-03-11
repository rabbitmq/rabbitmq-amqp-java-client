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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpEnvironment implements Environment {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpEnvironment.class);
  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private final Client client;
  private final DefaultConnectionSettings<?> connectionSettings =
      DefaultConnectionSettings.instance();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final boolean internalExecutor;
  private final boolean internalScheduledExecutor;
  private final boolean internalPublisherExecutor;
  private final ExecutorService executorService;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ExecutorService dispatchingExecutorService;
  private final ExecutorService publisherExecutorService;
  private final ConnectionManager connectionManager = new ConnectionManager(this);
  private final long id;
  private final Clock clock = new Clock();
  private volatile ScheduledFuture<?> clockRefreshFuture;
  private final AtomicBoolean clockRefreshSet = new AtomicBoolean(false);
  private final MetricsCollector metricsCollector;
  private final ObservationCollector observationCollector;
  private final ConnectionUtils.AffinityCache affinityCache = new ConnectionUtils.AffinityCache();
  private final EventLoop recoveryEventLoop;
  private final ExecutorService recoveryEventLoopExecutorService;
  private final CredentialsManagerFactory credentialsManagerFactory =
      new CredentialsManagerFactory(this);

  AmqpEnvironment(
      ExecutorService executorService,
      ScheduledExecutorService scheduledExecutorService,
      ExecutorService dispatchingExecutorService,
      ExecutorService publisherExecutorService,
      DefaultConnectionSettings<?> connectionSettings,
      MetricsCollector metricsCollector,
      ObservationCollector observationCollector) {
    this.id = ID_SEQUENCE.getAndIncrement();
    connectionSettings.copyTo(this.connectionSettings);
    this.connectionSettings.consolidate();
    ClientOptions clientOptions = new ClientOptions();
    this.client = Client.create(clientOptions);

    String threadPrefix = String.format("rabbitmq-amqp-environment-%d-", this.id);
    if (executorService == null) {
      this.executorService = Executors.newCachedThreadPool(Utils.threadFactory(threadPrefix));
      this.internalExecutor = true;
    } else {
      this.executorService = executorService;
      this.internalExecutor = false;
    }
    if (scheduledExecutorService == null) {
      this.scheduledExecutorService =
          Executors.newScheduledThreadPool(1, Utils.threadFactory(threadPrefix + "scheduler-"));
      this.internalScheduledExecutor = true;
    } else {
      this.scheduledExecutorService = scheduledExecutorService;
      this.internalScheduledExecutor = false;
    }
    this.dispatchingExecutorService = dispatchingExecutorService;
    if (publisherExecutorService == null) {
      this.publisherExecutorService = Utils.executorService(threadPrefix);
      this.internalPublisherExecutor = true;
    } else {
      this.publisherExecutorService = publisherExecutorService;
      this.internalPublisherExecutor = false;
    }
    this.metricsCollector =
        metricsCollector == null ? NoOpMetricsCollector.INSTANCE : metricsCollector;
    this.observationCollector =
        observationCollector == null ? Utils.NO_OP_OBSERVATION_COLLECTOR : observationCollector;
    this.recoveryEventLoopExecutorService =
        new ThreadPoolExecutor(
            1,
            1,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            Utils.threadFactory(threadPrefix + "event-loop-"));
    this.recoveryEventLoop = new EventLoop(this.recoveryEventLoopExecutorService);
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

  CredentialsManagerFactory credentialsManagerFactory() {
    return this.credentialsManagerFactory;
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      LOGGER.debug("Closing environment {}", this);
      this.connectionManager.close();
      this.client.close();
      this.recoveryEventLoop.close();
      this.recoveryEventLoopExecutorService.shutdownNow();
      if (this.internalExecutor) {
        this.executorService.shutdownNow();
      }
      if (this.internalScheduledExecutor) {
        this.scheduledExecutorService.shutdownNow();
      }
      if (this.internalPublisherExecutor) {
        this.publisherExecutorService.shutdownNow();
      }
      if (this.clockRefreshFuture != null) {
        this.clockRefreshFuture.cancel(false);
      }
      this.scheduledExecutorService.shutdownNow();
      LOGGER.debug("Environment {} has been closed", this);
    }
  }

  @Override
  public ConnectionBuilder connectionBuilder() {
    return new AmqpConnectionBuilder(this);
  }

  ExecutorService executorService() {
    return this.executorService;
  }

  ExecutorService dispatchingExecutorService() {
    return this.dispatchingExecutorService;
  }

  ExecutorService publisherExecutorService() {
    return this.publisherExecutorService;
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

  EventLoop recoveryEventLoop() {
    return this.recoveryEventLoop;
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
