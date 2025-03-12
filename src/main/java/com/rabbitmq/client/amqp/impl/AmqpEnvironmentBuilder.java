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
import com.rabbitmq.client.amqp.ConnectionSettings;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.EnvironmentBuilder;
import com.rabbitmq.client.amqp.ObservationCollector;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import com.rabbitmq.client.amqp.metrics.NoOpMetricsCollector;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/** Builder to create an {@link Environment} instance of the RabbitMQ AMQP 1.0 Java Client. */
public class AmqpEnvironmentBuilder implements EnvironmentBuilder {

  private final DefaultEnvironmentConnectionSettings connectionSettings =
      new DefaultEnvironmentConnectionSettings(this);
  private ExecutorService executorService;
  private ScheduledExecutorService scheduledExecutorService;
  private Executor dispatchingExecutor;
  private ExecutorService publisherExecutorService;
  private MetricsCollector metricsCollector = NoOpMetricsCollector.INSTANCE;
  private ObservationCollector observationCollector = Utils.NO_OP_OBSERVATION_COLLECTOR;

  public AmqpEnvironmentBuilder() {}

  /**
   * Set executor service used for internal tasks (e.g. connection recovery).
   *
   * <p>The library uses sensible defaults, override only in case of problems.
   *
   * @param executorService the executor service
   * @return this builder instance
   */
  public AmqpEnvironmentBuilder executorService(ExecutorService executorService) {
    this.executorService = executorService;
    return this;
  }

  /**
   * Set the shared executor to use for incoming message delivery in this environment instance
   * connections.
   *
   * <p>There is no shared executor by default, each connection uses its own, see {@link
   * ConnectionBuilder#dispatchingExecutor(Executor)}.
   *
   * <p>It is the developer's responsibility to shut down the executor when it is no longer needed.
   *
   * @param executor the executor for incoming message delivery
   * @return this builder instance
   * @see ConnectionBuilder#dispatchingExecutor(Executor)
   */
  public AmqpEnvironmentBuilder dispatchingExecutor(Executor executor) {
    this.dispatchingExecutor = executor;
    return this;
  }

  /**
   * Set scheduled executor service used for internal tasks (e.g. connection recovery).
   *
   * <p>The library uses sensible defaults, override only in case of problems.
   *
   * @param scheduledExecutorService the scheduled executor service
   * @return this builder instance
   */
  public AmqpEnvironmentBuilder scheduledExecutorService(
      ScheduledExecutorService scheduledExecutorService) {
    this.scheduledExecutorService = scheduledExecutorService;
    return this;
  }

  /**
   * Set executor service used to deal with broker responses after processing outbound messages.
   *
   * <p>The library uses sensible defaults, override only in case of problems.
   *
   * @param publisherExecutorService the executor service
   * @return this builder instance
   */
  public AmqpEnvironmentBuilder publisherExecutorService(ExecutorService publisherExecutorService) {
    this.publisherExecutorService = publisherExecutorService;
    return this;
  }

  /**
   * Deprecated, do not use anymore. Consumers do not use a polling loop anymore.
   *
   * <p>Set executor service used for consumer loops.
   *
   * <p>The library uses sensible defaults, override only in case of problems.
   *
   * @param consumerExecutorService the executor service
   * @return this builder instance
   * @deprecated Do not use anymore
   */
  @Deprecated(forRemoval = true)
  public AmqpEnvironmentBuilder consumerExecutorService(ExecutorService consumerExecutorService) {
    return this;
  }

  /**
   * Set up a {@link MetricsCollector}.
   *
   * @param metricsCollector the metrics collector
   * @return this builder instance
   * @see com.rabbitmq.client.amqp.metrics.MicrometerMetricsCollector
   */
  public AmqpEnvironmentBuilder metricsCollector(MetricsCollector metricsCollector) {
    this.metricsCollector = metricsCollector;
    return this;
  }

  /**
   * Set up an {@link ObservationCollector}.
   *
   * @param observationCollector the observation collector
   * @return this builder instance
   * @see com.rabbitmq.client.amqp.observation.micrometer.MicrometerObservationCollectorBuilder
   */
  public AmqpEnvironmentBuilder observationCollector(ObservationCollector observationCollector) {
    this.observationCollector = observationCollector;
    return this;
  }

  /**
   * Returns connection settings shared by connection builders.
   *
   * <p>These settings are optional, they are overridden with the appropriate methods in {@link
   * com.rabbitmq.client.amqp.ConnectionBuilder}.
   *
   * @return shared connection settings
   * @see ConnectionSettings
   * @see com.rabbitmq.client.amqp.ConnectionBuilder
   */
  @SuppressFBWarnings("EI_EXPOSE_REP")
  public EnvironmentConnectionSettings connectionSettings() {
    return this.connectionSettings;
  }

  /**
   * Create the environment instance.
   *
   * @return the configured environment
   */
  @Override
  public Environment build() {
    return new AmqpEnvironment(
        executorService,
        scheduledExecutorService,
        dispatchingExecutor,
        publisherExecutorService,
        connectionSettings,
        metricsCollector,
        observationCollector);
  }

  /** Common settings for connections created by an environment instance. */
  public interface EnvironmentConnectionSettings
      extends ConnectionSettings<EnvironmentConnectionSettings> {

    /**
     * The owning environment builder.
     *
     * @return builder instance
     */
    AmqpEnvironmentBuilder environmentBuilder();
  }

  static class DefaultEnvironmentConnectionSettings
      extends DefaultConnectionSettings<EnvironmentConnectionSettings>
      implements EnvironmentConnectionSettings {

    private final AmqpEnvironmentBuilder builder;

    public DefaultEnvironmentConnectionSettings(AmqpEnvironmentBuilder builder) {
      this.builder = builder;
    }

    @Override
    EnvironmentConnectionSettings toReturn() {
      return this;
    }

    @Override
    public AmqpEnvironmentBuilder environmentBuilder() {
      return this.builder;
    }
  }
}
