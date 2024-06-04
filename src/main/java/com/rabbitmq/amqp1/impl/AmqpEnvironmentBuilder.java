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
package com.rabbitmq.amqp1.impl;

import com.rabbitmq.amqp1.ConnectionSettings;
import com.rabbitmq.amqp1.Environment;
import com.rabbitmq.amqp1.EnvironmentBuilder;
import com.rabbitmq.amqp1.ObservationCollector;
import com.rabbitmq.amqp1.metrics.MetricsCollector;
import com.rabbitmq.amqp1.metrics.NoOpMetricsCollector;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.ExecutorService;

public class AmqpEnvironmentBuilder implements EnvironmentBuilder {

  private final DefaultEnvironmentConnectionSettings connectionSettings =
      new DefaultEnvironmentConnectionSettings(this);
  private ExecutorService executorService;
  private MetricsCollector metricsCollector = NoOpMetricsCollector.INSTANCE;
  private ObservationCollector observationCollector = Utils.NO_OP_OBSERVATION_COLLECTOR;

  public AmqpEnvironmentBuilder() {}

  public AmqpEnvironmentBuilder executorService(ExecutorService executorService) {
    this.executorService = executorService;
    return this;
  }

  public AmqpEnvironmentBuilder metricsCollector(MetricsCollector metricsCollector) {
    this.metricsCollector = metricsCollector;
    return this;
  }

  public AmqpEnvironmentBuilder observationCollector(ObservationCollector observationCollector) {
    this.observationCollector = observationCollector;
    return this;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public EnvironmentConnectionSettings connectionSettings() {
    return this.connectionSettings;
  }

  @Override
  public Environment build() {
    return new AmqpEnvironment(
        executorService, connectionSettings, metricsCollector, observationCollector);
  }

  public interface EnvironmentConnectionSettings
      extends ConnectionSettings<EnvironmentConnectionSettings> {

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
