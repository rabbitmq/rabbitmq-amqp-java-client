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

import com.rabbitmq.model.BackOffDelayPolicy;
import com.rabbitmq.model.Connection;
import com.rabbitmq.model.ConnectionBuilder;
import com.rabbitmq.model.Resource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

class AmqpConnectionBuilder implements ConnectionBuilder {

  private final AmqpEnvironment environment;
  private final AmqpRecoveryConfiguration recoveryConfiguration =
      new AmqpRecoveryConfiguration(this);
  private final List<Resource.StateListener> listeners = new ArrayList<>();
  private String name;

  AmqpConnectionBuilder(AmqpEnvironment environment) {
    this.environment = environment;
  }

  @Override
  public ConnectionBuilder listeners(Resource.StateListener... listeners) {
    if (listeners == null || listeners.length == 0) {
      this.listeners.clear();
    } else {
      this.listeners.addAll(List.of(listeners));
    }
    return this;
  }

  @Override
  public RecoveryConfiguration recovery() {
    this.recoveryConfiguration.activated(true);
    return this.recoveryConfiguration;
  }

  @Override
  public Connection build() {
    // TODO copy the recovery configuration to keep the settings
    AmqpConnection connection = new AmqpConnection(this);
    this.environment.addConnection(connection);
    return connection;
  }

  AmqpConnectionBuilder name(String name) {
    this.name = name;
    return this;
  }

  AmqpEnvironment environment() {
    return environment;
  }

  AmqpRecoveryConfiguration recoveryConfiguration() {
    return recoveryConfiguration;
  }

  String name() {
    return name;
  }

  List<Resource.StateListener> listeners() {
    return listeners;
  }

  static class AmqpRecoveryConfiguration implements RecoveryConfiguration {

    private final AmqpConnectionBuilder connectionBuilder;
    private boolean activated = true;
    private boolean topology = true;
    private BackOffDelayPolicy backOffDelayPolicy = BackOffDelayPolicy.fixed(Duration.ofSeconds(5));

    AmqpRecoveryConfiguration(AmqpConnectionBuilder connectionBuilder) {
      this.connectionBuilder = connectionBuilder;
    }

    @Override
    public AmqpRecoveryConfiguration activated(boolean activated) {
      this.activated = activated;
      return this;
    }

    @Override
    public AmqpRecoveryConfiguration backOffDelayPolicy(BackOffDelayPolicy backOffDelayPolicy) {
      this.backOffDelayPolicy = backOffDelayPolicy;
      return this;
    }

    @Override
    public RecoveryConfiguration topology(boolean activated) {
      this.topology = true;
      return this;
    }

    @Override
    public ConnectionBuilder connectionBuilder() {
      return this.connectionBuilder;
    }

    boolean activated() {
      return this.activated;
    }

    boolean topology() {
      return this.topology;
    }

    BackOffDelayPolicy backOffDelayPolicy() {
      return this.backOffDelayPolicy;
    }
  }
}
