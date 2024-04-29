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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

class AmqpConnectionBuilder implements ConnectionBuilder {

  private final AmqpEnvironment environment;
  private final AmqpRecoveryConfiguration recoveryConfiguration =
      new AmqpRecoveryConfiguration(this);
  private final DefaultConnectionSettings<AmqpConnectionBuilder> connectionSettings =
      new AmqpConnectionBuilderConnectionSettings(this);
  private final List<Resource.StateListener> listeners = new ArrayList<>();
  private String name;
  private TopologyListener topologyListener;

  AmqpConnectionBuilder(AmqpEnvironment environment) {
    this.environment = environment;
    this.environment.connectionSettings().copyTo(this.connectionSettings);
  }

  @Override
  public ConnectionBuilder uri(String uri) {
    return this.connectionSettings.uri(uri);
  }

  @Override
  public ConnectionBuilder uris(String... uris) {
    return this.connectionSettings.uris(uris);
  }

  @Override
  public ConnectionBuilder username(String username) {
    return this.connectionSettings.username(username);
  }

  @Override
  public ConnectionBuilder password(String password) {
    return this.connectionSettings.password(password);
  }

  @Override
  public ConnectionBuilder host(String host) {
    return this.connectionSettings.host(host);
  }

  @Override
  public ConnectionBuilder port(int port) {
    return this.connectionSettings.port(port);
  }

  @Override
  public ConnectionBuilder virtualHost(String virtualHost) {
    return this.connectionSettings.virtualHost(virtualHost);
  }

  @Override
  public ConnectionBuilder credentialsProvider(CredentialsProvider credentialsProvider) {
    return this.connectionSettings.credentialsProvider(credentialsProvider);
  }

  @Override
  public ConnectionBuilder idleTimeout(Duration idleTimeout) {
    return this.connectionSettings.idleTimeout(idleTimeout);
  }

  @Override
  public ConnectionBuilder addressSelector(Function<List<Address>, Address> selector) {
    return this.connectionSettings.addressSelector(selector);
  }

  @Override
  public TlsSettings<? extends ConnectionBuilder> tls() {
    return this.connectionSettings.tls();
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

  AmqpConnectionBuilder topologyListener(TopologyListener topologyListener) {
    this.topologyListener = topologyListener;
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

  TopologyListener topologyListener() {
    return this.topologyListener;
  }

  List<Resource.StateListener> listeners() {
    return listeners;
  }

  DefaultConnectionSettings<AmqpConnectionBuilder> connectionSettings() {
    return this.connectionSettings;
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
      this.topology = activated;
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

  static class AmqpConnectionBuilderConnectionSettings
      extends DefaultConnectionSettings<AmqpConnectionBuilder> {

    private final AmqpConnectionBuilder builder;

    private AmqpConnectionBuilderConnectionSettings(AmqpConnectionBuilder builder) {
      this.builder = builder;
    }

    @Override
    AmqpConnectionBuilder toReturn() {
      return this.builder;
    }

    @Override
    public TlsSettings<AmqpConnectionBuilder> tls() {
      return super.tls();
    }
  }
}
