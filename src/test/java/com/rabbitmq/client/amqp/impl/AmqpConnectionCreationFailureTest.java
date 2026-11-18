// Copyright (c) 2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.client.amqp.Resource.State.OPENING;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.metrics.MicrometerMetricsCollector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AmqpConnectionCreationFailureTest {

  SimpleMeterRegistry registry;
  Environment environment;
  List<Resource.State> states;
  List<AmqpConnection> connections;

  @BeforeEach
  void init() {
    registry = new SimpleMeterRegistry();
    environment =
        new AmqpEnvironmentBuilder()
            .metricsCollector(new MicrometerMetricsCollector(registry))
            .build();
    states = new CopyOnWriteArrayList<>();
    connections = new CopyOnWriteArrayList<>();
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void listenersShouldOnlyBeNotifiedOfOpeningAndConnectionShouldNotRecover(
      boolean recoveryActivated) throws Exception {
    AmqpConnection c = failedConnection(recoveryActivated);
    // no state change, so no other notification
    assertThat(c.state()).isEqualTo(OPENING);
    assertThat(c.stateClientClosed()).isTrue();
    assertThat(states).containsExactly(OPENING);
    assertThat(openConnections()).isZero();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void closingConnectionWhoseCreationFailedShouldBeANoOp(boolean recoveryActivated)
      throws Exception {
    AmqpConnection c = failedConnection(recoveryActivated);
    c.close();
    assertThat(c.state()).isEqualTo(OPENING);
    assertThat(states).containsExactly(OPENING);
    assertThat(openConnections()).isZero();
  }

  private AmqpConnection failedConnection(boolean recoveryActivated) throws IOException {
    String uri = "amqp://localhost:" + freePort();
    assertThatThrownBy(() ->
            environment
                .connectionBuilder()
                .uri(uri)
                .listeners(ctx -> {
                  connections.add((AmqpConnection) ctx.resource());
                  states.add(ctx.currentState());
                })
                .recovery()
                .activated(recoveryActivated)
                .connectionBuilder()
                .build())
        .isInstanceOf(AmqpException.AmqpConnectionException.class);
    waitAtMost(() -> states.contains(OPENING));
    return connections.get(0);
  }

  private double openConnections() {
    return registry.get("rabbitmq.amqp.connections").gauge().value();
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
