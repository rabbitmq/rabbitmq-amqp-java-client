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

import static com.rabbitmq.client.amqp.Resource.State.CLOSED;
import static com.rabbitmq.client.amqp.Resource.State.CLOSING;
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.Resource.State.OPENING;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.Address;
import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.ConnectionBuilder;
import com.rabbitmq.client.amqp.ConnectionSettings;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Resource;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

@AmqpTestInfrastructure
public class AffinityTest {

  Environment environment;
  Connection connection;
  Management management;
  String name;

  @BeforeEach
  void init(TestInfo info) {
    this.name = TestUtils.name(info);
    management = connection.management();
  }

  @ParameterizedTest
  @EnumSource
  void sameConnectionShouldBeReturnedIfSameAffinityAndReuseActivated(
      Management.QueueType queueType) {
    management.queue(name).type(queueType).declare();
    environment
        .connectionBuilder()
        .affinity()
        .queue(name)
        .operation(ConnectionSettings.Affinity.Operation.PUBLISH)
        .connection()
        .build();
    try (AmqpConnection c1 = connection(b -> b.affinity().queue(name));
        AmqpConnection c2 = connection(b -> b.affinity().queue(name));
        AmqpConnection c3 = connection(b -> b.affinity().queue(name).reuse(true))) {
      assertThat(c1.id()).isNotEqualTo(c2.id());
      assertThat(c3.id()).isIn(c1.id(), c2.id());
    } finally {
      management.queueDelete(name);
    }
  }

  @Test
  void connectionShouldSucceedEvenIfAffinityQueueDoesNotExist() {
    try (Connection c =
        environment.connectionBuilder().affinity().queue("does not exist").connection().build()) {
      // testing the connection
      c.publisherBuilder().exchange("amq.fanout").build();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void connectionShouldNotRecoverWhenFallingBackFromUnreachableLeader(boolean recoveryActivated)
      throws Exception {
    management.queue(name).type(Management.QueueType.QUORUM).declare();
    try (AmqpEnvironment env = (AmqpEnvironment) TestUtils.environmentBuilder().build()) {
      Management.QueueInfo info = management.queueInfo(name);
      env.affinityCache()
          .queueInfo(info)
          .nodenameToAddress(info.leader(), new Address("localhost", freePort()));
      List<Resource.State> states = new CopyOnWriteArrayList<>();
      try (AmqpConnection c =
          (AmqpConnection)
              env.connectionBuilder()
                  .listeners(ctx -> states.add(ctx.currentState()))
                  .affinity()
                  .queue(name)
                  .operation(ConnectionSettings.Affinity.Operation.PUBLISH)
                  .connection()
                  .recovery()
                  .activated(recoveryActivated)
                  .connectionBuilder()
                  .build()) {
        waitAtMost(() -> states.contains(OPEN));
        // the disconnection of the unreachable native connection is notified asynchronously
        Thread.sleep(500);
        assertThat(states).containsExactly(OPENING, OPEN);
        assertThat(c.state()).isEqualTo(OPEN);
        assertThat(c).isOnLeader(info);
      }
    } finally {
      management.queueDelete(name);
    }
  }

  @Test
  void connectionShouldBeClosedAndNotRecoverWhenCreationFails() throws Exception {
    List<Resource.State> states = new CopyOnWriteArrayList<>();
    String uri = "amqp://localhost:" + freePort();
    assertThatThrownBy(() ->
            environment
                .connectionBuilder()
                .uri(uri)
                .listeners(ctx -> states.add(ctx.currentState()))
                .affinity()
                .queue(name)
                .connection()
                .build())
        .isInstanceOf(AmqpException.AmqpConnectionException.class);
    waitAtMost(() -> states.contains(CLOSED));
    // the disconnection of the unreachable native connection is notified asynchronously
    Thread.sleep(500);
    assertThat(states).containsExactly(OPENING, CLOSING, CLOSED);
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  AmqpConnection connection(Consumer<ConnectionBuilder> operation) {
    ConnectionBuilder builder = environment.connectionBuilder();
    operation.accept(builder);
    return (AmqpConnection) builder.build();
  }
}
