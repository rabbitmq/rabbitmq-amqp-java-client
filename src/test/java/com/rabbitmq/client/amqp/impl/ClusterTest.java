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

import static com.rabbitmq.client.amqp.ConnectionSettings.Affinity.Operation.CONSUME;
import static com.rabbitmq.client.amqp.ConnectionSettings.Affinity.Operation.PUBLISH;
import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@TestUtils.DisabledIfNotCluster
public class ClusterTest {

  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = BackOffDelayPolicy.fixed(ofMillis(100));
  Environment environment;
  Connection connection;
  Management management;
  String q, name;

  @BeforeEach
  void init(TestInfo info) {
    this.q = TestUtils.name(info);
    this.name = TestUtils.name(info);
    environment =
        new AmqpEnvironmentBuilder()
            .connectionSettings()
            .addressSelector(new RoundRobinAddressSelector())
            .uris("amqp://localhost:5672", "amqp://localhost:5673", "amqp://localhost:5674")
            .environmentBuilder()
            .build();
    this.connection = environment.connectionBuilder().build();
    this.management = connection.management();
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  @EnumSource(names = {"QUORUM", "STREAM"})
  @ParameterizedTest
  void connectionsShouldBeMemberLocalReplicatedQueues(Management.QueueType type) {
    try {
      management.queue(q).type(type).declare();
      AmqpConnection consumeConnection = connection(b -> b.affinity().queue(q).operation(CONSUME));
      AmqpConnection publishConnection = connection(b -> b.affinity().queue(q).operation(PUBLISH));
      Management.QueueInfo info = connection.management().queueInfo(q);
      assertThat(publishConnection.connectionNodename()).isEqualTo(info.leader());
      assertThat(consumeConnection.connectionNodename())
          .isIn(info.replicas())
          .isNotEqualTo(info.leader());
      assertThat(Cli.listConnections()).hasSize(3);
    } finally {
      management.queueDeletion().delete(q);
    }
  }

  @Test
  void connectionShouldRecoverToNewQuorumQueueLeaderAfterAfterItHasMoved() {
    try {
      management.queue(q).type(Management.QueueType.QUORUM).declare();
      Management.QueueInfo info = queueInfo();
      String initialLeader = info.leader();

      TestUtils.Sync recoveredSync = TestUtils.sync();
      AmqpConnection publishConnection =
          connection(
              b ->
                  b.name(name)
                      .listeners(
                          context -> {
                            if (context.previousState() == Resource.State.RECOVERING
                                && context.currentState() == Resource.State.OPEN) {
                              recoveredSync.down();
                            }
                          })
                      .affinity()
                      .queue(q)
                      .operation(PUBLISH));
      assertThat(publishConnection.connectionNodename()).isEqualTo(initialLeader);

      int initialReplicaCount = info.replicas().size();
      Cli.deleteQuorumQueueMember(q, initialLeader);
      TestUtils.waitAtMost(() -> !queueInfo().leader().equals(initialLeader));
      assertThat(queueInfo().replicas()).hasSize(initialReplicaCount - 1);
      Cli.addQuorumQueueMember(q, initialLeader);
      TestUtils.waitAtMost(() -> queueInfo().replicas().size() == initialReplicaCount);
      info = queueInfo();
      TestUtils.assertThat(info).doesNotHaveLeader(initialLeader);
      String newLeader = info.leader();

      Cli.closeConnection(name);
      TestUtils.assertThat(recoveredSync).completes();
      assertThat(publishConnection.connectionNodename()).isEqualTo(newLeader);
    } finally {
      management.queueDeletion().delete(q);
    }
  }

  Management.QueueInfo queueInfo() {
    return this.management.queueInfo(q);
  }

  AmqpConnection connection(Consumer<AmqpConnectionBuilder> operation) {
    AmqpConnectionBuilder builder = (AmqpConnectionBuilder) environment.connectionBuilder();
    builder.recovery().backOffDelayPolicy(BACK_OFF_DELAY_POLICY);
    operation.accept(builder);
    return (AmqpConnection) builder.build();
  }

  private static class RoundRobinAddressSelector implements AddressSelector {

    private final AtomicInteger count = new AtomicInteger();

    @Override
    public Address select(List<Address> addresses) {
      if (addresses.isEmpty()) {
        throw new IllegalStateException("There should at least one node to connect to");
      } else if (addresses.size() == 1) {
        return addresses.get(0);
      } else {
        return addresses.get(count.getAndIncrement() % addresses.size());
      }
    }
  }
}
