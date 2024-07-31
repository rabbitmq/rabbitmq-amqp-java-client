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
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;

@TestUtils.DisabledIfNotCluster
public class ClusterTest {

  Environment environment;
  Connection connection;
  Management management;
  String name;

  @BeforeEach
  void init(TestInfo info) {
    this.name = TestUtils.name(info);
    environment =
        new AmqpEnvironmentBuilder()
            .connectionSettings()
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

  @Test
  void connectionsShouldBeMemberLocalForQuorumQueue() {
    try {
      management.queue(name).type(Management.QueueType.QUORUM).declare();
      AmqpConnection publishConnection =
          connection(b -> b.affinity().queue(name).operation(PUBLISH));
      AmqpConnection consumeConnection =
          connection(b -> b.affinity().queue(name).operation(CONSUME));
      Management.QueueInfo info = connection.management().queueInfo(name);
      assertThat(publishConnection.connectionNodename()).isEqualTo(info.leader());
      assertThat(consumeConnection.connectionNodename())
          .isIn(info.replicas())
          .isNotEqualTo(info.leader());
    } finally {
      management.queueDeletion().delete(name);
    }
  }

  AmqpConnection connection(Consumer<ConnectionBuilder> operation) {
    ConnectionBuilder builder = environment.connectionBuilder();
    operation.accept(builder);
    return (AmqpConnection) builder.build();
  }
}
