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

import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.*;
import java.util.List;
import org.junit.jupiter.api.*;

@DisabledIfNotCluster
public class RecoveryClusterTest {

  static final String[] URIS =
      new String[] {"amqp://localhost:5672", "amqp://localhost:5673", "amqp://localhost:5674"};
  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = BackOffDelayPolicy.fixed(ofSeconds(1));
  Environment environment;
  Connection connection;
  Management management;

  @BeforeEach
  void init() {
    environment =
        new AmqpEnvironmentBuilder().connectionSettings().uris(URIS).environmentBuilder().build();
    this.connection =
        environment
            .connectionBuilder()
            .recovery()
            .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
            .connectionBuilder()
            .build();
    this.management = connection.management();
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  @Test
  void clusterRestart(TestInfo info) {
    String name = TestUtils.name(info);
    try {
      Management.QueueInfo queueInfo =
          management.queue(name).type(Management.QueueType.QUORUM).declare();
      List<String> replicas = queueInfo.replicas();
      replicas.forEach(Cli::restartNode);

      waitAtMost(
          () -> {
            try {
              management.queueInfo(name);
              return true;
            } catch (Exception e) {
              return false;
            }
          });
      assertThat(management.queueInfo(name).replicas())
          .hasSameSizeAs(replicas)
          .containsExactlyInAnyOrderElementsOf(replicas);

    } finally {
      management.queueDeletion().delete(name);
    }
  }
}
