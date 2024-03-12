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

import static com.rabbitmq.model.BackOffDelayPolicy.fixed;
import static com.rabbitmq.model.Resource.State.OPEN;
import static com.rabbitmq.model.Resource.State.RECOVERING;
import static com.rabbitmq.model.amqp.Cli.closeConnection;
import static com.rabbitmq.model.amqp.TestUtils.CountDownLatchConditions.completed;
import static com.rabbitmq.model.amqp.TestUtils.name;
import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.model.BackOffDelayPolicy;
import com.rabbitmq.model.Connection;
import com.rabbitmq.model.Resource;
import com.rabbitmq.model.amqp.TestUtils.DisabledIfRabbitMqCtlNotSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@DisabledIfRabbitMqCtlNotSet
public class AmqpConnectionRecoveryTest {

  static AmqpEnvironment environment;
  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = fixed(ofMillis(100));

  @BeforeAll
  static void initAll() {
    environment = new AmqpEnvironment("amqp://guest:guest@localhost:5672/%2f", null);
  }

  @AfterAll
  static void afterAll() {
    environment.close();
  }

  @Test
  void closingConnectionShouldTriggerRecovery(TestInfo info) throws Exception {
    String q = name(info);
    String connectionName = UUID.randomUUID().toString();
    Map<Resource.State, CountDownLatch> stateLatches = new ConcurrentHashMap<>();
    stateLatches.put(RECOVERING, new CountDownLatch(1));
    stateLatches.put(OPEN, new CountDownLatch(2));
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .name(connectionName)
                .listeners(
                    context -> {
                      if (stateLatches.containsKey(context.currentState())) {
                        stateLatches.get(context.currentState()).countDown();
                      }
                    })
                .recovery()
                .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
                .connectionBuilder();
    try (Connection c = new AmqpConnection(builder)) {
      c.management().queue().name(q).exclusive(true).declare();
      closeConnection(connectionName);
      assertThat(stateLatches.get(RECOVERING)).is(completed());
      assertThat(stateLatches.get(OPEN)).is(completed());
      c.management().queue().name(q).exclusive(true).declare();
    }
  }
}
