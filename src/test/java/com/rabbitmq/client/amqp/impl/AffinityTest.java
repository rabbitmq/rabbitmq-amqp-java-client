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

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@ExtendWith(AmqpTestInfrastructureExtension.class)
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
        .queue("my-qq")
        .operation(ConnectionSettings.Affinity.Operation.PUBLISH)
        .connection()
        .build();
    try (AmqpConnection c1 = connection(b -> b.affinity().queue(name));
        AmqpConnection c2 = connection(b -> b.affinity().queue(name));
        AmqpConnection c3 = connection(b -> b.affinity().queue(name).reuse(true))) {
      assertThat(c1.id()).isNotEqualTo(c2.id());
      assertThat(c3.id()).isIn(c1.id(), c2.id());
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
