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
package com.rabbitmq.amqp.client.impl;

import static com.rabbitmq.amqp.client.impl.TestUtils.environmentBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.amqp.client.Environment;
import com.rabbitmq.amqp.client.Management;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.*;

public class ManagementTest {

  static Environment environment;
  AmqpConnection connection;
  AmqpManagement management;

  @BeforeAll
  static void initAll() {
    environment = environmentBuilder().build();
  }

  @BeforeEach
  void init() {
    this.connection = (AmqpConnection) environment.connectionBuilder().build();
  }

  @AfterEach
  void tearDown() {
    if (this.management != null) {
      this.management.close();
    }
    this.connection.close();
  }

  @AfterAll
  static void tearDownAll() {
    environment.close();
  }

  @Test
  void queueDeclareWithClientNamedQueueShouldBeRetriedIfNameAlreadyExists() {
    String q = Utils.NAME_SUPPLIER.get();
    AtomicInteger nameSupplierCallCount = new AtomicInteger();
    Supplier<String> nameSupplier =
        () -> {
          // simulate there is a duplicate in the name generation
          // it should be retried to get a new name
          if (nameSupplierCallCount.incrementAndGet() < 3) {
            return q;
          } else {
            return Utils.NAME_SUPPLIER.get();
          }
        };
    management =
        new AmqpManagement(new AmqpManagementParameters(connection).nameSupplier(nameSupplier));
    management.init();
    Management.QueueInfo queueInfo = management.queue().exclusive(true).autoDelete(true).declare();
    TestUtils.assertThat(queueInfo).hasName(q);
    assertThat(nameSupplierCallCount).hasValue(1);
    queueInfo = management.queue().exclusive(true).autoDelete(false).declare();
    assertThat(queueInfo.name()).isNotEqualTo(q);
    assertThat(nameSupplierCallCount).hasValue(1 + 2);
  }
}
