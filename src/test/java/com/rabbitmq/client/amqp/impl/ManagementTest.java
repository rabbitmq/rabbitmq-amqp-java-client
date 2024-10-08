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

import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Management;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.*;

@AmqpTestInfrastructure
public class ManagementTest {

  AmqpConnection connection;
  AmqpManagement management;

  @AfterEach
  void tearDown() {
    if (this.management != null) {
      this.management.close();
    }
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
    assertThat(queueInfo).hasName(q);
    assertThat(nameSupplierCallCount).hasValue(1);
    queueInfo = management.queue().exclusive(true).autoDelete(false).declare();
    assertThat(queueInfo.name()).isNotEqualTo(q);
    assertThat(nameSupplierCallCount).hasValue(1 + 2);
  }

  @Test
  void queueInfoShouldThrowDoesNotExistExceptionWhenQueueDoesNotExist() {
    assertThatThrownBy(() -> connection.management().queueInfo("do not exists"))
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class);
  }

  @Test
  void receiveLoopShouldStopAfterBeingIdle() {
    management =
        new AmqpManagement(
            new AmqpManagementParameters(connection)
                .receiveLoopIdleTimeout(Duration.ofMillis(500)));
    management.init();
    assertThat(management.hasReceiveLoop()).isFalse();
    Management.QueueInfo info1 = management.queue().exclusive(true).autoDelete(true).declare();
    assertThat(management.hasReceiveLoop()).isTrue();
    TestUtils.waitAtMost(() -> !management.hasReceiveLoop());
    Management.QueueInfo info2 = management.queue().exclusive(true).autoDelete(true).declare();
    assertThat(management.hasReceiveLoop()).isTrue();
    assertThat(management.queueInfo(info1.name())).hasName(info1.name());
    assertThat(management.queueInfo(info2.name())).hasName(info2.name());
  }
}
