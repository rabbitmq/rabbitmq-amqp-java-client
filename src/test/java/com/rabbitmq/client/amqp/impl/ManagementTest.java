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
import static com.rabbitmq.client.amqp.impl.Cli.*;
import static com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersion.RABBITMQ_4_1_0;
import static com.rabbitmq.client.amqp.impl.TestUtils.closedOnSecurityExceptionListener;
import static com.rabbitmq.client.amqp.impl.TestUtils.sync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersionAtLeast;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@AmqpTestInfrastructure
public class ManagementTest {

  Environment environment;
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

  @Test
  void getManagementFromConnectionAfterManagementHasBeenClosed() {
    AmqpManagement m1 = (AmqpManagement) connection.management();
    String q = m1.queue().exclusive(true).declare().name();
    assertThat(m1.queueInfo(q)).isEmpty();
    assertThat(m1.isClosed()).isFalse();
    m1.close();
    assertThat(m1.isClosed()).isTrue();
    assertThatThrownBy(() -> m1.queueInfo(q))
        .isInstanceOf(AmqpException.AmqpResourceClosedException.class);
    AmqpManagement m2 = (AmqpManagement) connection.management();
    assertThat(m2.isClosed()).isFalse();
    assertThat(m2.queueInfo(q)).isEmpty();
    assertThat(m2).isSameAs(m1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void sessionShouldGetClosedAfterPermissionsChangedAndSetTokenCalled(
      boolean isolateResources, TestInfo info) {
    String username = "foo";
    String password = "bar";
    String vh = "/";
    String q = TestUtils.name(info);

    Connection c = null;
    try {
      addUser(username, password);
      setPermissions(username, vh, ".*");
      this.connection.management().queue(q).declare();

      c =
          ((AmqpConnectionBuilder) environment.connectionBuilder())
              .isolateResources(isolateResources)
              .username(username)
              .password(password)
              .build();
      Sync consumeSync = sync();
      Sync publisherClosedSync = sync();
      Sync consumerClosedSync = sync();
      Publisher p =
          c.publisherBuilder()
              .queue(q)
              .listeners(closedOnSecurityExceptionListener(publisherClosedSync))
              .build();
      c.consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, msg) -> {
                ctx.accept();
                consumeSync.down();
              })
          .listeners(closedOnSecurityExceptionListener(consumerClosedSync))
          .build();

      p.publish(p.message(), ctx -> {});
      assertThat(consumeSync).completes();

      setPermissions(username, vh, "foobar");
      AmqpManagement m = (AmqpManagement) c.management();
      m.setToken(password);
      assertThat(publisherClosedSync).completes();
      assertThat(consumerClosedSync).completes();
    } finally {
      if (c != null) {
        c.close();
      }
      this.connection.management().queueDelete(q);
      deleteUser(username);
    }
  }
}
