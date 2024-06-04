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

import static com.rabbitmq.amqp.client.Publisher.Status.ACCEPTED;
import static com.rabbitmq.amqp.client.Resource.State.OPEN;
import static com.rabbitmq.amqp.client.Resource.State.RECOVERING;
import static com.rabbitmq.amqp.client.impl.TestUtils.*;
import static com.rabbitmq.amqp.client.impl.TestUtils.CountDownLatchConditions.completed;
import static com.rabbitmq.amqp.client.impl.TestUtils.name;
import static com.rabbitmq.amqp.client.impl.TestUtils.waitAtMost;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.amqp.client.*;
import com.rabbitmq.amqp.client.impl.TestUtils.DisabledIfRabbitMqCtlNotSet;
import com.rabbitmq.amqp.client.metrics.NoOpMetricsCollector;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.*;

@DisabledIfRabbitMqCtlNotSet
public class AmqpConnectionRecoveryTest {

  static AmqpEnvironment environment;
  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = BackOffDelayPolicy.fixed(ofMillis(100));
  static AtomicInteger connectionAttemptCount = new AtomicInteger();

  @BeforeAll
  static void initAll() {
    DefaultConnectionSettings<?> connectionSettings = DefaultConnectionSettings.instance();
    connectionSettings.addressSelector(
        addresses -> {
          connectionAttemptCount.incrementAndGet();
          return addresses.get(0);
        });
    environment =
        new AmqpEnvironment(
            null,
            connectionSettings,
            NoOpMetricsCollector.INSTANCE,
            Utils.NO_OP_OBSERVATION_COLLECTOR);
  }

  @BeforeEach
  void init() {
    connectionAttemptCount.set(0);
  }

  @AfterAll
  static void afterAll() {
    environment.close();
  }

  @Test
  void connectionShouldRecoverAfterClosingIt(TestInfo info) throws Exception {
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
    Connection c = new AmqpConnection(builder);
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      c.management().queue().name(q).declare();
      AtomicInteger consumerOpenCount = new AtomicInteger(0);
      Collection<UUID> receivedMessageIds = Collections.synchronizedList(new ArrayList<>());
      AtomicReference<CountDownLatch> consumeLatch = new AtomicReference<>(new CountDownLatch(1));
      c.consumerBuilder()
          .queue(q)
          .messageHandler(
              (context, message) -> {
                context.accept();
                receivedMessageIds.add(message.messageIdAsUuid());
                consumeLatch.get().countDown();
              })
          .listeners(
              context -> {
                if (context.currentState() == OPEN) {
                  consumerOpenCount.incrementAndGet();
                }
              })
          .build();
      AtomicReference<CountDownLatch> publishLatch = new AtomicReference<>(new CountDownLatch(1));
      AtomicInteger publisherOpenCount = new AtomicInteger(0);
      Publisher p =
          c.publisherBuilder()
              .queue(q)
              .listeners(
                  context -> {
                    if (context.currentState() == OPEN) {
                      publisherOpenCount.incrementAndGet();
                    }
                  })
              .build();
      Collection<UUID> publishedMessageIds = Collections.synchronizedList(new ArrayList<>());
      Publisher.Callback outboundMessageCallback =
          context -> {
            if (context.status() == ACCEPTED) {
              publishedMessageIds.add(context.message().messageIdAsUuid());
              publishLatch.get().countDown();
            }
          };
      p.publish(p.message().messageId(UUID.randomUUID()), outboundMessageCallback);
      assertThat(publisherOpenCount).hasValue(1);
      assertThat(publishLatch).is(CountDownLatchReferenceConditions.completed());

      assertThat(consumerOpenCount).hasValue(1);
      assertThat(consumeLatch).is(CountDownLatchReferenceConditions.completed());
      assertThat(receivedMessageIds)
          .hasSameSizeAs(publishedMessageIds)
          .containsAll(publishedMessageIds);

      consumeLatch.set(new CountDownLatch(1));

      Cli.closeConnection(connectionName);
      assertThat(stateLatches.get(RECOVERING)).is(completed());
      assertThat(stateLatches.get(OPEN)).is(completed());
      assertThat(connectionAttemptCount).hasValue(2);
      waitAtMost(() -> consumerOpenCount.get() == 2);
      waitAtMost(() -> publisherOpenCount.get() == 2);

      publishLatch.set(new CountDownLatch(1));
      p.publish(p.message().messageId(UUID.randomUUID()), outboundMessageCallback);
      assertThat(publishLatch).is(CountDownLatchReferenceConditions.completed());
      assertThat(publishedMessageIds).hasSize(2);

      assertThat(consumeLatch).is(CountDownLatchReferenceConditions.completed());
      assertThat(receivedMessageIds)
          .hasSameSizeAs(publishedMessageIds)
          .containsAll(publishedMessageIds);

    } finally {
      c.management().queueDeletion().delete(q);
      c.close();
    }
  }

  @Test
  void connectionShouldRecoverAfterBrokerStopStart(TestInfo info) {
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
                .backOffDelayPolicy(BackOffDelayPolicy.fixed(ofMillis(500)))
                .connectionBuilder();
    try (Connection c = new AmqpConnection(builder)) {
      assertThat(connectionAttemptCount).hasValue(1);
      c.management().queue().name(q).autoDelete(true).exclusive(true).declare();
      try {
        Cli.stopBroker();
        assertThat(stateLatches.get(RECOVERING)).is(completed());
        stream(
                new ThrowingCallable[] {
                  () -> c.management().queue().name(q).exclusive(true).declare(),
                  () -> c.publisherBuilder(),
                  () -> c.consumerBuilder()
                })
            .forEach(
                op ->
                    assertThatThrownBy(op)
                        .isInstanceOf(ModelException.class)
                        .hasMessageContaining(RECOVERING.name()));
      } finally {
        Cli.startBroker();
      }
      assertThat(stateLatches.get(OPEN)).is(completed());
      assertThat(connectionAttemptCount).hasValueGreaterThan(1);
      c.management().queue().name(q).autoDelete(true).exclusive(true).declare();
    }
  }
}
