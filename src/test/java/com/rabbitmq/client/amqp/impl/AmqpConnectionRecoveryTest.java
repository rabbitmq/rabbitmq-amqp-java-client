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

import static com.rabbitmq.client.amqp.Publisher.Status.ACCEPTED;
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.Resource.State.RECOVERING;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.name;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfRabbitMqCtlNotSet;
import com.rabbitmq.client.amqp.metrics.NoOpMetricsCollector;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void connectionShouldRecoverAfterClosingIt(boolean isolateResources, TestInfo info) {
    String q = name(info);
    String connectionName = UUID.randomUUID().toString();
    Map<Resource.State, TestUtils.Sync> stateSync = new ConcurrentHashMap<>();
    stateSync.put(RECOVERING, TestUtils.sync(1));
    stateSync.put(OPEN, TestUtils.sync(2));
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .name(connectionName)
                .isolateResources(isolateResources)
                .listeners(
                    context -> {
                      if (stateSync.containsKey(context.currentState())) {
                        stateSync.get(context.currentState()).down();
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
      TestUtils.Sync consumeSync = TestUtils.sync();
      c.consumerBuilder()
          .queue(q)
          .messageHandler(
              (context, message) -> {
                context.accept();
                receivedMessageIds.add(message.messageIdAsUuid());
                consumeSync.down();
              })
          .listeners(
              context -> {
                if (context.currentState() == OPEN) {
                  consumerOpenCount.incrementAndGet();
                }
              })
          .build();
      TestUtils.Sync publishSync = TestUtils.sync();
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
              publishSync.down();
            }
          };
      p.publish(p.message().messageId(UUID.randomUUID()), outboundMessageCallback);
      assertThat(publisherOpenCount).hasValue(1);
      assertThat(publishSync).completes();

      assertThat(consumerOpenCount).hasValue(1);
      assertThat(consumeSync).completes();
      assertThat(receivedMessageIds)
          .hasSameSizeAs(publishedMessageIds)
          .containsAll(publishedMessageIds);

      consumeSync.reset();

      Cli.closeConnection(connectionName);
      assertThat(stateSync.get(RECOVERING)).completes();
      assertThat(stateSync.get(OPEN)).completes();
      assertThat(connectionAttemptCount).hasValue(2);
      waitAtMost(() -> consumerOpenCount.get() == 2);
      waitAtMost(() -> publisherOpenCount.get() == 2);

      publishSync.reset();
      p.publish(p.message().messageId(UUID.randomUUID()), outboundMessageCallback);
      assertThat(publishSync).completes();
      assertThat(publishedMessageIds).hasSize(2);

      assertThat(consumeSync).completes();
      assertThat(receivedMessageIds)
          .hasSameSizeAs(publishedMessageIds)
          .containsAll(publishedMessageIds);

    } finally {
      c.management().queueDeletion().delete(q);
      c.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void connectionShouldRecoverAfterBrokerStopStart(boolean isolateResources, TestInfo info) {
    String q = name(info);
    String connectionName = UUID.randomUUID().toString();
    Map<Resource.State, TestUtils.Sync> stateSync = new ConcurrentHashMap<>();
    stateSync.put(RECOVERING, TestUtils.sync(1));
    stateSync.put(OPEN, TestUtils.sync(2));
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .name(connectionName)
                .isolateResources(isolateResources)
                .listeners(
                    context -> {
                      if (stateSync.containsKey(context.currentState())) {
                        stateSync.get(context.currentState()).down();
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
        assertThat(stateSync.get(RECOVERING)).completes();
        stream(
                new ThrowingCallable[] {
                  () -> c.management().queue().name(q).exclusive(true).declare(),
                  () -> c.publisherBuilder(),
                  () -> c.consumerBuilder()
                })
            .forEach(
                op ->
                    assertThatThrownBy(op)
                        .isInstanceOf(AmqpException.class)
                        .hasMessageContaining(RECOVERING.name()));
      } finally {
        Cli.startBroker();
      }
      assertThat(stateSync.get(OPEN)).completes();
      assertThat(connectionAttemptCount).hasValueGreaterThan(1);
      c.management().queue().name(q).autoDelete(true).exclusive(true).declare();
    }
  }
}
