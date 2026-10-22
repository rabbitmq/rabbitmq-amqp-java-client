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
import static com.rabbitmq.client.amqp.Resource.State.CLOSED;
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.Resource.State.RECOVERING;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.maybeConfigureWebSocket;
import static com.rabbitmq.client.amqp.impl.TestUtils.name;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfRabbitMqCtlNotSet;
import com.rabbitmq.client.amqp.metrics.NoOpMetricsCollector;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

@DisabledIfRabbitMqCtlNotSet
public class AmqpConnectionRecoveryTest {

  static AmqpEnvironment environment;
  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = BackOffDelayPolicy.fixed(ofMillis(100));
  static AtomicInteger connectionAttemptCount = new AtomicInteger();

  @BeforeAll
  static void initAll() {
    DefaultConnectionSettings<?> connectionSettings = DefaultConnectionSettings.instance();
    maybeConfigureWebSocket(connectionSettings);
    connectionSettings.addressSelector(
        addresses -> {
          connectionAttemptCount.incrementAndGet();
          return addresses.get(0);
        });
    environment =
        new AmqpEnvironment(
            null,
            null,
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
      waitAtMost(() -> publisherOpenCount.get() == 1);
      p.publish(p.message().messageId(UUID.randomUUID()), outboundMessageCallback);
      assertThat(publishSync).completes();

      waitAtMost(() -> consumerOpenCount.get() == 1);
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
      c.management().queueDelete(q);
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

  @Test
  void connectionShouldRecoverWhenBrokerEndsSessionOnConsumerDetach(TestInfo info) {
    String downQ = name(info) + "-down";
    String q = name(info);
    Map<Resource.State, TestUtils.Sync> stateSync = new ConcurrentHashMap<>();
    stateSync.put(RECOVERING, TestUtils.sync(1));
    stateSync.put(OPEN, TestUtils.sync(2));
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .listeners(
                    context -> {
                      if (stateSync.containsKey(context.currentState())) {
                        stateSync.get(context.currentState()).down();
                      }
                    })
                .recovery()
                .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
                .connectionBuilder();
    try (Connection c = new AmqpConnection(builder)) {
      c.management().queue(downQ).quorum().queue().declare();
      c.management().queue(q).declare();
      try {
        Consumer downConsumer =
            c.consumerBuilder().queue(downQ).messageHandler((ctx, msg) -> ctx.accept()).build();
        TestUtils.Sync consumeSync = TestUtils.sync();
        AtomicInteger consumerOpenCount = new AtomicInteger(0);
        c.consumerBuilder()
            .queue(q)
            .messageHandler(
                (ctx, msg) -> {
                  ctx.accept();
                  consumeSync.down();
                })
            .listeners(
                context -> {
                  if (context.currentState() == OPEN) {
                    consumerOpenCount.incrementAndGet();
                  }
                })
            .build();
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

        // the broker ends the session when it cannot remove the consumer on detach
        Cli.stopQuorumQueueServers(downQ);
        downConsumer.close();

        assertThat(stateSync.get(RECOVERING)).completes();
        assertThat(stateSync.get(OPEN)).completes();
        assertThat(connectionAttemptCount).hasValue(2);
        waitAtMost(() -> consumerOpenCount.get() == 2);
        waitAtMost(() -> publisherOpenCount.get() == 2);
        assertThat(((AmqpConsumer) downConsumer).state()).isEqualTo(CLOSED);

        TestUtils.Sync publishSync = TestUtils.sync();
        p.publish(
            p.message(),
            context -> {
              if (context.status() == ACCEPTED) {
                publishSync.down();
              }
            });
        assertThat(publishSync).completes();
        assertThat(consumeSync).completes();

        c.consumerBuilder().queue(q).messageHandler((ctx, msg) -> ctx.accept()).build().close();
      } finally {
        Cli.restartQuorumQueueServers(downQ);
        c.management().queueDelete(downQ);
        c.management().queueDelete(q);
      }
    }
  }

  @Test
  void consumerOnQuorumQueueWithoutRunningMemberShouldNotBlockRecovery(TestInfo info) {
    String downQ = name(info) + "-down";
    String q = name(info);
    String connectionName = UUID.randomUUID().toString();
    Map<Resource.State, TestUtils.Sync> stateSync = new ConcurrentHashMap<>();
    stateSync.put(RECOVERING, TestUtils.sync(1));
    stateSync.put(OPEN, TestUtils.sync(2));
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .name(connectionName)
                .listeners(
                    context -> {
                      if (stateSync.containsKey(context.currentState())) {
                        stateSync.get(context.currentState()).down();
                      }
                    })
                .recovery()
                .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
                .connectionBuilder();
    try (Connection c = new AmqpConnection(builder)) {
      c.management().queue(downQ).quorum().queue().declare();
      c.management().queue(q).declare();
      try {
        Consumer downConsumer =
            c.consumerBuilder().queue(downQ).messageHandler((ctx, msg) -> ctx.accept()).build();
        TestUtils.Sync consumeSync = TestUtils.sync();
        c.consumerBuilder()
            .queue(q)
            .messageHandler(
                (ctx, msg) -> {
                  ctx.accept();
                  consumeSync.down();
                })
            .build();
        Publisher p = c.publisherBuilder().queue(q).build();

        Cli.stopQuorumQueueServers(downQ);
        Cli.closeConnection(connectionName);

        // the quorum queue members stay down, the connection must recover anyway
        assertThat(stateSync.get(RECOVERING)).completes();
        assertThat(stateSync.get(OPEN)).completes();
        waitAtMost(() -> ((AmqpConsumer) downConsumer).state() == CLOSED);

        TestUtils.Sync publishSync = TestUtils.sync();
        p.publish(
            p.message(),
            context -> {
              if (context.status() == ACCEPTED) {
                publishSync.down();
              }
            });
        assertThat(publishSync).completes();
        assertThat(consumeSync).completes();
      } finally {
        Cli.restartQuorumQueueServers(downQ);
        // the test connection may still be recovering if the test fails
        try (Connection cleanup = environment.connectionBuilder().build()) {
          cleanup.management().queueDelete(downQ);
          cleanup.management().queueDelete(q);
        }
      }
    }
  }

  @Test
  void staleTopologyRecoveryShouldNotCloseResources(TestInfo info) throws Exception {
    String q = name(info);
    String connectionName = UUID.randomUUID().toString();
    AtomicReference<Duration> recoveryDelay = new AtomicReference<>(ofMillis(100));
    Map<Resource.State, TestUtils.Sync> stateSync = new ConcurrentHashMap<>();
    stateSync.put(RECOVERING, TestUtils.sync(1));
    stateSync.put(OPEN, TestUtils.sync(2));
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .name(connectionName)
                .listeners(
                    context -> {
                      if (stateSync.containsKey(context.currentState())) {
                        stateSync.get(context.currentState()).down();
                      }
                    })
                .recovery()
                .backOffDelayPolicy(attempt -> recoveryDelay.get())
                .connectionBuilder();
    try (AmqpConnection c = new AmqpConnection(builder)) {
      c.management().queue(q).declare();
      try {
        TestUtils.Sync consumeSync = TestUtils.sync(2);
        Consumer consumer1 =
            c.consumerBuilder()
                .queue(q)
                .messageHandler(
                    (ctx, msg) -> {
                      ctx.accept();
                      consumeSync.down();
                    })
                .build();
        Consumer consumer2 =
            c.consumerBuilder()
                .queue(q)
                .messageHandler(
                    (ctx, msg) -> {
                      ctx.accept();
                      consumeSync.down();
                    })
                .build();
        Publisher p = c.publisherBuilder().queue(q).build();

        AtomicBoolean blockRecovery = new AtomicBoolean(true);
        CountDownLatch recoveryBlocked = new CountDownLatch(1);
        CountDownLatch releaseRecovery = new CountDownLatch(1);
        c.beforeResourceRecoveryHook =
            () -> {
              if (blockRecovery.compareAndSet(true, false)) {
                recoveryBlocked.countDown();
                try {
                  releaseRecovery.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
            };

        Cli.closeConnection(connectionName);
        assertThat(stateSync.get(RECOVERING)).completes();
        assertThat(recoveryBlocked).completes();

        // the first topology recovery is blocked, a second failure starts a new recovery
        recoveryDelay.set(ofSeconds(2));
        closeConnectionAndWaitForNewRecovery(c, connectionName);
        releaseRecovery.countDown();

        assertThat(stateSync.get(OPEN)).completes();
        assertThat(((AmqpConsumer) consumer1).state()).isEqualTo(OPEN);
        assertThat(((AmqpConsumer) consumer2).state()).isEqualTo(OPEN);
        assertThat(((AmqpPublisher) p).state()).isEqualTo(OPEN);

        TestUtils.Sync publishSync = TestUtils.sync(2);
        Publisher.Callback callback =
            context -> {
              if (context.status() == ACCEPTED) {
                publishSync.down();
              }
            };
        p.publish(p.message(), callback);
        p.publish(p.message(), callback);
        assertThat(publishSync).completes();
        assertThat(consumeSync).completes();
      } finally {
        c.beforeResourceRecoveryHook = () -> {};
        try (Connection cleanup = environment.connectionBuilder().build()) {
          cleanup.management().queueDelete(q);
        }
      }
    }
  }

  enum StaleRecoveryScenario {
    OPEN_CONSUMER,
    OPEN_PUBLISHER,
    CLOSE_FAILED_CONSUMER
  }

  @ParameterizedTest
  @EnumSource(StaleRecoveryScenario.class)
  void staleTopologyRecoveryShouldNotChangeResourceState(
      StaleRecoveryScenario scenario, TestInfo info) {
    String q = name(info);
    String connectionName = UUID.randomUUID().toString();
    AtomicReference<Duration> recoveryDelay = new AtomicReference<>(ofMillis(100));
    TestUtils.Sync openSync = TestUtils.sync(2);
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .name(connectionName)
                .listeners(
                    context -> {
                      if (context.currentState() == OPEN) {
                        openSync.down();
                      }
                    })
                .recovery()
                .backOffDelayPolicy(attempt -> recoveryDelay.get())
                .connectionBuilder();
    try (AmqpConnection c = new AmqpConnection(builder)) {
      c.management().queue(q).declare();
      try {
        TestUtils.Sync consumeSync = TestUtils.sync(1);
        AmqpConsumer consumer =
            (AmqpConsumer)
                c.consumerBuilder()
                    .queue(q)
                    .messageHandler(
                        (ctx, msg) -> {
                          ctx.accept();
                          consumeSync.down();
                        })
                    .build();
        AmqpPublisher publisher = (AmqpPublisher) c.publisherBuilder().queue(q).build();
        ResourceBase resource =
            scenario == StaleRecoveryScenario.OPEN_PUBLISHER ? publisher : consumer;

        AtomicInteger activationCount = new AtomicInteger();
        AtomicReference<Resource.State> stateInNewerRecovery = new AtomicReference<>();
        Runnable hook =
            () -> {
              if (activationCount.incrementAndGet() == 1) {
                // a disconnect between the stale check and the activation
                recoveryDelay.set(ofSeconds(1));
                closeConnectionAndWaitForNewRecovery(c, connectionName);
                if (scenario == StaleRecoveryScenario.CLOSE_FAILED_CONSUMER) {
                  throw new IllegalStateException("simulated recovery failure");
                }
              } else {
                stateInNewerRecovery.compareAndSet(null, resource.state());
              }
            };
        if (scenario == StaleRecoveryScenario.OPEN_PUBLISHER) {
          publisher.beforeActivateHook = hook;
        } else {
          consumer.beforeActivateHook = hook;
        }

        Cli.closeConnection(connectionName);
        assertThat(openSync).completes();
        assertThat(stateInNewerRecovery).hasValue(RECOVERING);
        assertThat(consumer.state()).isEqualTo(OPEN);
        assertThat(publisher.state()).isEqualTo(OPEN);

        publisher.publish(publisher.message(), ctx -> {});
        assertThat(consumeSync).completes();
      } finally {
        try (Connection cleanup = environment.connectionBuilder().build()) {
          cleanup.management().queueDelete(q);
        }
      }
    }
  }

  @Test
  void managementClosedByApplicationShouldBeClosedAfterStaleRecovery(TestInfo info) {
    String q = name(info);
    String connectionName = UUID.randomUUID().toString();
    AtomicReference<Duration> recoveryDelay = new AtomicReference<>(ofMillis(100));
    TestUtils.Sync openSync = TestUtils.sync(2);
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .name(connectionName)
                .listeners(
                    context -> {
                      if (context.currentState() == OPEN) {
                        openSync.down();
                      }
                    })
                .recovery()
                .backOffDelayPolicy(attempt -> recoveryDelay.get())
                .connectionBuilder();
    try (AmqpConnection c = new AmqpConnection(builder)) {
      AmqpManagement management = (AmqpManagement) c.management();
      management.queue(q).declare();
      try {
        c.consumerBuilder().queue(q).messageHandler((ctx, msg) -> ctx.accept()).build();
        management.close();

        AtomicBoolean disconnect = new AtomicBoolean(true);
        c.beforeResourceRecoveryHook =
            () -> {
              if (disconnect.compareAndSet(true, false)) {
                // the first recovery has opened management and goes stale
                recoveryDelay.set(ofSeconds(1));
                closeConnectionAndWaitForNewRecovery(c, connectionName);
              }
            };

        Cli.closeConnection(connectionName);
        assertThat(openSync).completes();
        assertThat(disconnect).isFalse();
        assertThat(management.isClosed()).isTrue();
      } finally {
        c.beforeResourceRecoveryHook = () -> {};
        try (Connection cleanup = environment.connectionBuilder().build()) {
          cleanup.management().queueDelete(q);
        }
      }
    }
  }

  @Test
  void managementReopenedByApplicationShouldStayOpenAfterRecovery(TestInfo info) {
    String q = name(info);
    String connectionName = UUID.randomUUID().toString();
    AtomicReference<TestUtils.Sync> openSync = new AtomicReference<>(TestUtils.sync());
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .name(connectionName)
                .listeners(
                    context -> {
                      if (context.currentState() == OPEN) {
                        openSync.get().down();
                      }
                    })
                .recovery()
                .backOffDelayPolicy(BackOffDelayPolicy.fixed(ofMillis(100)))
                .connectionBuilder();
    try (AmqpConnection c = new AmqpConnection(builder)) {
      assertThat(openSync.get()).completes();
      AmqpManagement management = (AmqpManagement) c.management();
      management.close();

      openSync.set(TestUtils.sync());
      Cli.closeConnection(connectionName);
      assertThat(openSync.get()).completes();
      assertThat(management.isClosed()).isTrue();

      assertThat(c.management()).isSameAs(management);
      management.queue(q).declare();
      try {
        openSync.set(TestUtils.sync());
        Cli.closeConnection(connectionName);
        assertThat(openSync.get()).completes();
        assertThat(management.isClosed()).isFalse();
        assertThat(management.queueInfo(q)).hasName(q);
      } finally {
        try (Connection cleanup = environment.connectionBuilder().build()) {
          cleanup.management().queueDelete(q);
        }
      }
    }
  }

  private static void closeConnectionAndWaitForNewRecovery(
      AmqpConnection c, String connectionName) {
    Cli.closeConnection(connectionName);
    waitAtMost(
        () -> {
          try {
            c.nativeSession(false);
            return false;
          } catch (AmqpException.AmqpResourceInvalidStateException e) {
            return true;
          }
        });
  }

  @Test
  void connectionShouldBeClosedWhenBrokerEndsSessionAndRecoveryIsDisabled(TestInfo info) {
    String q = name(info);
    TestUtils.Sync closedSync = TestUtils.sync();
    AtomicReference<Throwable> closeCause = new AtomicReference<>();
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .listeners(
                    context -> {
                      if (context.currentState() == CLOSED) {
                        closeCause.set(context.failureCause());
                        closedSync.down();
                      }
                    })
                .recovery()
                .activated(false)
                .connectionBuilder();
    try (Connection c = new AmqpConnection(builder)) {
      c.management().queue(q).quorum().queue().declare();
      try {
        Consumer consumer =
            c.consumerBuilder().queue(q).messageHandler((ctx, msg) -> ctx.accept()).build();
        Cli.stopQuorumQueueServers(q);
        consumer.close();
        assertThat(closedSync).completes();
        assertThat(closeCause.get())
            .isInstanceOf(AmqpException.AmqpResourceClosedException.class)
            .hasMessageContaining(q);
      } finally {
        Cli.restartQuorumQueueServers(q);
        try (Connection c2 = environment.connectionBuilder().build()) {
          c2.management().queueDelete(q);
        }
      }
    }
  }

  @Test
  void isolatedResourcesShouldBeClosedWhenBrokerEndsTheirSession(TestInfo info) {
    String consumerQ = name(info) + "-consumer";
    String publisherQ = name(info) + "-publisher";
    String otherQ = name(info) + "-other";
    Collection<Resource.State> connectionStates = Collections.synchronizedList(new ArrayList<>());
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            new AmqpConnectionBuilder(environment)
                .isolateResources(true)
                .listeners(context -> connectionStates.add(context.currentState()))
                .recovery()
                .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
                .connectionBuilder();
    try (Connection c = new AmqpConnection(builder)) {
      for (String q : new String[] {consumerQ, publisherQ, otherQ}) {
        c.management().queue(q).declare();
      }
      try {
        TestUtils.Sync consumerClosedSync = TestUtils.sync();
        AtomicReference<Throwable> consumerCloseCause = new AtomicReference<>();
        c.consumerBuilder()
            .queue(consumerQ)
            .messageHandler((ctx, msg) -> ctx.accept())
            .listeners(
                context -> {
                  if (context.currentState() == CLOSED) {
                    consumerCloseCause.set(context.failureCause());
                    consumerClosedSync.down();
                  }
                })
            .build();
        TestUtils.Sync publisherClosedSync = TestUtils.sync();
        AtomicReference<Throwable> publisherCloseCause = new AtomicReference<>();
        c.publisherBuilder()
            .queue(publisherQ)
            .listeners(
                context -> {
                  if (context.currentState() == CLOSED) {
                    publisherCloseCause.set(context.failureCause());
                    publisherClosedSync.down();
                  }
                })
            .build();
        TestUtils.Sync consumeSync = TestUtils.sync();
        Consumer otherConsumer =
            c.consumerBuilder()
                .queue(otherQ)
                .messageHandler(
                    (ctx, msg) -> {
                      ctx.accept();
                      consumeSync.down();
                    })
                .build();
        Publisher otherPublisher = c.publisherBuilder().queue(otherQ).build();

        Cli.endSessionsWithLinkTo(consumerQ);
        assertThat(consumerClosedSync).completes();
        assertThat(consumerCloseCause.get())
            .isInstanceOf(AmqpException.AmqpResourceClosedException.class);

        Cli.endSessionsWithLinkTo(publisherQ);
        assertThat(publisherClosedSync).completes();
        assertThat(publisherCloseCause.get())
            .isInstanceOf(AmqpException.AmqpResourceClosedException.class);

        assertThat(connectionStates).doesNotContain(RECOVERING, CLOSED);
        assertThat(connectionAttemptCount).hasValue(1);
        assertThat(((AmqpConsumer) otherConsumer).state()).isEqualTo(OPEN);
        TestUtils.Sync publishSync = TestUtils.sync();
        otherPublisher.publish(
            otherPublisher.message(),
            context -> {
              if (context.status() == ACCEPTED) {
                publishSync.down();
              }
            });
        assertThat(publishSync).completes();
        assertThat(consumeSync).completes();
      } finally {
        for (String q : new String[] {consumerQ, publisherQ, otherQ}) {
          c.management().queueDelete(q);
        }
      }
    }
  }
}
