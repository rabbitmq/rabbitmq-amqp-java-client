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

import static com.rabbitmq.client.amqp.Management.QueueType.QUORUM;
import static com.rabbitmq.client.amqp.Management.QueueType.STREAM;
import static com.rabbitmq.client.amqp.Resource.State.CLOSED;
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.Resource.State.RECOVERING;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.Cli.closeConnection;
import static com.rabbitmq.client.amqp.impl.TestUtils.name;
import static com.rabbitmq.client.amqp.impl.TestUtils.sync;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@AmqpTestInfrastructure
public class AmqpConsumerTest {

  // used by the test extension
  BackOffDelayPolicy backOffDelayPolicy = BackOffDelayPolicy.fixed(Duration.ofMillis(100));
  Environment environment;
  Connection connection;
  String q;
  String connectionName;

  @BeforeEach
  void init(TestInfo info) {
    this.q = name(info);
    this.connectionName = ((AmqpConnection) connection).name();
  }

  @AfterEach
  void tearDown() {
    waitAtMost(Duration.ofSeconds(5), () -> ((ResourceBase) connection).state() == OPEN);
    connection.management().queueDelete(this.q);
  }

  @Test
  void subscriptionListenerShouldBeCalledOnRecovery() {
    connection.management().queue(this.q).type(STREAM).declare();
    Sync subscriptionSync = sync();
    Sync recoveredSync = sync();
    connection
        .consumerBuilder()
        .queue(this.q)
        .subscriptionListener(ctx -> subscriptionSync.down())
        .listeners(recoveredListener(recoveredSync))
        .messageHandler((ctx, msg) -> {})
        .build();

    assertThat(subscriptionSync).completes();
    assertThat(recoveredSync).hasNotCompleted();
    sync().reset();
    closeConnection(this.connectionName);
    assertThat(recoveredSync).completes();
    assertThat(subscriptionSync).completes();
  }

  @Test
  void streamConsumerRestartsWhereItLeftOff() {
    connection.management().queue(this.q).type(STREAM).declare();
    Connection publisherConnection = environment.connectionBuilder().build();
    Publisher publisher = publisherConnection.publisherBuilder().queue(this.q).build();
    int messageCount = 100;
    Runnable publish =
        () -> {
          Sync publishSync = sync(messageCount);
          Publisher.Callback callback = ctx -> publishSync.down();
          IntStream.range(0, messageCount)
              .forEach(
                  ignored -> {
                    publisher.publish(publisher.message(), callback);
                  });
          assertThat(publishSync).completes();
        };

    publish.run();

    Sync consumeSync = sync(messageCount);
    AtomicLong lastOffsetProcessed = new AtomicLong(-1);
    AtomicInteger consumedMessageCount = new AtomicInteger(0);
    AtomicInteger subscriptionListenerCallCount = new AtomicInteger(0);
    Sync recoveredSync = sync();
    ConsumerBuilder.SubscriptionListener subscriptionListener =
        ctx -> {
          subscriptionListenerCallCount.incrementAndGet();
          ctx.streamOptions().offset(lastOffsetProcessed.get() + 1);
        };
    Consumer.MessageHandler messageHandler =
        (ctx, msg) -> {
          long offset = (long) msg.annotation("x-stream-offset");
          ctx.accept();
          lastOffsetProcessed.set(offset);
          consumedMessageCount.incrementAndGet();
          consumeSync.down();
        };
    Consumer consumer =
        connection
            .consumerBuilder()
            .listeners(recoveredListener(recoveredSync))
            .queue(this.q)
            .subscriptionListener(subscriptionListener)
            .messageHandler(messageHandler)
            .build();

    assertThat(subscriptionListenerCallCount).hasValue(1);
    assertThat(consumeSync).completes();

    closeConnection(this.connectionName);
    assertThat(recoveredSync).completes();
    assertThat(subscriptionListenerCallCount).hasValue(2);
    assertThat(consumedMessageCount).hasValue(messageCount);

    long offsetAfterRecovery = lastOffsetProcessed.get();
    consumeSync.reset(messageCount);
    publish.run();
    assertThat(consumeSync).completes();
    assertThat(consumedMessageCount).hasValue(messageCount * 2);
    assertThat(lastOffsetProcessed).hasValueGreaterThan(offsetAfterRecovery);

    consumer.close();

    long offsetAfterClosing = lastOffsetProcessed.get();
    consumeSync.reset(messageCount);
    publish.run();

    connection
        .consumerBuilder()
        .queue(this.q)
        .subscriptionListener(subscriptionListener)
        .messageHandler(messageHandler)
        .build();

    assertThat(subscriptionListenerCallCount).hasValue(3);
    assertThat(consumeSync).completes();
    assertThat(consumedMessageCount).hasValue(messageCount * 3);
    assertThat(lastOffsetProcessed).hasValueGreaterThan(offsetAfterClosing);
  }

  @Test
  void unsettledMessageShouldGoBackToQueueIfConnectionIsClosed(TestInfo testInfo) {
    String cName = name(testInfo);
    connection.management().queue(this.q).type(QUORUM).declare();
    Sync connectionRecoveredSync = sync();
    Connection c =
        ((AmqpConnectionBuilder) environment.connectionBuilder())
            .name(cName)
            .recovery()
            .backOffDelayPolicy(backOffDelayPolicy)
            .connectionBuilder()
            .listeners(recoveredListener(connectionRecoveredSync))
            .build();
    Publisher publisher = c.publisherBuilder().queue(this.q).build();

    Sync deliveredSync = sync(2);
    Sync consumerClosedSync = sync();
    AtomicInteger deliveryCount = new AtomicInteger();
    c.consumerBuilder()
        .listeners(
            ctx -> {
              if (ctx.currentState() == CLOSED) {
                consumerClosedSync.down();
              }
            })
        .queue(this.q)
        .messageHandler(
            (ctx, msg) -> {
              if (deliveryCount.incrementAndGet() == 1) {
                closeConnection(cName);
              }
              deliveredSync.down();
            })
        .build();

    publisher.publish(publisher.message(), ctx -> {});

    assertThat(deliveredSync).completes();
    assertThat(deliveryCount).hasValue(2);
    assertThat(connectionRecoveredSync).completes();
    assertThat(consumerClosedSync).hasNotCompleted();
    c.close();
    assertThat(consumerClosedSync).completes();

    waitAtMost(
        () -> {
          Management.QueueInfo info = connection.management().queueInfo(this.q);
          return info.messageCount() == 1 && info.consumerCount() == 0;
        });
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void pendingWorkItemsReturnToZeroAfterConnectionFailureWithInFlightMessages(
      boolean preSettled, TestInfo info) {
    String cName = name(info);
    connection.management().queue(this.q).type(QUORUM).declare();
    Connection c =
        ((AmqpConnectionBuilder) environment.connectionBuilder())
            .name(cName)
            .recovery()
            .backOffDelayPolicy(backOffDelayPolicy)
            .connectionBuilder()
            .build();
    Publisher publisher = c.publisherBuilder().queue(this.q).build();

    int initialCredits = 5;
    // holds every handler call briefly so a connection failure always happens while messages
    // are genuinely in flight, not merely published; a fixed sleep (rather than a latch shared
    // across generations) keeps stale-generation redeliveries self-contained
    Duration handlerDelay = Duration.ofMillis(200);
    ConsumerBuilder builder = c.consumerBuilder().queue(this.q).initialCredits(initialCredits);
    if (preSettled) {
      builder.preSettled();
    }
    Sync recoveredSync = sync();
    AmqpConsumer consumer =
        (AmqpConsumer)
            builder
                .listeners(recoveredListener(recoveredSync))
                .messageHandler(
                    (ctx, msg) -> {
                      TestUtils.simulateActivity(handlerDelay);
                      if (!preSettled) {
                        ctx.accept();
                      }
                    })
                .build();

    for (int i = 0; i < 3; i++) {
      recoveredSync.reset();
      Sync publishSync = sync(initialCredits);
      IntStream.range(0, initialCredits)
          .forEach(ignored -> publisher.publish(publisher.message(), ctx -> publishSync.down()));
      assertThat(publishSync).completes();
      // credit is not guaranteed to be back at initialCredits at the start of every iteration
      // (see assertCreditInvariants), so only require genuinely in-flight messages, not a full
      // window of them
      waitAtMost(() -> consumer.pendingWorkItems() > 0);

      closeConnection(cName);
      assertThat(recoveredSync).completes();
      // the publisher recovers independently of the consumer; wait for the whole connection
      // to settle before publishing the next batch
      waitAtMost(() -> ((ResourceBase) c).state() == OPEN);

      assertCreditInvariants(consumer, initialCredits);
    }

    c.close();
  }

  @Test
  void unsettledMessageCountNeverNegativeAcrossRecovery(TestInfo info) throws InterruptedException {
    String cName = name(info);
    connection.management().queue(this.q).type(QUORUM).declare();
    Connection c =
        ((AmqpConnectionBuilder) environment.connectionBuilder())
            .name(cName)
            .recovery()
            .backOffDelayPolicy(backOffDelayPolicy)
            .connectionBuilder()
            .build();
    Publisher publisher = c.publisherBuilder().queue(this.q).build();

    int messagesPerRound = 5;
    Sync recoveredSync = sync();
    AmqpConsumer consumer =
        (AmqpConsumer)
            c.consumerBuilder()
                .queue(this.q)
                .listeners(recoveredListener(recoveredSync))
                .messageHandler(
                    (ctx, msg) -> {
                      TestUtils.simulateActivity(Duration.ofMillis(200));
                      ctx.accept();
                    })
                .build();

    // samples unsettledMessageCount() continuously in the background, since the defect (a
    // hand-reset racing with in-flight decrements from the previous generation) only ever
    // showed up as a transient dip, not a value a synchronous check after the fact would catch
    AtomicLong minObserved = new AtomicLong(Long.MAX_VALUE);
    AtomicBoolean watching = new AtomicBoolean(true);
    Thread watcher =
        new Thread(
            () -> {
              while (watching.get()) {
                minObserved.getAndUpdate(min -> Math.min(min, consumer.unsettledMessageCount()));
              }
            });
    watcher.start();

    try {
      for (int i = 0; i < 3; i++) {
        recoveredSync.reset();
        Sync publishSync = sync(messagesPerRound);
        IntStream.range(0, messagesPerRound)
            .forEach(ignored -> publisher.publish(publisher.message(), ctx -> publishSync.down()));
        assertThat(publishSync).completes();
        waitAtMost(() -> consumer.unsettledMessageCount() == messagesPerRound);

        closeConnection(cName);
        assertThat(recoveredSync).completes();
        waitAtMost(() -> ((ResourceBase) c).state() == OPEN);

        waitAtMost(() -> consumer.unsettledMessageCount() == 0);
      }
    } finally {
      watching.set(false);
      watcher.join();
    }

    assertThat(minObserved).hasNonNegativeValue();

    c.close();
  }

  @Test
  void consumerResumesAfterPauseHandshakeTimeout() {
    connection.management().queue(this.q).declare();
    Publisher publisher = connection.publisherBuilder().queue(this.q).build();

    int initialCredits = 5;
    Sync consumeSync = sync(initialCredits);
    AmqpConsumer consumer =
        (AmqpConsumer)
            connection
                .consumerBuilder()
                .queue(this.q)
                .initialCredits(initialCredits)
                .messageHandler(
                    (ctx, msg) -> {
                      ctx.accept();
                      consumeSync.down();
                    })
                .build();
    // a zero timeout guarantees the handshake latch is never awaited long enough to see the
    // echoed flow, deterministically driving pause() down the timeout path
    consumer.pauseHandshakeTimeout = Duration.ZERO;

    consumer.pause();
    // doPause has already zeroed the link credit by this point: falling back to UNPAUSED here
    // would leave the consumer stalled with no way to re-credit the link
    assertThat(consumer.pausedOrPausing()).isTrue();

    consumer.unpause();

    Sync publishSync = sync(initialCredits);
    IntStream.range(0, initialCredits)
        .forEach(ignored -> publisher.publish(publisher.message(), ctx -> publishSync.down()));
    assertThat(publishSync).completes();

    assertThat(consumeSync).completes();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void consumptionProgressesWithInitialCreditsOfOneAndAsyncSettlement(int initialCredits) {
    connection.management().queue(this.q).declare();
    Publisher publisher = connection.publisherBuilder().queue(this.q).build();

    int messageCount = 20;
    Sync consumeSync = sync(messageCount);
    ExecutorService settlementExecutor = Executors.newSingleThreadExecutor();
    AmqpConsumer consumer =
        (AmqpConsumer)
            connection
                .consumerBuilder()
                .queue(this.q)
                .initialCredits(initialCredits)
                .messageHandler(
                    (ctx, msg) -> {
                      // settle from another thread, well after the handler returns: the
                      // replenish triggered by this settle is what must eventually grant
                      // credit back, since with such a small window, completion alone
                      // (pendingWorkItems reaching 0) is not enough
                      settlementExecutor.execute(
                          () -> {
                            ctx.accept();
                            consumeSync.down();
                          });
                    })
                .build();

    try {
      Sync publishSync = sync(messageCount);
      IntStream.range(0, messageCount)
          .forEach(ignored -> publisher.publish(publisher.message(), ctx -> publishSync.down()));
      assertThat(publishSync).completes();

      assertThat(consumeSync).completes();
      assertCreditInvariants(consumer, initialCredits);
    } finally {
      settlementExecutor.shutdownNow();
    }
  }

  @Test
  void batchAddFromAnotherConsumerThrows() {
    connection.management().queue(this.q).declare();
    String q2 = this.q + "-2";
    connection.management().queue(q2).declare();
    try {
      Publisher publisher1 = connection.publisherBuilder().queue(this.q).build();
      Publisher publisher2 = connection.publisherBuilder().queue(q2).build();

      AtomicReference<Consumer.Context> context1 = new AtomicReference<>();
      Sync received1 = sync();
      connection
          .consumerBuilder()
          .queue(this.q)
          .messageHandler(
              (ctx, msg) -> {
                context1.set(ctx);
                received1.down();
              })
          .build();

      AtomicReference<Consumer.Context> context2 = new AtomicReference<>();
      Sync received2 = sync();
      connection
          .consumerBuilder()
          .queue(q2)
          .messageHandler(
              (ctx, msg) -> {
                context2.set(ctx);
                received2.down();
              })
          .build();

      publisher1.publish(publisher1.message(), ctx -> {});
      publisher2.publish(publisher2.message(), ctx -> {});
      assertThat(received1).completes();
      assertThat(received2).completes();

      Consumer.BatchContext batch = context1.get().batch(10);
      assertThatThrownBy(() -> batch.add(context2.get()))
          .isInstanceOf(IllegalArgumentException.class);

      // clean up: both messages are still unsettled, the failed add() above must not have
      // consumed context2
      context1.get().accept();
      context2.get().accept();
    } finally {
      connection.management().queueDelete(q2);
    }
  }

  @Test
  void batchSkipsStaleContextsAfterRecovery(TestInfo info) {
    String cName = name(info);
    connection.management().queue(this.q).type(QUORUM).declare();
    Connection c =
        ((AmqpConnectionBuilder) environment.connectionBuilder())
            .name(cName)
            .recovery()
            .backOffDelayPolicy(backOffDelayPolicy)
            .connectionBuilder()
            .build();
    Publisher publisher = c.publisherBuilder().queue(this.q).build();

    byte[] freshBody = "fresh".getBytes(UTF_8);
    AtomicReference<Consumer.Context> staleContext = new AtomicReference<>();
    Sync staleReceived = sync();
    AtomicReference<Consumer.Context> freshContext = new AtomicReference<>();
    Sync freshReceived = sync();
    Sync recoveredSync = sync();

    c.consumerBuilder()
        .queue(this.q)
        .listeners(recoveredListener(recoveredSync))
        .messageHandler(
            (ctx, msg) -> {
              // identify the fresh (post-recovery) delivery by content rather than by timing:
              // the stale, unsettled first message is requeued and may be redelivered on the
              // new generation too, racing with the second publish below
              if (Arrays.equals(msg.body(), freshBody)) {
                freshContext.set(ctx);
                freshReceived.down();
              } else if (staleContext.get() == null) {
                staleContext.set(ctx);
                staleReceived.down();
              }
            })
        .build();

    publisher.publish(publisher.message(), ctx -> {});
    assertThat(staleReceived).completes();

    closeConnection(cName);
    assertThat(recoveredSync).completes();
    waitAtMost(() -> ((ResourceBase) c).state() == OPEN);

    publisher.publish(publisher.message(freshBody), ctx -> {});
    assertThat(freshReceived).completes();

    Consumer.BatchContext batch = freshContext.get().batch(10);
    batch.add(staleContext.get()); // stale generation: must be skipped, not throw
    assertThat(batch.size()).isEqualTo(0);

    batch.add(freshContext.get());
    assertThat(batch.size()).isEqualTo(1);
    batch.accept();

    c.close();
  }

  private static Resource.StateListener recoveredListener(Sync sync) {
    return context -> {
      if (context.previousState() == RECOVERING && context.currentState() == OPEN) {
        sync.down();
      }
    };
  }

  // Waits for pendingWorkItems to drain, then checks
  // the bounds hold. Credit is NOT expected to necessarily climb back to initialCredits: the
  // replenish formula stops topping up once credit exceeds half the window, so it can rest
  // anywhere in (initialCredits / 2, initialCredits] once idle.
  private static void assertCreditInvariants(AmqpConsumer consumer, int initialCredits) {
    waitAtMost(() -> consumer.pendingWorkItems() == 0);
    assertThat(consumer.credits()).isBetween(0, initialCredits);
    assertThat(consumer.pendingWorkItems()).isBetween(0, initialCredits);
  }
}
