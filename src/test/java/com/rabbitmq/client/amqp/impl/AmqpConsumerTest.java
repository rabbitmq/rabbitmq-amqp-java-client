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
import static com.rabbitmq.client.amqp.Resource.State.*;
import static com.rabbitmq.client.amqp.impl.Assertions.*;
import static com.rabbitmq.client.amqp.impl.Cli.closeConnection;
import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static org.assertj.core.api.Assertions.*;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

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
    connection.management().queueDeletion().delete(this.q);
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

  private static Resource.StateListener recoveredListener(Sync sync) {
    return context -> {
      if (context.previousState() == RECOVERING && context.currentState() == OPEN) {
        sync.down();
      }
    };
  }
}
