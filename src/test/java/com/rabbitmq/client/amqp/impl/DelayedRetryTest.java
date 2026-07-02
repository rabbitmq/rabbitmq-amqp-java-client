// Copyright (c) 2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.client.amqp.Management.DelayedRetryType.ALL;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.Publisher;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

@AmqpTestInfrastructure
public class DelayedRetryTest {

  Connection connection;
  Management management;
  String q;

  @BeforeEach
  void init(TestInfo info) {
    this.management = connection.management();
    this.q = TestUtils.name(info);
  }

  @AfterEach
  void tearDown() {
    this.management.queueDelete(q);
  }

  @Test
  void requeuedMessageShouldBeRequeuedAfterDelay() {
    Duration retryDelay = ofMillis(1000);
    this.management
        .queue()
        .name(q)
        .quorum()
        .delayedRetryType(ALL)
        .delayedRetryMin(retryDelay)
        .queue()
        .declare();

    Publisher publisher = this.connection.publisherBuilder().queue(q).build();
    AtomicInteger deliveryCount = new AtomicInteger();
    TestUtils.Sync redeliveredSync = TestUtils.sync();
    Queue<Message> messages = new ArrayBlockingQueue<>(2);
    AtomicLong timeBetweenDeliveriesNs = new AtomicLong();

    this.connection
        .consumerBuilder()
        .queue(q)
        .messageHandler(
            (context, message) -> {
              deliveryCount.incrementAndGet();
              messages.offer(message);
              if (deliveryCount.get() == 1) {
                timeBetweenDeliveriesNs.set(System.nanoTime());
                context.requeue();
              } else {
                long now = System.nanoTime();
                timeBetweenDeliveriesNs.set(now - timeBetweenDeliveriesNs.get());
                context.accept();
                redeliveredSync.down();
              }
            })
        .build();

    publisher.publish(publisher.message(), ctx -> {});
    assertThat(redeliveredSync).completes();

    Message message1 = messages.poll();
    Message message2 = messages.poll();

    assertThat(message1).isNotNull().doesNotHaveAnnotation("x-acquired-count").hasDeliveryCount(0L);
    assertThat(message2).isNotNull().hasAnnotation("x-acquired-count", 1L).hasDeliveryCount(0L);

    waitAtMost(() -> management.queueInfo(q).messageCount() == 0);

    assertThat(Duration.ofNanos(timeBetweenDeliveriesNs.get()))
        .isCloseTo(retryDelay, retryDelay.dividedBy(5));
  }

  @ParameterizedTest
  @EnumSource(ExplicitDeliveryTimeStrategy.class)
  void explicitDeliveryTimeInMessageShouldOverrideRetryDelay(
      ExplicitDeliveryTimeStrategy strategy) {
    Duration retryDelay = ofMillis(100);
    Duration deliveryTimeOffset = ofMillis(1000);
    this.management
        .queue()
        .name(q)
        .quorum()
        .delayedRetryType(ALL)
        .delayedRetryMin(retryDelay)
        .queue()
        .declare();

    Publisher publisher = this.connection.publisherBuilder().queue(q).build();
    AtomicInteger count = new AtomicInteger();
    TestUtils.Sync redeliveredSync = TestUtils.sync();
    AtomicLong timeBetweenDeliveriesNs = new AtomicLong();

    this.connection
        .consumerBuilder()
        .queue(q)
        .messageHandler(
            (context, message) -> {
              if (count.incrementAndGet() == 1) {
                timeBetweenDeliveriesNs.set(System.nanoTime());
                strategy.apply(context, deliveryTimeOffset);
              } else {
                timeBetweenDeliveriesNs.set(System.nanoTime() - timeBetweenDeliveriesNs.get());
                context.accept();
                redeliveredSync.down();
              }
            })
        .build();

    publisher.publish(publisher.message(), ctx -> {});
    assertThat(redeliveredSync).completes();
    waitAtMost(() -> management.queueInfo(q).messageCount() == 0);

    assertThat(Duration.ofNanos(timeBetweenDeliveriesNs.get()))
        .isCloseTo(deliveryTimeOffset, deliveryTimeOffset.dividedBy(5));
  }

  @ParameterizedTest
  @EnumSource(DelayedRetryStrategy.class)
  void delayedRetryShouldRedeliver(DelayedRetryStrategy strategy) {
    Duration retryDelay = ofMillis(1000);
    this.management
        .queue()
        .name(q)
        .quorum()
        .delayedRetryType(ALL)
        .delayedRetryMin(retryDelay)
        .queue()
        .declare();

    Publisher publisher = this.connection.publisherBuilder().queue(q).build();
    AtomicInteger count = new AtomicInteger();
    TestUtils.Sync redeliveredSync = TestUtils.sync();
    AtomicLong timeBetweenDeliveriesNs = new AtomicLong();

    this.connection
        .consumerBuilder()
        .queue(q)
        .messageHandler(
            (context, message) -> {
              if (count.incrementAndGet() == 1) {
                timeBetweenDeliveriesNs.set(System.nanoTime());
                strategy.apply(context, retryDelay);
              } else {
                timeBetweenDeliveriesNs.set(System.nanoTime() - timeBetweenDeliveriesNs.get());
                context.accept();
                redeliveredSync.down();
              }
            })
        .build();

    publisher.publish(publisher.message(), ctx -> {});
    assertThat(redeliveredSync).completes();
    waitAtMost(() -> management.queueInfo(q).messageCount() == 0);

    assertThat(Duration.ofNanos(timeBetweenDeliveriesNs.get()))
        .isCloseTo(retryDelay, retryDelay.dividedBy(5));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void deliveryFailedFlagShouldControlDeliveryCount(boolean deliveryFailed) {
    Duration retryDelay = ofMillis(500);
    this.management
        .queue()
        .name(q)
        .quorum()
        .delayedRetryType(ALL)
        .delayedRetryMin(retryDelay)
        .queue()
        .declare();

    Publisher publisher = this.connection.publisherBuilder().queue(q).build();
    AtomicInteger count = new AtomicInteger();
    TestUtils.Sync redeliveredSync = TestUtils.sync();
    Queue<Message> messages = new ArrayBlockingQueue<>(1);

    this.connection
        .consumerBuilder()
        .queue(q)
        .messageHandler(
            (context, message) -> {
              if (count.incrementAndGet() == 1) {
                context.delayedRetry(retryDelay, deliveryFailed);
              } else {
                messages.offer(message);
                context.accept();
                redeliveredSync.down();
              }
            })
        .build();

    publisher.publish(publisher.message(), ctx -> {});
    assertThat(redeliveredSync).completes();

    Message redelivered = messages.poll();
    assertThat(redelivered).isNotNull().hasDeliveryCount(deliveryFailed ? 1L : 0L);
    waitAtMost(() -> management.queueInfo(q).messageCount() == 0);
  }

  enum ExplicitDeliveryTimeStrategy {
    REQUEUE_WITH_ANNOTATION {
      @Override
      public void apply(Consumer.Context ctx, Duration deliveryTimeOffset) {
        long time = System.currentTimeMillis() + deliveryTimeOffset.toMillis();
        ctx.requeue(Map.of("x-opt-delivery-time", time));
      }
    },
    DELAYED_RETRY_DURATION {
      @Override
      public void apply(Consumer.Context ctx, Duration deliveryTimeOffset) {
        ctx.delayedRetry(deliveryTimeOffset);
      }
    },
    DELAYED_RETRY_INSTANT {
      @Override
      public void apply(Consumer.Context ctx, Duration deliveryTimeOffset) {
        ctx.delayedRetry(Instant.now().plus(deliveryTimeOffset));
      }
    };

    public abstract void apply(Consumer.Context ctx, Duration deliveryTimeOffset);
  }

  enum DelayedRetryStrategy {
    DURATION {
      @Override
      public void apply(Consumer.Context ctx, Duration delay) {
        ctx.delayedRetry(delay);
      }
    },
    DURATION_NO_FAILURE {
      @Override
      public void apply(Consumer.Context ctx, Duration delay) {
        ctx.delayedRetry(delay, false);
      }
    },
    DURATION_DELIVERY_FAILED {
      @Override
      public void apply(Consumer.Context ctx, Duration delay) {
        ctx.delayedRetry(delay, true);
      }
    },
    INSTANT {
      @Override
      public void apply(Consumer.Context ctx, Duration delay) {
        ctx.delayedRetry(Instant.now().plus(delay));
      }
    },
    INSTANT_NO_FAILURE {
      @Override
      public void apply(Consumer.Context ctx, Duration delay) {
        ctx.delayedRetry(Instant.now().plus(delay), false);
      }
    },
    INSTANT_DELIVERY_FAILED {
      @Override
      public void apply(Consumer.Context ctx, Duration delay) {
        ctx.delayedRetry(Instant.now().plus(delay), true);
      }
    };

    public abstract void apply(Consumer.Context ctx, Duration delay);
  }
}
