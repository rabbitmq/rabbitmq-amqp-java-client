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
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.Publisher;
import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

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

  @Test
  void explicitDeliveryTimeInMessageShouldOverrideRetryDelay() {
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
                long time = System.currentTimeMillis() + deliveryTimeOffset.toMillis();
                context.requeue(Map.of("x-opt-delivery-time", time));
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

    assertThat(message1).isNotNull();
    assertThat(message2).isNotNull();

    waitAtMost(() -> management.queueInfo(q).messageCount() == 0);
    assertThat(Duration.ofNanos(timeBetweenDeliveriesNs.get()))
        .isCloseTo(deliveryTimeOffset, deliveryTimeOffset.dividedBy(5));
  }
}
