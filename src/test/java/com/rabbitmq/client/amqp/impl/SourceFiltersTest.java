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

import static com.rabbitmq.client.amqp.ConsumerBuilder.StreamOffsetSpecification.*;
import static com.rabbitmq.client.amqp.impl.TestUtils.sync;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(AmqpTestInfrastructureExtension.class)
public class SourceFiltersTest {

  Connection connection;
  String name;

  @BeforeEach
  void init(TestInfo info) {
    this.name = TestUtils.name(info);
  }

  @Test
  void streamConsumerOptionsOffsetLong() {
    connection.management().queue(name).type(Management.QueueType.STREAM).declare();
    try {
      int messageCount = 100;
      publish(messageCount);

      SortedSet<Long> offsets = new ConcurrentSkipListSet<>();
      Sync consumeSync = sync(messageCount);
      Consumer consumer =
          connection.consumerBuilder().queue(name).stream()
              .offset(0L)
              .builder()
              .messageHandler(
                  (ctx, msg) -> {
                    ctx.accept();
                    offsets.add((Long) msg.annotation("x-stream-offset"));
                    consumeSync.down();
                  })
              .build();
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(consumeSync).completes();
      SortedSet<Long> offsetTail = offsets.tailSet(offsets.last() / 2);
      Assertions.assertThat(offsetTail.first()).isPositive();
      consumer.close();
      consumeSync.reset(offsetTail.size());
      consumer =
          connection.consumerBuilder().stream()
              .offset(offsetTail.first())
              .builder()
              .queue(name)
              .messageHandler(
                  (ctx, msg) -> {
                    ctx.accept();
                    consumeSync.down();
                  })
              .build();
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(consumeSync).completes();
      consumer.close();
    } finally {
      connection.management().queueDeletion().delete(name);
    }
  }

  @Test
  void streamConsumerOptionsOffsetFirst() {
    connection.management().queue(name).type(Management.QueueType.STREAM).declare();
    try {
      int messageCount = 100;
      publish(messageCount);

      Sync consumeSync = sync(messageCount);
      Consumer consumer =
          connection.consumerBuilder().queue(name).stream()
              .offset(FIRST)
              .builder()
              .messageHandler(
                  (ctx, msg) -> {
                    ctx.accept();
                    consumeSync.down();
                  })
              .build();
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(consumeSync).completes();
      consumer.close();
    } finally {
      connection.management().queueDeletion().delete(name);
    }
  }

  @Test
  void streamConsumerOptionsOffsetLast() {
    connection.management().queue(name).type(Management.QueueType.STREAM).declare();
    try {
      int messageCount = 100;
      publish(messageCount);

      Sync consumeSync = sync(1);
      AtomicLong firstOffset = new AtomicLong(-1);
      Consumer consumer =
          connection.consumerBuilder().queue(name).stream()
              .offset(LAST)
              .builder()
              .messageHandler(
                  (ctx, msg) -> {
                    ctx.accept();
                    firstOffset.compareAndSet(-1, (Long) msg.annotation("x-stream-offset"));
                    consumeSync.down();
                  })
              .build();
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(consumeSync).completes();
      Assertions.assertThat(firstOffset).hasPositiveValue();
      consumer.close();
    } finally {
      connection.management().queueDeletion().delete(name);
    }
  }

  @Test
  void streamConsumerOptionsOffsetNext() {
    connection.management().queue(name).type(Management.QueueType.STREAM).declare();
    try {
      int messageCount = 100;
      publish(messageCount);

      Sync consumeSync = sync(messageCount);
      Sync openSync = sync(1);
      AtomicLong firstOffset = new AtomicLong(-1);
      Consumer consumer =
          connection.consumerBuilder().queue(name).stream()
              .offset(NEXT)
              .builder()
              .listeners(
                  context -> {
                    if (context.currentState() == Resource.State.OPEN) {
                      openSync.down();
                    }
                  })
              .messageHandler(
                  (ctx, msg) -> {
                    ctx.accept();
                    firstOffset.compareAndSet(-1, (Long) msg.annotation("x-stream-offset"));
                    consumeSync.down();
                  })
              .build();
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(openSync).completes();
      publish(messageCount);
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(consumeSync).completes();
      Assertions.assertThat(firstOffset).hasPositiveValue();
      consumer.close();
    } finally {
      connection.management().queueDeletion().delete(name);
    }
  }

  @Test
  void streamConsumerOptionsOffsetTimestamp() {
    connection.management().queue(name).type(Management.QueueType.STREAM).declare();
    try {
      int messageCount = 100;
      publish(messageCount);

      Sync consumeSync = sync(messageCount);
      Consumer consumer =
          connection.consumerBuilder().queue(name).stream()
              .offset(Instant.now().minus(Duration.ofMinutes(10)))
              .builder()
              .messageHandler(
                  (ctx, msg) -> {
                    ctx.accept();
                    consumeSync.down();
                  })
              .build();
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(consumeSync).completes();
      consumer.close();
    } finally {
      connection.management().queueDeletion().delete(name);
    }
  }

  @Test
  void streamConsumerOptionsOffsetInterval() {
    connection.management().queue(name).type(Management.QueueType.STREAM).declare();
    try {
      int messageCount = 100;
      publish(messageCount);

      Sync consumeSync = sync(messageCount);
      Consumer consumer =
          connection.consumerBuilder().queue(name).stream()
              .offset("10m")
              .builder()
              .messageHandler(
                  (ctx, msg) -> {
                    ctx.accept();
                    consumeSync.down();
                  })
              .build();
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(consumeSync).completes();
      consumer.close();
    } finally {
      connection.management().queueDeletion().delete(name);
    }
  }

  @Test
  void streamConsumerOptionsOffsetIntervalWithInvalidSyntaxShouldThrow() {
    assertThatThrownBy(() -> connection.consumerBuilder().stream().offset("foo"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void streamFiltering() {
    connection.management().queue(name).type(Management.QueueType.STREAM).declare();
    try {
      int messageWaveCount = 100;
      List<String> waves = List.of("apple", "orange", "", "banana");
      waves.forEach(v -> publish(messageWaveCount, v));
      int waveCount = waves.size();

      AtomicInteger receivedCount = new AtomicInteger(0);
      Consumer consumer =
          connection.consumerBuilder().queue(name).stream()
              .offset(FIRST)
              .filterValues("banana")
              .filterMatchUnfiltered(false)
              .builder()
              .messageHandler(
                  (ctx, msg) -> {
                    receivedCount.incrementAndGet();
                    ctx.accept();
                  })
              .build();
      TestUtils.waitUntilStable(receivedCount::get);
      Assertions.assertThat(receivedCount)
          .hasValueGreaterThanOrEqualTo(messageWaveCount)
          .hasValueLessThan(waveCount * messageWaveCount);
      consumer.close();

      receivedCount.set(0);
      consumer =
          connection.consumerBuilder().queue(name).stream()
              .offset(FIRST)
              .filterValues("banana")
              .filterMatchUnfiltered(true)
              .builder()
              .messageHandler(
                  (ctx, msg) -> {
                    receivedCount.incrementAndGet();
                    ctx.accept();
                  })
              .build();
      TestUtils.waitUntilStable(receivedCount::get);
      Assertions.assertThat(receivedCount)
          .hasValueGreaterThanOrEqualTo(2 * messageWaveCount)
          .hasValueLessThan(waveCount * messageWaveCount);
      consumer.close();
    } finally {
      connection.management().queueDeletion().delete(name);
    }
  }

  void publish(int messageCount) {
    this.publish(messageCount, null);
  }

  void publish(int messageCount, String filterValue) {
    try (Publisher publisher = connection.publisherBuilder().queue(name).build()) {
      Sync publishSync = sync(messageCount);
      range(0, messageCount)
          .forEach(
              ignored -> {
                Message message = publisher.message();
                if (filterValue != null && !filterValue.isBlank()) {
                  message.annotation("x-stream-filter-value", filterValue);
                }
                publisher.publish(message, ctx -> publishSync.down());
              });
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(publishSync).completes();
    }
  }
}
