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
import static com.rabbitmq.client.amqp.impl.TestUtils.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.sync;
import static java.util.stream.IntStream.range;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.time.Duration;
import java.time.Instant;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
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
      assertThat(consumeSync).completes();
      SortedSet<Long> offsetTail = offsets.tailSet(offsets.last() / 2);
      Assertions.assertThat(offsetTail.first()).isPositive();
      consumeSync.reset(offsetTail.size());
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
      assertThat(consumeSync).completes();
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
      connection.consumerBuilder().queue(name).stream()
          .offset(FIRST)
          .builder()
          .messageHandler(
              (ctx, msg) -> {
                ctx.accept();
                consumeSync.down();
              })
          .build();
      assertThat(consumeSync).completes();
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
      assertThat(consumeSync).completes();
      Assertions.assertThat(firstOffset).hasPositiveValue();
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
      assertThat(openSync).completes();
      publish(messageCount);
      assertThat(consumeSync).completes();
      Assertions.assertThat(firstOffset).hasPositiveValue();
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
      connection.consumerBuilder().queue(name).stream()
          .offset(Instant.now().minus(Duration.ofMinutes(10)))
          .builder()
          .messageHandler(
              (ctx, msg) -> {
                ctx.accept();
                consumeSync.down();
              })
          .build();
      assertThat(consumeSync).completes();
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
      connection.consumerBuilder().queue(name).stream()
          .offset("10m")
          .builder()
          .messageHandler(
              (ctx, msg) -> {
                ctx.accept();
                consumeSync.down();
              })
          .build();
      assertThat(consumeSync).completes();
    } finally {
      connection.management().queueDeletion().delete(name);
    }
  }

  void publish(int messageCount) {
    Publisher publisher = connection.publisherBuilder().queue(name).build();
    Sync publishSync = sync(messageCount);
    range(0, messageCount)
        .forEach(ignored -> publisher.publish(publisher.message(), ctx -> publishSync.down()));
    assertThat(publishSync).completes();
  }
}
