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
import static com.rabbitmq.client.amqp.Management.QueueType.STREAM;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersion.RABBITMQ_4_1_0;
import static com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersion.RABBITMQ_4_2_0;
import static com.rabbitmq.client.amqp.impl.TestUtils.sync;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitUntilStable;
import static java.nio.charset.StandardCharsets.*;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersionAtLeast;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.arbitraries.ArrayArbitrary;
import net.jqwik.api.arbitraries.StringArbitrary;
import org.apache.qpid.protonj2.types.Symbol;
import org.junit.jupiter.api.*;

@AmqpTestInfrastructure
public class SourceFiltersTest {

  Connection connection;
  String name;
  ArrayArbitrary<Byte, byte[]> binaryArbitrary;
  StringArbitrary stringArbitrary;

  @BeforeEach
  void init(TestInfo info) {
    this.name = TestUtils.name(info);
    connection.management().queue(this.name).type(STREAM).declare();
    binaryArbitrary = Arbitraries.bytes().array(byte[].class).ofMinSize(10).ofMaxSize(20);
    stringArbitrary = Arbitraries.strings().ofMinLength(10).ofMaxLength(20);
  }

  @AfterEach
  void tearDown() {
    connection.management().queueDelete(this.name);
  }

  @Test
  void streamConsumerOptionsOffsetLong() {
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
    assertThat(consumeSync).completes();
    SortedSet<Long> offsetTail = offsets.tailSet(offsets.last() / 2);
    assertThat(offsetTail.first()).isPositive();
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
    assertThat(consumeSync).completes();
    consumer.close();
  }

  @Test
  void streamConsumerOptionsOffsetFirst() {
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
    assertThat(consumeSync).completes();
    consumer.close();
  }

  @Test
  void streamConsumerOptionsOffsetLast() {
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
    assertThat(consumeSync).completes();
    assertThat(firstOffset).hasPositiveValue();
    consumer.close();
  }

  @Test
  void streamConsumerOptionsOffsetNext() {
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
    assertThat(openSync).completes();
    publish(messageCount);
    assertThat(consumeSync).completes();
    assertThat(firstOffset).hasPositiveValue();
    consumer.close();
  }

  @Test
  void streamConsumerOptionsOffsetTimestamp() {
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
    assertThat(consumeSync).completes();
    consumer.close();
  }

  @Test
  void streamConsumerOptionsOffsetInterval() {
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
    assertThat(consumeSync).completes();
    consumer.close();
  }

  @Test
  void streamConsumerOptionsOffsetIntervalWithInvalidSyntaxShouldThrow() {
    assertThatThrownBy(() -> connection.consumerBuilder().stream().offset("foo"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void streamFiltering() {
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
    waitUntilStable(receivedCount::get);
    assertThat(receivedCount)
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
    waitUntilStable(receivedCount::get);
    assertThat(receivedCount)
        .hasValueGreaterThanOrEqualTo(2 * messageWaveCount)
        .hasValueLessThan(waveCount * messageWaveCount);
    consumer.close();
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void filterExpressionApplicationProperties() {
    int messageCount = 10;
    UUID uuid = UUID.randomUUID();
    long now = System.currentTimeMillis();
    byte[] binary = binaryArbitrary.sample();
    publish(messageCount, msg -> msg.property("foo", true));
    publish(messageCount, msg -> msg.property("foo", 42));
    publish(messageCount, msg -> msg.property("foo", 42.1));
    publish(messageCount, msg -> msg.property("foo", now));
    publish(messageCount, msg -> msg.property("foo", uuid));
    publish(messageCount, msg -> msg.property("foo", binary));
    publish(messageCount, msg -> msg.property("foo", "bar"));
    publish(messageCount, msg -> msg.property("foo", "baz"));
    publish(messageCount, msg -> msg.property("foo", "bar"));
    publish(messageCount, msg -> msg.propertySymbol("foo", "symbol"));
    publish(messageCount, msg -> msg.property("foo", "bar").property("k1", 42));

    Collection<Message> msgs = consume(messageCount, options -> options.property("foo", true));
    msgs.forEach(m -> assertThat(m).hasProperty("foo", true));

    msgs = consume(messageCount, options -> options.property("foo", 42));
    msgs.forEach(m -> assertThat(m).hasProperty("foo", 42));

    msgs = consume(messageCount, options -> options.property("foo", 42.1));
    msgs.forEach(m -> assertThat(m).hasProperty("foo", 42.1));

    msgs = consume(messageCount, options -> options.propertyTimestamp("foo", now));
    msgs.forEach(m -> assertThat(m).hasProperty("foo", now));

    msgs = consume(messageCount, options -> options.property("foo", uuid));
    msgs.forEach(m -> assertThat(m).hasProperty("foo", uuid));

    msgs = consume(messageCount, options -> options.property("foo", binary));
    msgs.forEach(m -> assertThat(m).hasProperty("foo", binary));

    msgs = consume(messageCount, options -> options.property("foo", "baz"));
    msgs.forEach(m -> assertThat(m).hasProperty("foo", "baz"));

    msgs = consume(messageCount * 3, options -> options.property("foo", "bar"));
    msgs.forEach(m -> assertThat(m).hasProperty("foo", "bar"));

    msgs = consume(messageCount * 4, options -> options.property("foo", "&p:b"));
    assertThat(msgs).allMatch(m -> m.property("foo").toString().startsWith("b"));

    msgs = consume(messageCount, options -> options.propertySymbol("foo", "symbol"));
    msgs.forEach(m -> assertThat(m).hasProperty("foo", Symbol.valueOf("symbol")));

    msgs = consume(messageCount, options -> options.property("foo", "bar").property("k1", 42));
    msgs.forEach(m -> assertThat(m).hasProperty("foo", "bar").hasProperty("k1", 42));
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void filterExpressionProperties() {
    int messageCount = 10;
    byte[] userId = "guest".getBytes(UTF_8);
    long now = System.currentTimeMillis();
    long later = now + Duration.ofMinutes(10).toMillis();
    UUID uuid = UUID.randomUUID();
    publish(messageCount, msg -> msg.messageId(42));
    publish(messageCount, msg -> msg.messageId(42 * 2));
    publish(messageCount, msg -> msg.messageId(uuid));
    publish(messageCount, msg -> msg.correlationId(43));
    publish(messageCount, msg -> msg.correlationId(43 * 2));
    publish(messageCount, msg -> msg.correlationId(uuid));
    publish(messageCount, msg -> msg.userId(userId));
    publish(messageCount, msg -> msg.to("to foo bar"));
    publish(messageCount, msg -> msg.to("to foo baz"));
    publish(messageCount, msg -> msg.subject("subject foo bar"));
    publish(messageCount, msg -> msg.subject("subject foo baz"));
    publish(messageCount, msg -> msg.replyTo("reply-to foo bar"));
    publish(messageCount, msg -> msg.replyTo("reply-to foo baz"));
    publish(messageCount, msg -> msg.contentType("text/plain"));
    publish(messageCount, msg -> msg.contentType("text/html"));
    publish(messageCount, msg -> msg.contentEncoding("gzip"));
    publish(messageCount, msg -> msg.contentEncoding("zstd"));
    publish(messageCount, msg -> msg.absoluteExpiryTime(now));
    publish(messageCount, msg -> msg.absoluteExpiryTime(later));
    publish(messageCount, msg -> msg.creationTime(now));
    publish(messageCount, msg -> msg.creationTime(later));
    publish(messageCount, msg -> msg.groupId("group-id foo bar"));
    publish(messageCount, msg -> msg.groupId("group-id foo baz"));
    publish(messageCount, msg -> msg.groupSequence(44));
    publish(messageCount, msg -> msg.groupSequence(44 * 2));
    publish(messageCount, msg -> msg.replyToGroupId("reply-to-group-id foo bar"));
    publish(messageCount, msg -> msg.replyToGroupId("reply-to-group-id foo baz"));

    Collection<Message> msgs = consume(messageCount, options -> options.messageId(42));
    msgs.forEach(m -> assertThat(m).hasId(42));

    msgs = consume(messageCount, options -> options.messageId(uuid));
    msgs.forEach(m -> assertThat(m).hasId(uuid));

    msgs = consume(messageCount, options -> options.correlationId(43));
    msgs.forEach(m -> assertThat(m).hasCorrelationId(43));

    msgs = consume(messageCount, options -> options.correlationId(uuid));
    msgs.forEach(m -> assertThat(m).hasCorrelationId(uuid));

    msgs = consume(messageCount, options -> options.userId(userId));
    msgs.forEach(m -> assertThat(m).hasUserId(userId));

    msgs = consume(messageCount, options -> options.to("to foo bar"));
    msgs.forEach(m -> assertThat(m).hasTo("to foo bar"));

    msgs = consume(messageCount, options -> options.subject("subject foo bar"));
    msgs.forEach(m -> assertThat(m).hasSubject("subject foo bar"));

    msgs = consume(messageCount, options -> options.replyTo("reply-to foo bar"));
    msgs.forEach(m -> assertThat(m).hasReplyTo("reply-to foo bar"));

    msgs = consume(messageCount, options -> options.contentType("text/html"));
    msgs.forEach(m -> assertThat(m).hasContentType("text/html"));

    msgs = consume(messageCount, options -> options.contentEncoding("zstd"));
    msgs.forEach(m -> assertThat(m).hasContentEncoding("zstd"));

    msgs = consume(messageCount, options -> options.absoluteExpiryTime(later));
    msgs.forEach(m -> assertThat(m).hasAbsoluteExpiryTime(later));

    msgs = consume(messageCount, options -> options.creationTime(later));
    msgs.forEach(m -> assertThat(m).hasCreationTime(later));

    msgs = consume(messageCount, options -> options.groupId("group-id foo baz"));
    msgs.forEach(m -> assertThat(m).hasGroupId("group-id foo baz"));

    msgs = consume(messageCount, options -> options.groupSequence(44));
    msgs.forEach(m -> assertThat(m).hasGroupSequence(44));

    msgs = consume(messageCount, options -> options.replyToGroupId("reply-to-group-id foo baz"));
    msgs.forEach(m -> assertThat(m).hasReplyToGroupId("reply-to-group-id foo baz"));
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void filterExpressionsPropertiesAndApplicationProperties() {
    int messageCount = 10;
    String subject = stringArbitrary.sample();
    String appKey = stringArbitrary.sample();
    int appValue = new Random().nextInt();
    byte[] body1 = binaryArbitrary.sample();
    byte[] body2 = binaryArbitrary.sample();
    byte[] body3 = binaryArbitrary.sample();

    publish(messageCount, msg -> msg.subject(subject).body(body1));
    publish(messageCount, msg -> msg.property(appKey, appValue).body(body2));
    publish(messageCount, msg -> msg.subject(subject).property(appKey, appValue).body(body3));

    List<Message> msgs = consume(messageCount * 2, options -> options.subject(subject));
    msgs.subList(0, messageCount).forEach(m -> assertThat(m).hasSubject(subject).hasBody(body1));
    msgs.subList(messageCount, messageCount * 2)
        .forEach(m -> assertThat(m).hasSubject(subject).hasBody(body3));

    msgs = consume(messageCount * 2, options -> options.property(appKey, appValue));
    msgs.subList(0, messageCount)
        .forEach(m -> assertThat(m).hasProperty(appKey, appValue).hasBody(body2));
    msgs.subList(messageCount, messageCount * 2)
        .forEach(m -> assertThat(m).hasProperty(appKey, appValue).hasBody(body3));

    msgs = consume(messageCount, options -> options.subject(subject).property(appKey, appValue));
    msgs.subList(0, messageCount)
        .forEach(
            m -> assertThat(m).hasSubject(subject).hasProperty(appKey, appValue).hasBody(body3));
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void filterExpressionFilterFewMessagesFromManyToTestFlowControl() {
    String groupId = stringArbitrary.sample();
    publish(1, m -> m.groupId(groupId));
    publish(1000);
    publish(1, m -> m.groupId(groupId));

    List<Message> msgs = consume(2, m -> m.groupId(groupId));
    msgs.forEach(m -> assertThat(m).hasGroupId(groupId));
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void filterExpressionStringModifier() {
    publish(1, m -> m.subject("abc 123"));
    publish(1, m -> m.subject("foo bar"));
    publish(1, m -> m.subject("ab 12"));

    List<Message> msgs = consume(2, m -> m.subject("&p:ab"));
    msgs.forEach(m -> assertThat(m.subject()).startsWith("ab"));

    msgs = consume(1, m -> m.subject("&s:bar"));
    msgs.forEach(m -> assertThat(m).hasSubject("foo bar"));
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_2_0)
  void sqlFilterExpressionsShouldFilterMessages() {
    publish(1, m -> m.subject("abc 123"));
    publish(1, m -> m.subject("foo bar"));
    publish(1, m -> m.subject("ab 12"));

    List<Message> msgs = consume(2, m -> m.sql("properties.subject LIKE 'ab%'"));
    msgs.forEach(m -> assertThat(m.subject()).startsWith("ab"));

    msgs = consume(1, m -> m.sql("properties.subject like 'foo%'"));
    msgs.forEach(m -> assertThat(m).hasSubject("foo bar"));
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_2_0)
  void incorrectFilterShouldThrowException() {
    assertThatThrownBy(
            () ->
                connection.consumerBuilder().queue(name).messageHandler((ctx, msg) -> {}).stream()
                    .offset(FIRST)
                    .filter()
                    .sql("TRUE TRUE")
                    .stream()
                    .builder()
                    .build())
        .isInstanceOf(AmqpException.class)
        .hasMessageContaining("filters do not match");
  }

  void publish(int messageCount) {
    this.publish(messageCount, UnaryOperator.identity());
  }

  void publish(int messageCount, String filterValue) {
    publish(messageCount, msg -> msg.annotation("x-stream-filter-value", filterValue));
  }

  void publish(int messageCount, UnaryOperator<Message> messageLogic) {
    try (Publisher publisher = connection.publisherBuilder().queue(name).build()) {
      Sync publishSync = sync(messageCount);
      Publisher.Callback callback = ctx -> publishSync.down();
      range(0, messageCount)
          .forEach(ignored -> publisher.publish(messageLogic.apply(publisher.message()), callback));
      assertThat(publishSync).completes();
    }
  }

  List<Message> consume(
      int expectedMessageCount,
      java.util.function.Consumer<ConsumerBuilder.StreamFilterOptions> filterOptions) {
    Queue<Message> messages = new LinkedBlockingQueue<>();
    Sync consumedSync = sync(expectedMessageCount);
    AtomicInteger receivedMessageCount = new AtomicInteger();
    ConsumerBuilder builder =
        connection
            .consumerBuilder()
            .queue(this.name)
            .messageHandler(
                (ctx, msg) -> {
                  messages.add(msg);
                  receivedMessageCount.incrementAndGet();
                  consumedSync.down();
                  ctx.accept();
                });

    filterOptions.accept(builder.stream().offset(FIRST).filter());

    try (Consumer ignored = builder.build()) {
      assertThat(consumedSync).completes();
      waitUntilStable(receivedMessageCount::get, Duration.ofMillis(50));
      assertThat(receivedMessageCount).hasValue(expectedMessageCount);
    }

    return List.copyOf(messages);
  }
}
