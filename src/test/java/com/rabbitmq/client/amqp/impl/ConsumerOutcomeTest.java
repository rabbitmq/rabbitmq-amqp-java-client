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

import static com.rabbitmq.client.amqp.Management.ExchangeType.FANOUT;
import static com.rabbitmq.client.amqp.Management.QueueType.QUORUM;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.sync;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@AmqpTestInfrastructure
public class ConsumerOutcomeTest {

  private static final String ANNOTATION_KEY = "x-opt-string";
  private static final String ANNOTATION_VALUE = "bar";
  private static final String ANNOTATION_KEY_ARRAY = "x-opt-array";
  private static final String[] ANNOTATION_VALUE_ARRAY = new String[] {"foo", "bar", "baz"};
  private static final String ANNOTATION_KEY_LIST = "x-opt-list";
  private static final List<String> ANNOTATION_VALUE_LIST = List.of("one", "two", "three");
  private static final String ANNOTATION_KEY_MAP = "x-opt-map";
  private static final Map<String, String> ANNOTATION_VALUE_MAP =
      Map.of("k1", "v1", "k2", "v2", "k3", "v3");
  private static final Map<String, Object> ANNOTATIONS =
      Map.of(
          ANNOTATION_KEY,
          ANNOTATION_VALUE,
          ANNOTATION_KEY_ARRAY,
          ANNOTATION_VALUE_ARRAY,
          ANNOTATION_KEY_LIST,
          ANNOTATION_VALUE_LIST,
          ANNOTATION_KEY_MAP,
          ANNOTATION_VALUE_MAP);

  Connection connection;
  Management management;
  String q, dlx, dlq;

  @BeforeEach
  void init(TestInfo info) {
    this.management = connection.management();
    this.q = TestUtils.name(info);
    this.dlx = TestUtils.name(info);
    this.dlq = TestUtils.name(info);
  }

  @AfterEach
  void tearDown() {
    this.connection.management().queueDelete(q);
  }

  @Test
  void requeuedMessageShouldBeRequeued() {
    this.management.queue().name(q).type(QUORUM).declare();

    Publisher publisher = this.connection.publisherBuilder().queue(q).build();
    AtomicInteger deliveryCount = new AtomicInteger();
    TestUtils.Sync redeliveredSync = TestUtils.sync();
    Queue<Message> messages = new ArrayBlockingQueue<>(2);
    this.connection
        .consumerBuilder()
        .queue(q)
        .messageHandler(
            (context, message) -> {
              deliveryCount.incrementAndGet();
              messages.offer(message);
              if (deliveryCount.get() == 1) {
                context.requeue();
              } else {
                context.accept();
                redeliveredSync.down();
              }
            })
        .build();

    publisher.publish(publisher.message(), ctx -> {});
    assertThat(redeliveredSync).completes();
    Message message = messages.poll();
    assertThat(message).doesNotHaveAnnotation("x-delivery-count");
    message = messages.poll();
    assertThat(message).hasAnnotation("x-delivery-count", 1L);
    waitAtMost(() -> management.queueInfo(q).messageCount() == 0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void requeuedMessageWithAnnotationShouldContainAnnotationsOnRedelivery(boolean batch) {
    this.management.queue().name(q).type(QUORUM).declare();

    Publisher publisher = this.connection.publisherBuilder().queue(q).build();
    AtomicInteger deliveryCount = new AtomicInteger();
    TestUtils.Sync redeliveredSync = TestUtils.sync();
    Queue<Message> messages = new ArrayBlockingQueue<>(2);
    this.connection
        .consumerBuilder()
        .queue(q)
        .messageHandler(
            (ctx, message) -> {
              deliveryCount.incrementAndGet();
              messages.offer(message);
              if (deliveryCount.get() == 1) {
                if (batch) {
                  Consumer.BatchContext bc = ctx.batch(1);
                  bc.add(ctx);
                  ctx = bc;
                }
                ctx.requeue(ANNOTATIONS);
              } else {
                ctx.accept();
                redeliveredSync.down();
              }
            })
        .build();

    publisher.publish(publisher.message(), ctx -> {});
    assertThat(redeliveredSync).completes();
    Message message = messages.poll();
    assertThat(message).doesNotHaveAnnotation("x-delivery-count");
    message = messages.poll();
    assertThat(message)
        .hasAnnotation("x-delivery-count", 1L)
        .hasAnnotation(ANNOTATION_KEY, ANNOTATION_VALUE)
        .hasAnnotation(ANNOTATION_KEY_ARRAY, ANNOTATION_VALUE_ARRAY)
        .hasAnnotation(ANNOTATION_KEY_LIST, ANNOTATION_VALUE_LIST)
        .hasAnnotation(ANNOTATION_KEY_MAP, ANNOTATION_VALUE_MAP);
    waitAtMost(() -> management.queueInfo(q).messageCount() == 0);
  }

  @Test
  void discardedMessageShouldBeDeadLeadLetteredWhenConfigured() {
    declareDeadLetterTopology();
    Publisher publisher = this.connection.publisherBuilder().queue(q).build();
    this.connection.consumerBuilder().queue(q).messageHandler((ctx, msg) -> ctx.discard()).build();

    TestUtils.Sync deadLetteredSync = TestUtils.sync();
    AtomicReference<Message> deadLetteredMessage = new AtomicReference<>();
    this.connection
        .consumerBuilder()
        .queue(dlq)
        .messageHandler(
            (ctx, msg) -> {
              deadLetteredMessage.set(msg);
              ctx.accept();
              deadLetteredSync.down();
            })
        .build();

    UUID messageID = UUID.randomUUID();
    publisher.publish(publisher.message().messageId(messageID), ctx -> {});
    assertThat(deadLetteredSync).completes();
    assertThat(deadLetteredMessage).doesNotHaveNullValue();
    Message message = deadLetteredMessage.get();
    assertThat(message).hasId(messageID);
    waitAtMost(() -> management.queueInfo(q).messageCount() == 0);
    waitAtMost(() -> management.queueInfo(dlq).messageCount() == 0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void discardedMessageWithAnnotationsShouldBeDeadLeadLetteredAndContainAnnotationsWhenConfigured(
      boolean batch) {
    declareDeadLetterTopology();
    Publisher publisher = this.connection.publisherBuilder().queue(q).build();
    this.connection
        .consumerBuilder()
        .queue(q)
        .messageHandler(
            (ctx, msg) -> {
              if (batch) {
                Consumer.BatchContext bc = ctx.batch(1);
                bc.add(ctx);
                ctx = bc;
              }
              ctx.discard(ANNOTATIONS);
            })
        .build();

    TestUtils.Sync deadLetteredSync = TestUtils.sync();
    AtomicReference<Message> deadLetteredMessage = new AtomicReference<>();
    this.connection
        .consumerBuilder()
        .queue(dlq)
        .messageHandler(
            (ctx, msg) -> {
              deadLetteredMessage.set(msg);
              ctx.accept();
              deadLetteredSync.down();
            })
        .build();

    UUID messageID = UUID.randomUUID();
    publisher.publish(publisher.message().messageId(messageID), ctx -> {});
    assertThat(deadLetteredSync).completes();
    Message message = deadLetteredMessage.get();
    assertThat(message)
        .hasId(messageID)
        .hasAnnotation(ANNOTATION_KEY, ANNOTATION_VALUE)
        .hasAnnotation(ANNOTATION_KEY_ARRAY, ANNOTATION_VALUE_ARRAY)
        .hasAnnotation(ANNOTATION_KEY_LIST, ANNOTATION_VALUE_LIST)
        .hasAnnotation(ANNOTATION_KEY_MAP, ANNOTATION_VALUE_MAP);
    waitAtMost(() -> management.queueInfo(q).messageCount() == 0);
    waitAtMost(() -> management.queueInfo(dlq).messageCount() == 0);
  }

  @Test
  void batchAcceptShouldSettleMessages() {
    declareDeadLetterTopology();
    Management management = connection.management();
    int messageCount = 1000;
    TestUtils.Sync confirmSync = sync(messageCount);
    Publisher.Callback callback =
        context -> {
          if (context.status() == Publisher.Status.ACCEPTED) {
            confirmSync.down();
          }
        };
    Publisher publisher = connection.publisherBuilder().queue(q).build();
    IntStream.range(0, messageCount)
        .forEach(ignored -> publisher.publish(publisher.message(), callback));
    Assertions.assertThat(confirmSync).completes();
    publisher.close();

    int batchSize = messageCount / 100;
    AtomicReference<com.rabbitmq.client.amqp.Consumer.BatchContext> batchContext =
        new AtomicReference<>();
    Random random = new Random();
    TestUtils.Sync receivedSync = sync(messageCount);
    AtomicInteger discardedCount = new AtomicInteger();
    connection
        .consumerBuilder()
        .queue(q)
        .messageHandler(
            (ctx, msg) -> {
              if (batchContext.get() == null) {
                batchContext.set(ctx.batch(batchSize));
              }
              if (random.nextInt(10) == 0) {
                ctx.discard();
                discardedCount.incrementAndGet();
              } else {
                batchContext.get().add(ctx);
                if (batchContext.get().size() == batchSize) {
                  batchContext.get().accept();
                  batchContext.set(null);
                }
              }
              receivedSync.down();
            })
        .build();

    Assertions.assertThat(receivedSync).completes();
    Consumer.BatchContext bctx = batchContext.get();
    if (bctx != null && bctx.size() != 0 && bctx.size() < batchSize) {
      bctx.accept();
    }
    waitAtMost(() -> management.queueInfo(q).messageCount() == 0);
    waitAtMost(() -> management.queueInfo(dlq).messageCount() == discardedCount.get());
  }

  private void declareDeadLetterTopology() {
    this.management.exchange(dlx).type(FANOUT).autoDelete(true).declare();
    this.management.queue(dlq).exclusive(true).declare();
    this.management.binding().sourceExchange(dlx).destinationQueue(dlq).bind();
    this.management.queue().name(q).type(QUORUM).deadLetterExchange(dlx).declare();
  }
}
