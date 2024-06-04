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
package com.rabbitmq.amqp.client.impl;

import static com.rabbitmq.amqp.client.Management.ExchangeType.DIRECT;
import static com.rabbitmq.amqp.client.Management.ExchangeType.FANOUT;
import static com.rabbitmq.amqp.client.Management.QueueType.QUORUM;
import static com.rabbitmq.amqp.client.impl.TestUtils.CountDownLatchConditions.completed;
import static com.rabbitmq.amqp.client.impl.TestUtils.assertThat;
import static com.rabbitmq.amqp.client.impl.TestUtils.environmentBuilder;
import static com.rabbitmq.amqp.client.impl.TestUtils.waitAtMost;
import static java.nio.charset.StandardCharsets.*;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.amqp.client.Connection;
import com.rabbitmq.amqp.client.Environment;
import com.rabbitmq.amqp.client.Management;
import com.rabbitmq.amqp.client.Publisher;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AmqpTest {

  static Environment environment;
  Connection connection;

  @BeforeAll
  static void initAll() {
    environment = environmentBuilder().build();
  }

  @BeforeEach
  void init() {
    this.connection = environment.connectionBuilder().build();
  }

  @AfterEach
  void tearDown() {
    this.connection.close();
  }

  @AfterAll
  static void tearDownAll() {
    environment.close();
  }

  @Test
  void queueInfoTest(TestInfo info) {
    String q = TestUtils.name(info);
    Management management = connection.management();
    try {
      management.queue(q).quorum().queue().declare();

      Management.QueueInfo queueInfo = management.queueInfo(q);
      assertThat(queueInfo)
          .hasName(q)
          .is(QUORUM)
          .isDurable()
          .isNotAutoDelete()
          .isNotExclusive()
          .isEmpty()
          .hasNoConsumers()
          .hasArgument("x-queue-type", "quorum");

    } finally {
      management.queueDeletion().delete(q);
    }
  }

  @Test
  void queueDeclareDeletePublishConsume(TestInfo info) {
    String q = TestUtils.name(info);
    try {
      connection.management().queue().name(q).quorum().queue().declare();
      Publisher publisher = connection.publisherBuilder().queue(q).build();

      int messageCount = 100;
      CountDownLatch confirmLatch = new CountDownLatch(messageCount);
      range(0, messageCount)
          .forEach(
              ignored -> {
                UUID messageId = UUID.randomUUID();
                publisher.publish(
                    publisher.message("hello".getBytes(UTF_8)).messageId(messageId),
                    context -> {
                      if (context.status() == Publisher.Status.ACCEPTED) {
                        confirmLatch.countDown();
                      }
                    });
              });

      Assertions.assertThat(confirmLatch).is(completed());

      Management.QueueInfo queueInfo = connection.management().queueInfo(q);
      assertThat(queueInfo).hasName(q).hasNoConsumers().hasMessageCount(messageCount);

      CountDownLatch consumeLatch = new CountDownLatch(messageCount);
      com.rabbitmq.amqp.client.Consumer consumer =
          connection
              .consumerBuilder()
              .queue(q)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeLatch.countDown();
                  })
              .build();
      Assertions.assertThat(consumeLatch).is(completed());

      queueInfo = connection.management().queueInfo(q);
      assertThat(queueInfo).hasConsumerCount(1).isEmpty();

      consumer.close();
      publisher.close();
    } finally {
      connection.management().queueDeletion().delete(q);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void binding(boolean addBindingArguments, TestInfo info) {
    String e1 = TestUtils.name(info);
    String e2 = TestUtils.name(info);
    String q = TestUtils.name(info);
    String rk = "foo";
    Map<String, Object> bindingArguments =
        addBindingArguments ? singletonMap("foo", "bar") : emptyMap();
    Management management = connection.management();
    try {
      management.exchange().name(e1).type(DIRECT).declare();
      management.exchange().name(e2).type(FANOUT).declare();
      management.queue().name(q).type(QUORUM).declare();
      management
          .binding()
          .sourceExchange(e1)
          .destinationExchange(e2)
          .key(rk)
          .arguments(bindingArguments)
          .bind();
      management
          .binding()
          .sourceExchange(e2)
          .destinationQueue(q)
          .arguments(bindingArguments)
          .bind();

      Publisher publisher1 = connection.publisherBuilder().exchange(e1).key(rk).build();
      Publisher publisher2 = connection.publisherBuilder().exchange(e2).build();

      int messageCount = 1;
      CountDownLatch confirmLatch = new CountDownLatch(messageCount * 2);

      Consumer<Publisher> publish =
          publisher ->
              publisher.publish(
                  publisher.message("hello".getBytes(UTF_8)),
                  context -> {
                    if (context.status() == Publisher.Status.ACCEPTED) {
                      confirmLatch.countDown();
                    }
                  });

      range(0, messageCount).forEach(ignored -> publish.accept(publisher1));
      range(0, messageCount).forEach(ignored -> publish.accept(publisher2));

      Assertions.assertThat(confirmLatch).is(completed());

      CountDownLatch consumeLatch = new CountDownLatch(messageCount * 2);
      com.rabbitmq.amqp.client.Consumer consumer =
          connection
              .consumerBuilder()
              .queue(q)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeLatch.countDown();
                  })
              .build();
      Assertions.assertThat(consumeLatch).is(completed());
      publisher1.close();
      publisher2.close();
      consumer.close();
    } finally {
      management
          .unbind()
          .sourceExchange(e2)
          .destinationQueue(q)
          .arguments(bindingArguments)
          .unbind();
      management
          .unbind()
          .sourceExchange(e1)
          .destinationExchange(e2)
          .key(rk)
          .arguments(bindingArguments)
          .unbind();
      management.exchangeDeletion().delete(e2);
      management.exchangeDeletion().delete(e1);
      management.queueDeletion().delete(q);
    }
  }

  @Test
  void sameTypeMessagesInQueue() {
    String q = connection.management().queue().exclusive(true).declare().name();
    Publisher publisher = connection.publisherBuilder().queue(q).build();

    Set<byte[]> messageBodies = ConcurrentHashMap.newKeySet(2);
    CountDownLatch consumeLatch = new CountDownLatch(2);
    connection
        .consumerBuilder()
        .queue(q)
        .messageHandler(
            (ctx, message) -> {
              ctx.accept();
              messageBodies.add(message.body());
              consumeLatch.countDown();
            })
        .build();

    publisher.publish(publisher.message("one".getBytes(UTF_8)), ctx -> {});
    publisher.publish(publisher.message("two".getBytes(UTF_8)), ctx -> {});

    assertThat(consumeLatch).completes();
    assertThat(messageBodies).hasSize(2).containsOnly("one".getBytes(UTF_8), "two".getBytes(UTF_8));
  }

  @Test
  void pauseShouldStopMessageArrivalUnpauseShouldResumeIt() throws Exception {
    String q = connection.management().queue().exclusive(true).declare().name();
    Publisher publisher = connection.publisherBuilder().queue(q).build();
    int messageCount = 100;
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Publisher.Callback callback = ctx -> publishLatch.countDown();
    IntStream.range(0, messageCount)
        .forEach(ignored -> publisher.publish(publisher.message(), callback));

    assertThat(publishLatch).completes();

    int initialCredits = 10;
    Set<com.rabbitmq.amqp.client.Consumer.Context> messageContexts = ConcurrentHashMap.newKeySet();
    com.rabbitmq.amqp.client.Consumer consumer =
        connection
            .consumerBuilder()
            .queue(q)
            .initialCredits(initialCredits)
            .messageHandler(
                (ctx, msg) -> {
                  messageContexts.add(ctx);
                })
            .build();

    waitAtMost(() -> messageContexts.size() == initialCredits);

    assertThat(connection.management().queueInfo(q)).hasMessageCount(messageCount - initialCredits);

    assertThat(Cli.queueInfo(q).unackedMessageCount()).isEqualTo(initialCredits);

    ((AmqpConsumer) consumer).pause();
    new ArrayList<>(messageContexts).forEach(com.rabbitmq.amqp.client.Consumer.Context::accept);

    waitAtMost(() -> Cli.queueInfo(q).unackedMessageCount() == 0);
    waitAtMost(() -> messageContexts.size() == initialCredits);
    ((AmqpConsumer) consumer).unpause();
    waitAtMost(() -> messageContexts.size() == initialCredits * 2);
  }
}
