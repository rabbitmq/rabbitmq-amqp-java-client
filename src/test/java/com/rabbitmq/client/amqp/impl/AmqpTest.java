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

import static com.rabbitmq.client.amqp.Management.ExchangeType.DIRECT;
import static com.rabbitmq.client.amqp.Management.ExchangeType.FANOUT;
import static com.rabbitmq.client.amqp.Management.QueueType.QUORUM;
import static com.rabbitmq.client.amqp.impl.TestUtils.CountDownLatchConditions.completed;
import static com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import static com.rabbitmq.client.amqp.impl.TestUtils.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.sync;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static java.nio.charset.StandardCharsets.*;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfAddressV1Permitted;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(AmqpTestInfrastructureExtension.class)
public class AmqpTest {

  Connection connection;
  String name;

  @BeforeEach
  void init(TestInfo info) {
    this.name = TestUtils.name(info);
  }

  @Test
  void queueInfoTest() {
    Management management = connection.management();
    try {
      management.queue(name).quorum().queue().declare();

      Management.QueueInfo queueInfo = management.queueInfo(name);
      assertThat(queueInfo)
          .hasName(name)
          .is(QUORUM)
          .isDurable()
          .isNotAutoDelete()
          .isNotExclusive()
          .isEmpty()
          .hasNoConsumers()
          .hasArgument("x-queue-type", "quorum");

    } finally {
      management.queueDeletion().delete(name);
    }
  }

  @Test
  void queueDeclareDeletePublishConsume() {
    try {
      connection.management().queue().name(name).quorum().queue().declare();
      Publisher publisher = connection.publisherBuilder().queue(name).build();

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

      Management.QueueInfo queueInfo = connection.management().queueInfo(name);
      assertThat(queueInfo).hasName(name).hasNoConsumers().hasMessageCount(messageCount);

      CountDownLatch consumeLatch = new CountDownLatch(messageCount);
      com.rabbitmq.client.amqp.Consumer consumer =
          connection
              .consumerBuilder()
              .queue(name)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeLatch.countDown();
                  })
              .build();
      Assertions.assertThat(consumeLatch).is(completed());

      queueInfo = connection.management().queueInfo(name);
      assertThat(queueInfo).hasConsumerCount(1).isEmpty();

      consumer.close();
      publisher.close();
    } finally {
      connection.management().queueDeletion().delete(name);
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
      com.rabbitmq.client.amqp.Consumer consumer =
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
    Set<com.rabbitmq.client.amqp.Consumer.Context> messageContexts = ConcurrentHashMap.newKeySet();
    com.rabbitmq.client.amqp.Consumer consumer =
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
    new ArrayList<>(messageContexts).forEach(com.rabbitmq.client.amqp.Consumer.Context::accept);

    waitAtMost(() -> Cli.queueInfo(q).unackedMessageCount() == 0);
    waitAtMost(() -> messageContexts.size() == initialCredits);
    ((AmqpConsumer) consumer).unpause();
    waitAtMost(() -> messageContexts.size() == initialCredits * 2);
  }

  @Test
  void publishingToNonExistingExchangeShouldThrow() {
    String doesNotExist = uuid();
    assertThatThrownBy(() -> connection.publisherBuilder().exchange(doesNotExist).build())
        .isInstanceOf(AmqpException.AmqpEntityNotFoundException.class)
        .hasMessageContaining(doesNotExist);
  }

  @Test
  void deletingExchangeShouldTriggerExceptionOnSender() throws Exception {
    connection.management().exchange(name).type(FANOUT).declare();
    Publisher publisher = connection.publisherBuilder().exchange(name).build();
    try {
      String q = connection.management().queue().exclusive(true).declare().name();
      connection.management().binding().sourceExchange(name).destinationQueue(q).bind();
      Sync sync = sync();
      publisher.publish(
          publisher.message(),
          ctx -> {
            if (ctx.status() == Publisher.Status.ACCEPTED) {
              sync.down();
            }
          });
      assertThat(sync).completes();
    } finally {
      connection.management().exchangeDeletion().delete(name);
    }
    AtomicReference<Exception> exception = new AtomicReference<>();
    waitAtMost(
        () -> {
          try {
            publisher.publish(publisher.message(), ctx -> {});
            return false;
          } catch (AmqpException.AmqpEntityNotFoundException e) {
            exception.set(e);
            return true;
          }
        });
    assertThat(exception.get())
        .isInstanceOf(AmqpException.AmqpEntityNotFoundException.class)
        .hasMessageContaining(name)
        .hasMessageContaining(ExceptionUtils.ERROR_NOT_FOUND);
  }

  @Test
  @DisabledIfAddressV1Permitted
  void publishingToNonExistingQueueShouldThrow() {
    String doesNotExist = uuid();
    assertThatThrownBy(() -> connection.publisherBuilder().queue(doesNotExist).build())
        .isInstanceOf(AmqpException.AmqpEntityNotFoundException.class)
        .hasMessageContaining(doesNotExist);
  }

  @Test
  void deletingQueueShouldTriggerExceptionOnSender() throws Exception {
    connection.management().queue(name).declare();
    Publisher publisher = connection.publisherBuilder().queue(name).build();
    try {
      Sync sync = sync();
      publisher.publish(
          publisher.message(),
          ctx -> {
            if (ctx.status() == Publisher.Status.ACCEPTED) {
              sync.down();
            }
          });
      assertThat(sync).completes();
    } finally {
      connection.management().queueDeletion().delete(name);
    }
    AtomicReference<Exception> exception = new AtomicReference<>();
    waitAtMost(
        () -> {
          try {
            publisher.publish(publisher.message(), ctx -> {});
            return false;
          } catch (AmqpException.AmqpEntityNotFoundException e) {
            exception.set(e);
            return true;
          }
        });
    assertThat(exception.get())
        .isInstanceOf(AmqpException.AmqpEntityNotFoundException.class)
        .hasMessageContaining(ExceptionUtils.ERROR_RESOURCE_DELETED);
  }

  @Test
  void publishingToNonExistingExchangeWithToPropertyShouldThrow() throws Exception {
    String doesNotExist = uuid();
    Publisher publisher = connection.publisherBuilder().build();
    AtomicReference<Exception> exception = new AtomicReference<>();
    waitAtMost(
        () -> {
          try {
            publisher.publish(
                publisher.message().toAddress().exchange(doesNotExist).message(), ctx -> {});
            return false;
          } catch (AmqpException.AmqpEntityNotFoundException e) {
            exception.set(e);
            return true;
          }
        });
    assertThat(exception.get())
        .isInstanceOf(AmqpException.AmqpEntityNotFoundException.class)
        .hasMessageContaining(doesNotExist);
  }

  @Test
  @DisabledIfAddressV1Permitted
  void consumingFromNonExistingQueueShouldThrow() {
    String doesNotExist = uuid();
    assertThatThrownBy(
            () ->
                connection
                    .consumerBuilder()
                    .queue(doesNotExist)
                    .messageHandler((ctx, msg) -> {})
                    .build())
        .isInstanceOf(AmqpException.AmqpEntityNotFoundException.class)
        .hasMessageContaining(doesNotExist);
  }

  private static String uuid() {
    return UUID.randomUUID().toString();
  }
}
