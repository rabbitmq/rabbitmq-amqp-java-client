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
import static com.rabbitmq.client.amqp.Management.QueueType.*;
import static com.rabbitmq.client.amqp.Management.QueueType.STREAM;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersion.RABBITMQ_4_0_3;
import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static java.nio.charset.StandardCharsets.*;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.IntStream.range;
import static java.util.stream.Stream.of;
import static org.assertj.core.api.Assertions.*;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersionAtLeast;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfAddressV1Permitted;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@AmqpTestInfrastructure
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
      management.queueDelete(name);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"foobar", "фообар"})
  void queueDeclareDeletePublishConsume(String subject) {
    try {
      connection.management().queue().name(name).quorum().queue().declare();
      Publisher publisher = connection.publisherBuilder().queue(name).build();

      int messageCount = 100;
      Sync confirmSync = sync(messageCount);
      range(0, messageCount)
          .forEach(
              ignored -> {
                UUID messageId = UUID.randomUUID();
                publisher.publish(
                    publisher
                        .message("hello".getBytes(UTF_8))
                        .messageId(messageId)
                        .subject(subject),
                    acceptedCallback(confirmSync));
              });

      assertThat(confirmSync).completes();

      Management.QueueInfo queueInfo = connection.management().queueInfo(name);
      assertThat(queueInfo).hasName(name).hasNoConsumers().hasMessageCount(messageCount);

      AtomicReference<String> receivedSubject = new AtomicReference<>();
      Sync consumeSync = TestUtils.sync(messageCount);
      com.rabbitmq.client.amqp.Consumer consumer =
          connection
              .consumerBuilder()
              .queue(name)
              .messageHandler(
                  (context, message) -> {
                    receivedSubject.set(message.subject());
                    context.accept();
                    consumeSync.down();
                  })
              .build();

      assertThat(consumeSync).completes();
      org.assertj.core.api.Assertions.assertThat(receivedSubject)
          .doesNotHaveNullValue()
          .hasValue(subject);

      queueInfo = connection.management().queueInfo(name);
      assertThat(queueInfo).hasConsumerCount(1).isEmpty();

      consumer.close();
      publisher.close();
    } finally {
      connection.management().queueDelete(name);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "foo,false",
    "foo,true",
    "фообар,true",
    "фообар,false",
    "фоо!бар,false",
    "фоо!бар,true",
  })
  void binding(String prefix, boolean addBindingArguments, TestInfo info) {
    String e1 = prefix + "-" + TestUtils.name(info);
    String e2 = prefix + "-" + TestUtils.name(info);
    String q = prefix + "-" + TestUtils.name(info);
    String rk = prefix + "-" + "foo";
    Map<String, Object> bindingArguments =
        addBindingArguments ? singletonMap("foo", prefix + "-bar") : emptyMap();
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
      Sync confirmSync = sync(messageCount * 2);

      Consumer<Publisher> publish =
          publisher ->
              publisher.publish(
                  publisher.message("hello".getBytes(UTF_8)), acceptedCallback(confirmSync));

      range(0, messageCount).forEach(ignored -> publish.accept(publisher1));
      range(0, messageCount).forEach(ignored -> publish.accept(publisher2));

      assertThat(confirmSync).completes();

      Sync consumeSync = sync(messageCount * 2);
      com.rabbitmq.client.amqp.Consumer consumer =
          connection
              .consumerBuilder()
              .queue(q)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeSync.down();
                  })
              .build();
      assertThat(consumeSync).completes();
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
      management.exchangeDelete(e2);
      management.exchangeDelete(e1);
      management.queueDelete(q);
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
    org.assertj.core.api.Assertions.assertThat(messageBodies)
        .hasSize(2)
        .containsOnly("one".getBytes(UTF_8), "two".getBytes(UTF_8));
  }

  @Test
  void pauseShouldStopMessageArrivalUnpauseShouldResumeIt() {
    String q = connection.management().queue().exclusive(true).declare().name();
    Publisher publisher = connection.publisherBuilder().queue(q).build();
    int messageCount = 100;
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Publisher.Callback callback = ctx -> publishLatch.countDown();
    range(0, messageCount).forEach(ignored -> publisher.publish(publisher.message(), callback));

    assertThat(publishLatch).completes();

    int initialCredits = 10;
    Set<com.rabbitmq.client.amqp.Consumer.Context> messageContexts = ConcurrentHashMap.newKeySet();
    com.rabbitmq.client.amqp.Consumer consumer =
        connection
            .consumerBuilder()
            .queue(q)
            .initialCredits(initialCredits)
            .messageHandler((ctx, msg) -> messageContexts.add(ctx))
            .build();

    waitAtMost(() -> messageContexts.size() == initialCredits);

    assertThat(connection.management().queueInfo(q)).hasMessageCount(messageCount - initialCredits);

    org.assertj.core.api.Assertions.assertThat(Cli.queueInfo(q).unackedMessageCount())
        .isEqualTo(initialCredits);

    consumer.pause();
    new ArrayList<>(messageContexts).forEach(com.rabbitmq.client.amqp.Consumer.Context::accept);

    waitAtMost(() -> Cli.queueInfo(q).unackedMessageCount() == 0);
    waitAtMost(() -> messageContexts.size() == initialCredits);
    consumer.unpause();
    waitAtMost(() -> messageContexts.size() == initialCredits * 2);
    consumer.pause();
    messageContexts.forEach(com.rabbitmq.client.amqp.Consumer.Context::accept);
  }

  @Test
  void publisherShouldThrowWhenExchangeDoesNotExist() {
    String doesNotExist = uuid();
    assertThatThrownBy(() -> connection.publisherBuilder().exchange(doesNotExist).build())
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class)
        .hasMessageContaining(doesNotExist);
  }

  @Test
  void publisherSendingShouldThrowWhenExchangeHasBeenDeleted() {
    connection.management().exchange(name).type(FANOUT).declare();
    Sync closedSync = sync();
    AtomicReference<Throwable> closedException = new AtomicReference<>();
    Publisher publisher =
        connection
            .publisherBuilder()
            .exchange(name)
            .listeners(closedListener(closedSync, ctx -> closedException.set(ctx.failureCause())))
            .build();
    try {
      String q = connection.management().queue().exclusive(true).declare().name();
      connection.management().binding().sourceExchange(name).destinationQueue(q).bind();
      Sync sync = sync();
      publisher.publish(publisher.message(), acceptedCallback(sync));
      assertThat(sync).completes();
    } finally {
      connection.management().exchangeDelete(name);
    }
    AtomicReference<Exception> exception = new AtomicReference<>();
    waitAtMost(
        () -> {
          try {
            publisher.publish(publisher.message(), ctx -> {});
            return false;
          } catch (AmqpException.AmqpEntityDoesNotExistException e) {
            exception.set(e);
            return true;
          }
        });
    assertThat(closedSync).completes();
    of(exception.get(), closedException.get())
        .forEach(
            e ->
                org.assertj.core.api.Assertions.assertThat(e)
                    .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class)
                    .hasMessageContaining(name)
                    .hasMessageContaining(ExceptionUtils.ERROR_NOT_FOUND));
  }

  @Test
  @DisabledIfAddressV1Permitted
  void publisherShouldThrowWhenQueueDoesNotExist() {
    String doesNotExist = uuid();
    assertThatThrownBy(() -> connection.publisherBuilder().queue(doesNotExist).build())
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class)
        .hasMessageContaining(doesNotExist);
  }

  @Test
  void publisherSendingShouldThrowWhenQueueHasBeenDeleted() {
    connection.management().queue(name).declare();
    Sync closedSync = sync();
    AtomicReference<Throwable> closedException = new AtomicReference<>();
    Publisher publisher =
        connection
            .publisherBuilder()
            .queue(name)
            .listeners(closedListener(closedSync, ctx -> closedException.set(ctx.failureCause())))
            .build();
    try {
      Sync sync = sync();
      publisher.publish(publisher.message(), acceptedCallback(sync));
      assertThat(sync).completes();
    } finally {
      connection.management().queueDelete(name);
    }
    AtomicReference<Exception> exception = new AtomicReference<>();
    waitAtMost(
        () -> {
          try {
            publisher.publish(publisher.message(), ctx -> {});
            return false;
          } catch (AmqpException.AmqpEntityDoesNotExistException e) {
            exception.set(e);
            return true;
          }
        });
    assertThat(closedSync).completes();
    of(exception.get(), closedException.get())
        .forEach(
            e ->
                org.assertj.core.api.Assertions.assertThat(e)
                    .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class)
                    .hasMessageContaining(ExceptionUtils.ERROR_RESOURCE_DELETED));
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_0_3)
  void publisherSendingShouldThrowWhenPublishingToNonExistingExchangeWithToProperty() {
    String doesNotExist = uuid();
    connection.management().queue(name).exclusive(true).declare();
    Publisher publisher = connection.publisherBuilder().build();
    Sync consumedSync = sync();
    connection
        .consumerBuilder()
        .queue(name)
        .messageHandler(
            (ctx, msg) -> {
              ctx.accept();
              consumedSync.down();
            })
        .build();
    Sync acceptedSync = sync();
    publisher.publish(
        publisher.message().toAddress().queue(name).message(), ctx -> acceptedSync.down());
    assertThat(acceptedSync).completes();
    assertThat(consumedSync).completes();

    acceptedSync.reset();
    consumedSync.reset();

    Sync rejectedSync = sync();
    publisher.publish(
        publisher.message().toAddress().exchange(doesNotExist).message(),
        ctx -> rejectedSync.down());
    assertThat(rejectedSync).completes();

    assertThat(consumedSync).hasNotCompleted();
    publisher.publish(
        publisher.message().toAddress().queue(name).message(), ctx -> acceptedSync.down());
    assertThat(acceptedSync).completes();
    assertThat(consumedSync).completes();
  }

  @Test
  @DisabledIfAddressV1Permitted
  void consumerShouldThrowWhenQueueDoesNotExist() {
    String doesNotExist = uuid();
    assertThatThrownBy(
            () ->
                connection
                    .consumerBuilder()
                    .queue(doesNotExist)
                    .messageHandler((ctx, msg) -> {})
                    .build())
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class)
        .hasMessageContaining(doesNotExist);
  }

  @Test
  void consumerShouldGetClosedWhenQueueIsDeleted() {
    connection.management().queue(name).exclusive(true).declare();
    Sync consumeSync = sync();
    Sync closedSync = sync();
    AtomicReference<Throwable> exception = new AtomicReference<>();
    connection
        .consumerBuilder()
        .queue(name)
        .messageHandler(
            (ctx, msg) -> {
              ctx.accept();
              consumeSync.down();
            })
        .listeners(
            context -> {
              if (context.currentState() == Resource.State.CLOSED) {
                exception.set(context.failureCause());
                closedSync.down();
              }
            })
        .build();
    Publisher publisher = connection.publisherBuilder().queue(name).build();
    publisher.publish(publisher.message(), ctx -> {});
    assertThat(consumeSync).completes();
    connection.management().queueDelete(name);
    assertThat(closedSync).completes();
    org.assertj.core.api.Assertions.assertThat(exception.get())
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class)
        .hasMessageContaining(ExceptionUtils.ERROR_RESOURCE_DELETED);
  }

  @Test
  void consumerPauseThenClose() {
    connection.management().queue(name).exclusive(true).declare();
    int messageCount = 100;
    int initialCredits = messageCount / 10;
    Publisher publisher = connection.publisherBuilder().queue(name).build();
    Sync publishSync = sync(messageCount);
    range(0, messageCount)
        .forEach(ignored -> publisher.publish(publisher.message(), ctx -> publishSync.down()));
    assertThat(publishSync).completes();

    AtomicInteger receivedCount = new AtomicInteger(0);
    Set<com.rabbitmq.client.amqp.Consumer.Context> unsettledMessages =
        ConcurrentHashMap.newKeySet();
    com.rabbitmq.client.amqp.Consumer consumer =
        connection
            .consumerBuilder()
            .queue(name)
            .initialCredits(messageCount / 10)
            .messageHandler(
                (ctx, msg) -> {
                  if (receivedCount.incrementAndGet() <= initialCredits) {
                    ctx.accept();
                  } else {
                    unsettledMessages.add(ctx);
                  }
                })
            .build();

    int unsettledMessageCount = waitUntilStable(unsettledMessages::size);
    org.assertj.core.api.Assertions.assertThat(unsettledMessageCount).isNotZero();
    consumer.pause();
    int receivedCountAfterPausing = receivedCount.get();
    unsettledMessages.forEach(com.rabbitmq.client.amqp.Consumer.Context::accept);
    consumer.close();
    org.assertj.core.api.Assertions.assertThat(receivedCount).hasValue(receivedCountAfterPausing);
    assertThat(connection.management().queueInfo(name))
        .hasMessageCount(messageCount - receivedCount.get());
  }

  @Test
  void consumerGracefulShutdownExample() {
    connection.management().queue(name).exclusive(true).declare();
    int messageCount = 1000;
    int initialCredits = messageCount / 10;
    Publisher publisher = connection.publisherBuilder().queue(name).build();
    Sync publishSync = sync(messageCount);
    range(0, messageCount)
        .forEach(ignored -> publisher.publish(publisher.message(), ctx -> publishSync.down()));
    assertThat(publishSync).completes();

    AtomicInteger receivedCount = new AtomicInteger(0);
    Random random = new Random();
    com.rabbitmq.client.amqp.Consumer consumer =
        connection
            .consumerBuilder()
            .queue(name)
            .initialCredits(initialCredits)
            .messageHandler(
                (ctx, msg) -> {
                  receivedCount.incrementAndGet();
                  int processTime = random.nextInt(10) + 1;
                  TestUtils.simulateActivity(processTime);
                  ctx.accept();
                })
            .build();

    waitAtMost(() -> receivedCount.get() > initialCredits * 2);
    consumer.pause();
    waitAtMost(() -> consumer.unsettledMessageCount() == 0);
    consumer.close();
    assertThat(connection.management().queueInfo(name))
        .hasMessageCount(messageCount - receivedCount.get());
  }

  @Test
  void consumerUnsettledMessagesGoBackToQueueAfterClosing() {
    connection.management().queue(name).exclusive(true).declare();
    int messageCount = 100;
    int initialCredits = messageCount / 10;
    int settledCount = initialCredits * 2;
    Publisher publisher = connection.publisherBuilder().queue(name).build();
    Sync publishSync = sync(messageCount);
    range(0, messageCount)
        .forEach(ignored -> publisher.publish(publisher.message(), ctx -> publishSync.down()));
    assertThat(publishSync).completes();

    AtomicInteger receivedCount = new AtomicInteger(0);
    com.rabbitmq.client.amqp.Consumer consumer =
        connection
            .consumerBuilder()
            .queue(name)
            .initialCredits(initialCredits)
            .messageHandler(
                (ctx, msg) -> {
                  receivedCount.incrementAndGet();
                  if (receivedCount.get() <= settledCount) {
                    ctx.accept();
                  }
                })
            .build();

    waitAtMost(() -> receivedCount.get() > settledCount);
    consumer.close();
    assertThat(connection.management().queueInfo(name))
        .hasMessageCount(messageCount - settledCount);
  }

  @Test
  void consumerWithHigherPriorityShouldGetMessagesFirst() {
    int messageCount = 100;
    connection.management().queue(name).exclusive(true).declare();
    AtomicInteger lowCount = new AtomicInteger(0);
    AtomicInteger highCount = new AtomicInteger(0);
    Sync consumeSync = sync(messageCount);
    com.rabbitmq.client.amqp.Consumer lowPriorityConsumer =
        connection
            .consumerBuilder()
            .queue(name)
            .priority(1)
            .messageHandler(
                (ctx, msg) -> {
                  ctx.accept();
                  lowCount.incrementAndGet();
                  consumeSync.down();
                })
            .build();

    com.rabbitmq.client.amqp.Consumer highPriorityConsumer =
        connection
            .consumerBuilder()
            .queue(name)
            .priority(5)
            .messageHandler(
                (ctx, msg) -> {
                  ctx.accept();
                  highCount.incrementAndGet();
                  consumeSync.down();
                })
            .build();

    Publisher publisher = connection.publisherBuilder().queue(name).build();
    Runnable publish =
        () ->
            IntStream.range(0, messageCount)
                .forEach(ignored -> publisher.publish(publisher.message(), ctx -> {}));

    publish.run();

    assertThat(consumeSync).completes();
    org.assertj.core.api.Assertions.assertThat(lowCount).hasValue(0);
    org.assertj.core.api.Assertions.assertThat(highCount).hasValue(messageCount);

    highPriorityConsumer.close();

    consumeSync.reset(messageCount);
    publish.run();
    assertThat(consumeSync).completes();
    org.assertj.core.api.Assertions.assertThat(lowCount).hasValue(messageCount);
    org.assertj.core.api.Assertions.assertThat(highCount).hasValue(messageCount);

    lowPriorityConsumer.close();
  }

  @Test
  void redeclareQueueWithDifferentArguments() {
    Management management = connection.management();
    management.queue(name).type(CLASSIC).declare();
    try {
      management.queue(name).type(QUORUM).declare();
      fail("Declaring an existing queue with different arguments should trigger an exception");
    } catch (AmqpException e) {
      // OK
    } finally {
      management.queueDelete(name);
    }
  }

  @Test
  void redeclareExchangesWithDifferentArguments() {
    Management management = connection.management();
    management.exchange(name).type(DIRECT).declare();
    try {
      management.exchange(name).type(FANOUT).declare();
      fail("Declaring an existing exchange with different arguments should trigger an exception");
    } catch (AmqpException e) {
      org.assertj.core.api.Assertions.assertThat(e).hasMessageContaining("409");
      // OK
    } finally {
      management.exchangeDelete(name);
    }
  }

  @Test
  void declareQueueWithUnsupportedArgument() {
    Management management = connection.management();
    List<Consumer<Management>> operations =
        List.of(
            m -> m.queue(name).type(CLASSIC).argument("x-max-age", "1000s").declare(),
            m -> m.queue(name).type(QUORUM).argument("x-max-age", "1000s").declare(),
            m -> m.queue(name).type(STREAM).deadLetterRoutingKey("not-supported").declare());
    operations.forEach(
        operation -> {
          try {
            operation.accept(management);
            fail("Creating a queue with unsupported arguments should trigger an exception");
          } catch (AmqpException e) {
            org.assertj.core.api.Assertions.assertThat(e).hasMessageContaining("409");
          }
        });
  }

  @Test
  void publishedMessageShouldBeRejectedWhenQueueLimitIsReached(TestInfo info) {
    Management management = connection.management();
    String q = TestUtils.name(info);
    int maxLength = 10;
    try {
      management
          .queue(q)
          .maxLength(maxLength)
          .overflowStrategy(Management.OverflowStrategy.REJECT_PUBLISH)
          .declare();
      CountDownLatch rejectedLatch = new CountDownLatch(1);
      Publisher.Callback callback =
          context -> {
            if (context.status() == Publisher.Status.REJECTED) {
              rejectedLatch.countDown();
            }
          };
      Publisher publisher = connection.publisherBuilder().queue(q).build();
      IntStream.range(0, maxLength + 1)
          .forEach(ignored -> publisher.publish(publisher.message(), callback));
      assertThat(rejectedLatch).completes();
    } finally {
      management.queueDelete(q);
    }
  }

  @Test
  void queuePurgeShouldRemoveAllMessages(TestInfo info) {
    Management management = connection.management();
    String q = TestUtils.name(info);
    management.queue(q).exclusive(true).declare();
    int messageCount = 100;
    Sync publishSync = sync(messageCount);
    Publisher.Callback callback =
        context -> {
          if (context.status() == Publisher.Status.ACCEPTED) {
            publishSync.down();
          }
        };
    Publisher publisher = connection.publisherBuilder().queue(q).build();
    IntStream.range(0, messageCount)
        .forEach(ignored -> publisher.publish(publisher.message(), callback));
    assertThat(publishSync).completes();
    assertThat(management.queueInfo(q)).hasMessageCount(messageCount);
    management.queuePurge(q);
    assertThat(management.queueInfo(q)).isEmpty();
  }

  @Test
  void queuePurgeOnNonExistingQueueShouldThrowException(TestInfo info) {
    String q = TestUtils.name(info);
    assertThatThrownBy(() -> connection.management().queuePurge(q))
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class)
        .hasMessageContaining(q);
  }

  @Test
  void types(TestInfo info) {
    Management management = connection.management();
    String q = TestUtils.name(info);
    management.queue(q).exclusive(true).declare();
    Sync publishSync = sync();
    Publisher.Callback callback =
        context -> {
          if (context.status() == Publisher.Status.ACCEPTED) {
            publishSync.down();
          }
        };
    Publisher publisher = connection.publisherBuilder().queue(q).build();
    publisher.publish(publisher.message().property("key1", -1L), callback);
    assertThat(publishSync).completes();
    AtomicReference<Message> message = new AtomicReference<>();
    Sync consumeSync = sync();
    connection
        .consumerBuilder()
        .queue(q)
        .messageHandler(
            (ctx, msg) -> {
              message.set(msg);
              ctx.accept();
              consumeSync.down();
            })
        .build();
    assertThat(consumeSync).completes();
    assertThat(message.get()).hasProperty("key1", -1L);
  }

  private static String uuid() {
    return UUID.randomUUID().toString();
  }

  private static Resource.StateListener closedListener(
      Sync sync, Consumer<Resource.Context> callback) {
    return context -> {
      if (context.currentState() == Resource.State.CLOSED) {
        callback.accept(context);
        sync.down();
      }
    };
  }

  private static Publisher.Callback acceptedCallback(Sync sync) {
    return ctx -> {
      if (ctx.status() == Publisher.Status.ACCEPTED) {
        sync.down();
      }
    };
  }
}
