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
import static com.rabbitmq.client.amqp.Publisher.Status.ACCEPTED;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static java.time.Duration.ofMillis;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.AmqpException.AmqpResourceClosedException;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestUtils.DisabledIfRabbitMqCtlNotSet
@AmqpTestInfrastructure
public class TopologyRecoveryTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyRecoveryTest.class);

  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = BackOffDelayPolicy.fixed(ofMillis(100));
  static Environment environment;
  TestInfo testInfo;
  String connectionName;
  Sync recoveredSync;
  AtomicInteger connectionAttemptCount;

  @BeforeEach
  void init(TestInfo info) {
    this.testInfo = info;
    this.recoveredSync = TestUtils.sync();
    this.connectionName = connectionName();
    this.connectionAttemptCount = new AtomicInteger();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void topologyCallbacksShouldBeCalledWhenUpdatingTopology(boolean isolateResources) {
    Set<String> events = ConcurrentHashMap.newKeySet();
    AtomicInteger eventCount = new AtomicInteger();
    TopologyListener topologyListener =
        new TopologyListener() {
          @Override
          public void exchangeDeclared(AmqpExchangeSpecification specification) {
            events.add("exchangeDeclared");
            eventCount.incrementAndGet();
          }

          @Override
          public void exchangeDeleted(String name) {
            events.add("exchangeDeleted");
            eventCount.incrementAndGet();
          }

          @Override
          public void queueDeclared(AmqpQueueSpecification specification) {
            events.add("queueDeclared");
            eventCount.incrementAndGet();
          }

          @Override
          public void queueDeleted(String name) {
            events.add("queueDeleted");
            eventCount.incrementAndGet();
          }

          @Override
          public void bindingDeclared(
              AmqpBindingManagement.AmqpBindingSpecification specification) {
            events.add("bindingDeclared");
            eventCount.incrementAndGet();
          }

          @Override
          public void bindingDeleted(AmqpBindingManagement.AmqpUnbindSpecification specification) {
            events.add("bindingDeleted");
            eventCount.incrementAndGet();
          }

          @Override
          public void consumerCreated(long id, String queue) {
            events.add("consumerCreated");
            eventCount.incrementAndGet();
          }

          @Override
          public void consumerDeleted(long id, String queue) {
            events.add("consumerDeleted");
            eventCount.incrementAndGet();
          }
        };
    try (AmqpConnection connection =
        new AmqpConnection(
            new AmqpConnectionBuilder((AmqpEnvironment) environment)
                .isolateResources(isolateResources)
                .topologyListener(topologyListener))) {
      Management management = connection.management();
      String e = TestUtils.name(testInfo);
      management.exchange().name(e).declare();
      assertThat(events).contains("exchangeDeclared");
      assertThat(eventCount).hasValue(1);

      String q = TestUtils.name(testInfo);
      management.queue().name(q).declare();
      assertThat(events).contains("queueDeclared");
      assertThat(eventCount).hasValue(2);

      management.binding().sourceExchange(e).key("foo").destinationQueue(q).bind();
      assertThat(events).contains("bindingDeclared");
      assertThat(eventCount).hasValue(3);

      Consumer consumer =
          connection.consumerBuilder().queue(q).messageHandler((context, message) -> {}).build();

      assertThat(events).contains("consumerCreated");
      assertThat(eventCount).hasValue(4);

      consumer.close();

      assertThat(events).contains("consumerDeleted");
      assertThat(eventCount).hasValue(5);

      management.unbind().sourceExchange(e).key("foo").destinationQueue(q).unbind();
      assertThat(events).contains("bindingDeleted");
      assertThat(eventCount).hasValue(6);

      management.exchangeDelete(e);
      assertThat(events).contains("exchangeDeleted");
      assertThat(eventCount).hasValue(7);

      management.queueDelete(q);
      assertThat(events).contains("queueDeleted");
      assertThat(eventCount).hasValue(8);
    }
  }

  @Test
  void queueShouldNotBeRecoveredWhenNoTopologyRecovery() {
    try (Connection connection =
        connection(
            this.connectionName, false, this.recoveredSync, b -> b.recovery().topology(false))) {
      assertThat(connectionAttemptCount).hasValue(1);
      String q = queue();
      connection.management().queue(q).autoDelete(false).exclusive(true).declare();
      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);
      connection.management().queue(q).autoDelete(true).exclusive(true).declare();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void resourceListenersShouldBeCalled(boolean isolateResources) {
    List<String> events = new CopyOnWriteArrayList<>();
    try (Connection connection =
        connection(
            this.connectionName,
            isolateResources,
            this.recoveredSync,
            b ->
                b.listeners(
                    context -> events.add("connection " + context.currentState().name())))) {
      assertThat(connectionAttemptCount).hasValue(1);
      String e = exchange();
      String q = queue();
      connection.management().exchange(e).type(FANOUT).autoDelete(true).declare();
      connection.management().queue(q).autoDelete(true).exclusive(true).declare();
      // to delete the exchange automatically
      connection.management().binding().sourceExchange(e).destinationQueue(q).bind();

      connection
          .publisherBuilder()
          .exchange(e)
          .key("foo")
          .listeners(ctx -> events.add("publisher " + ctx.currentState().name()))
          .build();
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler((context, message) -> {})
          .listeners(ctx -> events.add("consumer " + ctx.currentState().name()))
          .build();

      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      String[] expectedStates =
          new String[] {
            "connection OPENING",
            "connection OPEN",
            "publisher OPENING",
            "publisher OPEN",
            "consumer OPENING",
            "consumer OPEN",
            "connection RECOVERING",
            "publisher RECOVERING",
            "consumer RECOVERING",
            "consumer OPEN",
            "publisher OPEN",
            "connection OPEN"
          };

      waitAtMost(() -> events.size() == expectedStates.length);
      assertThat(events).containsExactly(expectedStates);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void autoDeleteExchangeAndExclusiveQueueShouldBeRedeclared(boolean isolateResources) {
    try (Connection connection = connection(isolateResources)) {
      assertThat(connectionAttemptCount).hasValue(1);
      String e = exchange();
      String q = queue();
      connection.management().exchange(e).type(DIRECT).autoDelete(true).declare();
      connection.management().queue(q).autoDelete(true).exclusive(true).declare();
      connection.management().binding().sourceExchange(e).key("foo").destinationQueue(q).bind();

      Sync consumeSync = TestUtils.sync();
      Publisher publisher = connection.publisherBuilder().exchange(e).key("foo").build();
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (context, message) -> {
                context.accept();
                consumeSync.down();
              })
          .build();

      publisher.publish(publisher.message(), context -> {});

      assertThat(consumeSync).completes();

      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      consumeSync.reset();

      publisher.publish(publisher.message(), context -> {});
      assertThat(consumeSync).completes();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void autoDeleteExchangeAndExclusiveQueueWithE2eBindingShouldBeRedeclared(
      boolean isolateResources) {
    try (Connection connection = connection(isolateResources)) {
      assertThat(connectionAttemptCount).hasValue(1);
      String e1 = exchange();
      String e2 = exchange();
      String q = queue();
      connection.management().exchange(e1).type(FANOUT).autoDelete(true).declare();
      connection.management().exchange(e2).type(FANOUT).autoDelete(true).declare();
      connection.management().queue(q).autoDelete(true).exclusive(true).declare();
      connection.management().binding().sourceExchange(e1).destinationExchange(e2).bind();
      connection.management().binding().sourceExchange(e2).destinationQueue(q).bind();

      Sync consumeSync = TestUtils.sync();
      Publisher publisher = connection.publisherBuilder().exchange(e1).build();
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (context, message) -> {
                context.accept();
                consumeSync.down();
              })
          .build();

      publisher.publish(publisher.message(), context -> {});

      assertThat(consumeSync).completes();

      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      consumeSync.reset();

      publisher.publish(publisher.message(), context -> {});
      assertThat(consumeSync).completes();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void deletedQueueBindingIsNotRecovered(boolean isolateResources) {
    String e = exchange();
    String q = queue();
    Connection connection = connection(isolateResources);
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      connection.management().exchange(e).type(FANOUT).declare();
      connection.management().queue(q).declare();
      connection.management().binding().sourceExchange(e).destinationQueue(q).bind();

      Sync consumeSync = TestUtils.sync();
      Publisher publisher = connection.publisherBuilder().exchange(e).build();
      Consumer consumer =
          connection
              .consumerBuilder()
              .queue(q)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeSync.down();
                  })
              .build();

      publisher.publish(publisher.message(), context -> {});

      assertThat(consumeSync).completes();

      connection.management().unbind().sourceExchange(e).destinationQueue(q).unbind();

      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      consumer.close();

      CountDownLatch acceptedLatch = new CountDownLatch(1);
      publisher.publish(
          publisher.message(),
          context -> {
            if (context.status() == Publisher.Status.RELEASED) {
              acceptedLatch.countDown();
            }
          });
      assertThat(acceptedLatch).completes();
      assertThat(connection.management().queueInfo(q)).isEmpty();
    } finally {
      connection.management().queueDelete(q);
      connection.management().exchangeDelete(e);
      connection.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void deletedExchangeBindingIsNotRecovered(boolean isolateResources) {
    String e1 = exchange();
    String e2 = exchange();
    String q = queue();
    Connection connection = connection(isolateResources);
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      connection.management().exchange(e1).type(FANOUT).declare();
      connection.management().exchange(e2).type(FANOUT).declare();
      connection.management().queue(q).declare();
      connection.management().binding().sourceExchange(e1).destinationExchange(e2).bind();
      connection.management().binding().sourceExchange(e2).destinationQueue(q).bind();

      Sync consumeSync = TestUtils.sync();
      Publisher publisher = connection.publisherBuilder().exchange(e1).build();
      Consumer consumer =
          connection
              .consumerBuilder()
              .queue(q)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeSync.down();
                  })
              .build();

      publisher.publish(publisher.message(), context -> {});

      assertThat(consumeSync).completes();

      connection.management().unbind().sourceExchange(e1).destinationExchange(e2).unbind();

      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      consumer.close();

      CountDownLatch acceptedLatch = new CountDownLatch(1);
      publisher.publish(
          publisher.message(),
          context -> {
            if (context.status() == Publisher.Status.RELEASED) {
              acceptedLatch.countDown();
            }
          });
      assertThat(acceptedLatch).completes();
      assertThat(connection.management().queueInfo(q)).isEmpty();
    } finally {
      connection.management().queueDelete(q);
      connection.management().exchangeDelete(e2);
      connection.management().exchangeDelete(e1);
      connection.close();
    }
  }

  @Test
  void deletedExchangeIsNotRecovered() {
    String e = exchange();
    try (Connection connection = connection()) {
      assertThat(connectionAttemptCount).hasValue(1);
      connection.management().exchange(e).declare();
      Assertions.assertThat(Cli.exchangeExists(e)).isTrue();
      connection.management().exchangeDelete(e);
      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);
      Assertions.assertThat(Cli.exchangeExists(e)).isFalse();
    }
  }

  @Test
  void deletedQueueIsNotRecovered() {
    String q = queue();
    try (Connection connection = connection()) {
      assertThat(connectionAttemptCount).hasValue(1);
      connection.management().queue(q).declare();
      assertThat(connection.management().queueInfo(q)).hasName(q);
      connection.management().queueDelete(q);
      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);
      assertThatThrownBy(() -> connection.management().queueInfo(q))
          .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void closedConsumerIsNotRecovered(boolean isolateResources) {
    String q = queue();
    Connection connection = connection(isolateResources);
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      connection.management().queue(q).declare();
      Consumer consumer =
          connection.consumerBuilder().queue(q).messageHandler((ctx, m) -> {}).build();
      waitAtMost(() -> connection.management().queueInfo(q).consumerCount() == 1);
      consumer.close();
      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);
      waitAtMost(() -> connection.management().queueInfo(q).consumerCount() == 0);
    } finally {
      connection.management().queueDelete(q);
      connection.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void recoverConsumers(boolean isolateResources) {
    String e = exchange();
    String q = queue();
    Connection connection = connection(isolateResources);
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      connection.management().exchange(e).type(FANOUT).declare();
      connection.management().queue(q).declare();
      connection.management().binding().sourceExchange(e).destinationQueue(q).bind();
      int consumerCount = 60;
      Sync consumeSync = TestUtils.sync(consumerCount);
      Consumer.MessageHandler handler =
          (ctx, m) -> {
            consumeSync.down();
            ctx.accept();
          };
      range(0, consumerCount)
          .forEach(
              ignored -> connection.consumerBuilder().queue(q).messageHandler(handler).build());
      Publisher publisher = connection.publisherBuilder().exchange(e).build();
      range(0, consumerCount).forEach(ignored -> publisher.publish(publisher.message(), ctx -> {}));
      assertThat(consumeSync).completes();
      assertThat(connection.management().queueInfo(q)).hasConsumerCount(consumerCount);
      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      consumeSync.reset(consumerCount);
      range(0, consumerCount).forEach(ignored -> publisher.publish(publisher.message(), ctx -> {}));
      assertThat(consumeSync).completes();
      assertThat(connection.management().queueInfo(q)).isEmpty().hasConsumerCount(consumerCount);
    } finally {
      connection.management().queueDelete(q);
      connection.management().exchangeDelete(e);
      connection.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void recoverPublisherConsumerSeveralTimes(boolean isolateResources) {
    String e = exchange();
    String q = queue();
    Connection connection = connection(isolateResources);
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      connection.management().exchange(e).type(FANOUT).declare();
      connection.management().queue(q).declare();
      connection.management().binding().sourceExchange(e).destinationQueue(q).bind();

      Publisher publisher = connection.publisherBuilder().exchange(e).build();
      Sync consumeSync = TestUtils.sync();
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, m) -> {
                consumeSync.down();
                ctx.accept();
              })
          .build();

      IntStream.range(0, 10)
          .forEach(
              i -> {
                closeConnectionAndWaitForRecovery();
                assertThat(connectionAttemptCount).hasValue(2 + i);
                consumeSync.reset();
                publisher.publish(publisher.message(), ctx -> {});
                assertThat(consumeSync).completes();
                assertThat(connection.management().queueInfo(q)).isEmpty().hasConsumerCount(1);
              });

    } finally {
      connection.management().queueDelete(q);
      connection.management().exchangeDelete(e);
      connection.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void disposeStaleMessageShouldBeSilent(boolean isolateResources) throws Exception {
    String q = queue();
    Connection connection = connection(isolateResources);
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      connection.management().queue(q).declare();
      Publisher publisher = connection.publisherBuilder().queue(q).build();
      BlockingQueue<Consumer.Context> messageContexts = new ArrayBlockingQueue<>(10);
      CountDownLatch consumeLatch = new CountDownLatch(3);
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, m) -> {
                messageContexts.add(ctx);
                consumeLatch.countDown();
              })
          .build();

      publisher.publish(publisher.message(), ctx -> {});
      publisher.publish(publisher.message(), ctx -> {});
      publisher.publish(publisher.message(), ctx -> {});
      assertThat(consumeLatch).completes();
      assertThat(messageContexts).hasSize(3);

      closeConnectionAndWaitForRecovery();

      // the messages are settled after the connection recovery
      // their receiver instance is closed
      // we make sure no exceptions are thrown
      // this simulates long processing that spans over connection recovery
      Consumer.Context ctx = messageContexts.poll(10, TimeUnit.SECONDS);
      ctx.accept();
      ctx = messageContexts.poll(10, TimeUnit.SECONDS);
      ctx.discard();
      ctx = messageContexts.poll(10, TimeUnit.SECONDS);
      ctx.requeue();

      // the messages are requeued automatically, so they should come back
      messageContexts.poll(10, TimeUnit.SECONDS).accept();
      messageContexts.poll(10, TimeUnit.SECONDS).accept();
      messageContexts.poll(10, TimeUnit.SECONDS).accept();

      waitAtMost(() -> connection.management().queueInfo(q).messageCount() == 0);
    } finally {
      connection.management().queueDelete(q);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void autoDeleteClientNamedQueueShouldBeRecovered(boolean isolateResources) {
    try (Connection connection = connection(isolateResources)) {
      Management.QueueInfo queueInfo = connection.management().queue().exclusive(true).declare();

      String queueName = queueInfo.name();

      Sync consumeSync = TestUtils.sync();
      Publisher publisher = connection.publisherBuilder().queue(queueName).build();
      connection
          .consumerBuilder()
          .queue(queueName)
          .messageHandler(
              (ctx, message) -> {
                ctx.accept();
                consumeSync.down();
              })
          .build();

      publisher.publish(publisher.message(), ctx -> {});
      assertThat(consumeSync).completes();

      closeConnectionAndWaitForRecovery();

      publisher.publish(publisher.message(), ctx -> {});
      assertThat(consumeSync).completes();
      waitAtMost(() -> connection.management().queueInfo(queueName).messageCount() == 0);
    }
  }

  @Test
  void shouldRecoverEvenIfManagementIsClosed(TestInfo info) {
    try (Connection connection = connection()) {
      Management management = connection.management();
      Management.QueueInfo queueInfo = management.queue().exclusive(true).declare();
      String q = queueInfo.name();

      Sync consumeSync = TestUtils.sync();
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, message) -> {
                consumeSync.down();
                ctx.accept();
              })
          .build();

      waitAtMost(() -> management.queueInfo(q).consumerCount() == 1);

      management.close();

      closeConnectionAndWaitForRecovery();

      Publisher publisher = connection.publisherBuilder().queue(q).build();
      Sync publishSync = TestUtils.sync();
      Publisher.Callback callback =
          ctx -> {
            if (ctx.status() == ACCEPTED) {
              publishSync.down();
            } else {
              LOGGER.warn(
                  "Unexpected status: {} ({})", ctx.status(), info.getTestMethod().get().getName());
            }
          };
      publisher.publish(publisher.message(), callback);
      assertThat(publishSync).completes();

      assertThat(consumeSync).completes();

      assertThatThrownBy(() -> management.queueInfo(q))
          .isInstanceOf(AmqpResourceClosedException.class);
      assertThat(connection.management().queueInfo(q)).isEmpty();
    }
  }

  @Test
  void managementShouldStayClosedAfterRecoveryIfClosedBefore() {
    try (Connection connection = connection()) {
      AmqpManagement management = (AmqpManagement) connection.management();

      String q = management.queue().exclusive(true).declare().name();

      management.close();

      closeConnectionAndWaitForRecovery();
      assertThat(management.isClosed()).isTrue();
      assertThatThrownBy(() -> management.queueInfo(q))
          .isInstanceOf(AmqpResourceClosedException.class);
      assertThat(connection.management().queueInfo(q)).isEmpty();
    }
  }

  String exchange() {
    return "e-" + TestUtils.name(this.testInfo);
  }

  String queue() {
    return "q-" + TestUtils.name(this.testInfo);
  }

  String connectionName() {
    return "c-" + TestUtils.name(this.testInfo);
  }

  Connection connection(boolean isolateResources) {
    return this.connection(this.connectionName, isolateResources, this.recoveredSync, b -> {});
  }

  Connection connection() {
    return this.connection(this.connectionName, false, this.recoveredSync, b -> {});
  }

  Connection connection(
      String name,
      boolean isolateResources,
      Sync recoveredSync,
      java.util.function.Consumer<AmqpConnectionBuilder> builderCallback) {
    AmqpConnectionBuilder builder = (AmqpConnectionBuilder) environment.connectionBuilder();
    builder
        .name(name)
        .isolateResources(isolateResources)
        .addressSelector(
            addresses -> {
              connectionAttemptCount.incrementAndGet();
              return addresses.get(0);
            })
        .listeners(
            context -> {
              if (context.previousState() == Resource.State.RECOVERING
                  && context.currentState() == Resource.State.OPEN) {
                recoveredSync.down();
              }
            })
        .recovery()
        .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
        .connectionBuilder();
    builderCallback.accept(builder);
    return builder.build();
  }

  void closeConnectionAndWaitForRecovery() {
    Cli.closeConnection(this.connectionName);
    assertThat(this.recoveredSync).completes();
    this.recoveredSync.reset();
  }
}
