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
package com.rabbitmq.model.amqp;

import static com.rabbitmq.model.BackOffDelayPolicy.fixed;
import static com.rabbitmq.model.Management.ExchangeType.DIRECT;
import static com.rabbitmq.model.Management.ExchangeType.FANOUT;
import static com.rabbitmq.model.amqp.Cli.closeConnection;
import static com.rabbitmq.model.amqp.Cli.exchangeExists;
import static com.rabbitmq.model.amqp.TestUtils.assertThat;
import static com.rabbitmq.model.amqp.TestUtils.waitAtMost;
import static java.time.Duration.ofMillis;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.model.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.*;

@TestUtils.DisabledIfRabbitMqCtlNotSet
public class TopologyRecoveryTest {

  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = fixed(ofMillis(100));
  static Environment environment;
  TestInfo testInfo;
  String connectionName;
  AtomicReference<CountDownLatch> recoveredLatch;
  AtomicInteger connectionAttemptCount;

  @BeforeAll
  static void initAll() {
    environment = TestUtils.environmentBuilder().build();
  }

  @BeforeEach
  void init(TestInfo info) {
    this.testInfo = info;
    this.recoveredLatch = new AtomicReference<>(new CountDownLatch(1));
    this.connectionName = connectionName();
    this.connectionAttemptCount = new AtomicInteger();
  }

  @AfterAll
  static void tearDownAll() {
    environment.close();
  }

  @Test
  void topologyCallbacksShouldBeCalledWhenUpdatingTopology(TestInfo info) {
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
          public void consumerCreated(long id, String address) {
            events.add("consumerCreated");
            eventCount.incrementAndGet();
          }

          @Override
          public void consumerDeleted(long id, String address) {
            events.add("consumerDeleted");
            eventCount.incrementAndGet();
          }
        };
    try (AmqpConnection connection =
        new AmqpConnection(
            new AmqpConnectionBuilder((AmqpEnvironment) environment)
                .topologyListener(topologyListener))) {
      Management management = connection.management();
      String e = TestUtils.name(info);
      management.exchange().name(e).declare();
      assertThat(events).contains("exchangeDeclared");
      assertThat(eventCount).hasValue(1);

      String q = TestUtils.name(info);
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

      management.exchangeDeletion().delete(e);
      assertThat(events).contains("exchangeDeleted");
      assertThat(eventCount).hasValue(7);

      management.queueDeletion().delete(q);
      assertThat(events).contains("queueDeleted");
      assertThat(eventCount).hasValue(8);
    }
  }

  @Test
  void queueShouldNotBeRecoveredWhenNoTopologyRecovery() {
    try (Connection connection =
        connection(this.connectionName, this.recoveredLatch, b -> b.recovery().topology(false))) {
      assertThat(connectionAttemptCount).hasValue(1);
      String q = queue();
      connection.management().queue(q).autoDelete(false).exclusive(true).declare();
      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);
      connection.management().queue(q).autoDelete(true).exclusive(true).declare();
    }
  }

  @Test
  void resourceListenersShouldBeCalled() throws Exception {
    List<String> events = new CopyOnWriteArrayList<>();
    try (Connection connection =
        connection(
            this.connectionName,
            this.recoveredLatch,
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

  @Test
  void autoDeleteExchangeAndExclusiveQueueShouldBeRedeclared() {
    try (Connection connection = connection()) {
      assertThat(connectionAttemptCount).hasValue(1);
      String e = exchange();
      String q = queue();
      connection.management().exchange(e).type(DIRECT).autoDelete(true).declare();
      connection.management().queue(q).autoDelete(true).exclusive(true).declare();
      connection.management().binding().sourceExchange(e).key("foo").destinationQueue(q).bind();

      AtomicReference<CountDownLatch> consumeLatch = new AtomicReference<>(new CountDownLatch(1));
      Publisher publisher = connection.publisherBuilder().exchange(e).key("foo").build();
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler((context, message) -> consumeLatch.get().countDown())
          .build();

      publisher.publish(publisher.message(), context -> {});

      assertThat(consumeLatch).completes();

      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      consumeLatch.set(new CountDownLatch(1));

      publisher.publish(publisher.message(), context -> {});
      assertThat(consumeLatch).completes();
    }
  }

  @Test
  void autoDeleteExchangeAndExclusiveQueueWithE2eBindingShouldBeRedeclared() {
    try (Connection connection = connection()) {
      assertThat(connectionAttemptCount).hasValue(1);
      String e1 = exchange();
      String e2 = exchange();
      String q = queue();
      connection.management().exchange(e1).type(FANOUT).autoDelete(true).declare();
      connection.management().exchange(e2).type(FANOUT).autoDelete(true).declare();
      connection.management().queue(q).autoDelete(true).exclusive(true).declare();
      connection.management().binding().sourceExchange(e1).destinationExchange(e2).bind();
      connection.management().binding().sourceExchange(e2).destinationQueue(q).bind();

      AtomicReference<CountDownLatch> consumeLatch = new AtomicReference<>(new CountDownLatch(1));
      Publisher publisher = connection.publisherBuilder().exchange(e1).build();
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler((context, message) -> consumeLatch.get().countDown())
          .build();

      publisher.publish(publisher.message(), context -> {});

      assertThat(consumeLatch).completes();

      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      consumeLatch.set(new CountDownLatch(1));

      publisher.publish(publisher.message(), context -> {});
      assertThat(consumeLatch).completes();
    }
  }

  @Test
  void deletedQueueBindingIsNotRecovered() {
    String e = exchange();
    String q = queue();
    Connection connection = connection();
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      connection.management().exchange(e).type(FANOUT).declare();
      connection.management().queue(q).declare();
      connection.management().binding().sourceExchange(e).destinationQueue(q).bind();

      AtomicReference<CountDownLatch> consumeLatch = new AtomicReference<>(new CountDownLatch(1));
      Publisher publisher = connection.publisherBuilder().exchange(e).build();
      Consumer consumer =
          connection
              .consumerBuilder()
              .queue(q)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeLatch.get().countDown();
                  })
              .build();

      publisher.publish(publisher.message(), context -> {});

      assertThat(consumeLatch).completes();

      connection.management().unbind().sourceExchange(e).destinationQueue(q).unbind();

      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      consumer.close();

      CountDownLatch acceptedLatch = new CountDownLatch(1);
      publisher.publish(
          publisher.message(),
          context -> {
            if (context.status() == Publisher.Status.FAILED) {
              acceptedLatch.countDown();
            }
          });
      assertThat(acceptedLatch).completes();
      assertThat(connection.management().queueInfo(q)).isEmpty();
    } finally {
      connection.management().queueDeletion().delete(q);
      connection.management().exchangeDeletion().delete(e);
      connection.close();
    }
  }

  @Test
  void deletedEchangeBindingIsNotRecovered() {
    String e1 = exchange();
    String e2 = exchange();
    String q = queue();
    Connection connection = connection();
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      connection.management().exchange(e1).type(FANOUT).declare();
      connection.management().exchange(e2).type(FANOUT).declare();
      connection.management().queue(q).declare();
      connection.management().binding().sourceExchange(e1).destinationExchange(e2).bind();
      connection.management().binding().sourceExchange(e2).destinationQueue(q).bind();

      AtomicReference<CountDownLatch> consumeLatch = new AtomicReference<>(new CountDownLatch(1));
      Publisher publisher = connection.publisherBuilder().exchange(e1).build();
      Consumer consumer =
          connection
              .consumerBuilder()
              .queue(q)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeLatch.get().countDown();
                  })
              .build();

      publisher.publish(publisher.message(), context -> {});

      assertThat(consumeLatch).completes();

      connection.management().unbind().sourceExchange(e1).destinationExchange(e2).unbind();

      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      consumer.close();

      CountDownLatch acceptedLatch = new CountDownLatch(1);
      publisher.publish(
          publisher.message(),
          context -> {
            if (context.status() == Publisher.Status.FAILED) {
              acceptedLatch.countDown();
            }
          });
      assertThat(acceptedLatch).completes();
      assertThat(connection.management().queueInfo(q)).isEmpty();
    } finally {
      connection.management().queueDeletion().delete(q);
      connection.management().exchangeDeletion().delete(e2);
      connection.management().exchangeDeletion().delete(e1);
      connection.close();
    }
  }

  @Test
  void deletedExchangeIsNotRecovered() {
    String e = exchange();
    try (Connection connection = connection()) {
      assertThat(connectionAttemptCount).hasValue(1);
      connection.management().exchange(e).declare();
      assertThat(exchangeExists(e)).isTrue();
      connection.management().exchangeDeletion().delete(e);
      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);
      assertThat(exchangeExists(e)).isFalse();
    }
  }

  @Test
  void deletedQueueIsNotRecovered() {
    String q = queue();
    try (Connection connection = connection()) {
      assertThat(connectionAttemptCount).hasValue(1);
      connection.management().queue(q).declare();
      assertThat(connection.management().queueInfo(q)).hasName(q);
      connection.management().queueDeletion().delete(q);
      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);
      assertThatThrownBy(() -> connection.management().queueInfo(q))
          .isInstanceOf(ModelException.class)
          .hasMessageContaining("404");
    }
  }

  @Test
  void closedConsumerIsNotRecovered() throws Exception {
    String q = queue();
    Connection connection = connection();
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
      connection.management().queueDeletion().delete(q);
      connection.close();
    }
  }

  @Test
  void recoverConsumers() {
    String e = exchange();
    String q = queue();
    Connection connection = connection();
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      connection.management().exchange(e).type(FANOUT).declare();
      connection.management().queue(q).declare();
      connection.management().binding().sourceExchange(e).destinationQueue(q).bind();
      int consumerCount = 100;
      AtomicReference<CountDownLatch> consumeLatch =
          new AtomicReference<>(new CountDownLatch(consumerCount));
      Consumer.MessageHandler handler =
          (ctx, m) -> {
            consumeLatch.get().countDown();
            ctx.accept();
          };
      range(0, consumerCount)
          .forEach(
              ignored -> connection.consumerBuilder().queue(q).messageHandler(handler).build());
      Publisher publisher = connection.publisherBuilder().exchange(e).build();
      range(0, consumerCount).forEach(ignored -> publisher.publish(publisher.message(), ctx -> {}));
      assertThat(consumeLatch).completes();
      assertThat(connection.management().queueInfo(q)).hasConsumerCount(consumerCount);
      closeConnectionAndWaitForRecovery();
      assertThat(connectionAttemptCount).hasValue(2);

      consumeLatch.set(new CountDownLatch(consumerCount));
      range(0, consumerCount).forEach(ignored -> publisher.publish(publisher.message(), ctx -> {}));
      assertThat(consumeLatch).completes();
      assertThat(connection.management().queueInfo(q)).isEmpty().hasConsumerCount(consumerCount);
    } finally {
      connection.management().queueDeletion().delete(q);
      connection.management().exchangeDeletion().delete(e);
      connection.close();
    }
  }

  @Test
  void recoverPublisherConsumerSeveralTimes() {
    String e = exchange();
    String q = queue();
    Connection connection = connection();
    assertThat(connectionAttemptCount).hasValue(1);
    try {
      connection.management().exchange(e).type(FANOUT).declare();
      connection.management().queue(q).declare();
      connection.management().binding().sourceExchange(e).destinationQueue(q).bind();

      Publisher publisher = connection.publisherBuilder().exchange(e).build();
      AtomicReference<CountDownLatch> consumeLatch = new AtomicReference<>();
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, m) -> {
                consumeLatch.get().countDown();
                ctx.accept();
              })
          .build();

      IntStream.range(0, 10)
          .forEach(
              i -> {
                closeConnectionAndWaitForRecovery();
                assertThat(connectionAttemptCount).hasValue(2 + i);
                consumeLatch.set(new CountDownLatch(1));
                publisher.publish(publisher.message(), ctx -> {});
                assertThat(consumeLatch).completes();
                assertThat(connection.management().queueInfo(q)).isEmpty().hasConsumerCount(1);
              });

    } finally {
      connection.management().queueDeletion().delete(q);
      connection.management().exchangeDeletion().delete(e);
      connection.close();
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

  Connection connection() {
    return this.connection(this.connectionName, this.recoveredLatch, b -> {});
  }

  Connection connection(
      String name,
      AtomicReference<CountDownLatch> recoveredLatch,
      java.util.function.Consumer<AmqpConnectionBuilder> builderCallback) {
    AmqpConnectionBuilder builder = (AmqpConnectionBuilder) environment.connectionBuilder();
    builder
        .name(name)
        .addressSelector(
            addresses -> {
              connectionAttemptCount.incrementAndGet();
              return addresses.get(0);
            })
        .listeners(
            context -> {
              if (context.previousState() == Resource.State.RECOVERING
                  && context.currentState() == Resource.State.OPEN) {
                recoveredLatch.get().countDown();
              }
            })
        .recovery()
        .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
        .connectionBuilder();
    builderCallback.accept(builder);
    return builder.build();
  }

  void closeConnectionAndWaitForRecovery() {
    closeConnection(this.connectionName);
    assertThat(this.recoveredLatch).completes();
    this.recoveredLatch.set(new CountDownLatch(1));
  }
}
