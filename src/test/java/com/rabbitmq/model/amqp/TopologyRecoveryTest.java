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
import static com.rabbitmq.model.amqp.Cli.closeConnection;
import static com.rabbitmq.model.amqp.TestUtils.CountDownLatchReferenceConditions.completed;
import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.model.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;

@TestUtils.DisabledIfRabbitMqCtlNotSet
public class TopologyRecoveryTest {

  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = fixed(ofMillis(100));
  static Environment environment;
  TestInfo testInfo;
  CountDownLatch recoveredLatch;

  @BeforeAll
  static void initAll() {
    environment = TestUtils.environmentBuilder().build();
  }

  @BeforeEach
  void init(TestInfo info) {
    this.testInfo = info;
    this.recoveredLatch = new CountDownLatch(1);
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
          connection.consumerBuilder().address(q).messageHandler((context, message) -> {}).build();

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
  void autoDeleteExchangeAndExclusiveQueueShouldBeRedeclared() {
    String connectionName = connectionName();
    try (Connection connection = connection(connectionName)) {
      String e = exchange();
      String q = queue();
      connection.management().exchange().name(e).type(DIRECT).autoDelete(true).declare();
      connection.management().queue().name(q).autoDelete(true).exclusive(true).declare();
      connection.management().binding().sourceExchange(e).key("foo").destinationQueue(q).bind();

      AtomicReference<CountDownLatch> consumeLatch = new AtomicReference<>(new CountDownLatch(1));
      Publisher publisher =
          connection.publisherBuilder().address("/exchange/" + e + "/foo").build();
      connection
          .consumerBuilder()
          .address(q)
          .messageHandler((context, message) -> consumeLatch.get().countDown())
          .build();

      publisher.publish(publisher.message(), context -> {});

      assertThat(consumeLatch).is(completed());

      closeConnection(connectionName);

      assertThat(recoveredLatch).is(TestUtils.CountDownLatchConditions.completed());
      consumeLatch.set(new CountDownLatch(1));

      publisher.publish(publisher.message(), context -> {});
      assertThat(consumeLatch).is(completed());
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

  Connection connection(String name) {
    return this.connection(name, this.recoveredLatch);
  }

  Connection connection(String name, CountDownLatch recoveredLatch) {
    AmqpConnectionBuilder builder = (AmqpConnectionBuilder) environment.connectionBuilder();
    return builder
        .name(name)
        .listeners(
            context -> {
              if (context.previousState() == Resource.State.RECOVERING
                  && context.currentState() == Resource.State.OPEN) {
                recoveredLatch.countDown();
              }
            })
        .recovery()
        .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
        .connectionBuilder()
        .build();
  }
}
