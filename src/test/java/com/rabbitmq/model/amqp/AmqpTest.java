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

import static com.rabbitmq.model.Management.ExchangeType.DIRECT;
import static com.rabbitmq.model.Management.ExchangeType.FANOUT;
import static com.rabbitmq.model.Management.QueueType.CLASSIC;
import static com.rabbitmq.model.Management.QueueType.QUORUM;
import static com.rabbitmq.model.amqp.TestUtils.CountDownLatchConditions.completed;
import static com.rabbitmq.model.amqp.TestUtils.assertThat;
import static com.rabbitmq.model.amqp.TestUtils.environmentBuilder;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.IntStream.range;

import com.rabbitmq.model.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
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
                    publisher
                        .message()
                        .addData("hello".getBytes(StandardCharsets.UTF_8))
                        .messageId(messageId),
                    context -> {
                      if (context.status() == Publisher.Status.ACCEPTED) {
                        confirmLatch.countDown();
                      }
                    });
              });

      assertThat(confirmLatch).is(completed());

      Management.QueueInfo queueInfo = connection.management().queueInfo(q);
      assertThat(queueInfo).hasName(q).hasNoConsumers().hasMessageCount(messageCount);

      CountDownLatch consumeLatch = new CountDownLatch(messageCount);
      com.rabbitmq.model.Consumer consumer =
          connection
              .consumerBuilder()
              .queue(q)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeLatch.countDown();
                  })
              .build();
      assertThat(consumeLatch).is(completed());

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
                  publisher.message().addData("hello".getBytes(StandardCharsets.UTF_8)),
                  context -> {
                    if (context.status() == Publisher.Status.ACCEPTED) {
                      confirmLatch.countDown();
                    }
                  });

      range(0, messageCount).forEach(ignored -> publish.accept(publisher1));
      range(0, messageCount).forEach(ignored -> publish.accept(publisher2));

      assertThat(confirmLatch).is(completed());

      CountDownLatch consumeLatch = new CountDownLatch(messageCount * 2);
      com.rabbitmq.model.Consumer consumer =
          connection
              .consumerBuilder()
              .queue(q)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeLatch.countDown();
                  })
              .build();
      assertThat(consumeLatch).is(completed());
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

  //  @Test
  void test(TestInfo info) {
    String q = TestUtils.name(info);
    try (Connection c = environment.connectionBuilder().build()) {
      c.management().queue(q).type(CLASSIC).declare();
      c.management().queue(q).type(CLASSIC).declare();
      c.management().queue(q).type(QUORUM).declare();
    }
  }
}
