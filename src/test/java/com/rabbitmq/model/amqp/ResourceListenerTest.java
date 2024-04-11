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

import static com.rabbitmq.model.Publisher.Status.FAILED;
import static com.rabbitmq.model.amqp.TestUtils.assertThat;
import static com.rabbitmq.model.amqp.TestUtils.environmentBuilder;
import static com.rabbitmq.model.amqp.TestUtils.name;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.fail;

import com.rabbitmq.model.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ResourceListenerTest {

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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void publisherIsClosedOnExchangeDeletion(boolean toExchange, TestInfo info)
      throws InterruptedException {
    String entity = name(info);
    Runnable declare, delete;
    Consumer<PublisherBuilder> builderConfigurator;
    if (toExchange) {
      declare = () -> connection.management().exchange(entity).declare();
      delete = () -> connection.management().exchangeDeletion().delete(entity);
      builderConfigurator = b -> b.exchange(entity);
    } else {
      declare = () -> connection.management().queue(entity).declare();
      delete = () -> connection.management().queueDeletion().delete(entity);
      builderConfigurator = b -> b.queue(entity);
    }
    CountDownLatch closedLatch = new CountDownLatch(1);
    AtomicReference<Throwable> closedCause = new AtomicReference<>();
    Publisher publisher;
    try {
      declare.run();

      PublisherBuilder builder = connection.publisherBuilder();
      builder.listeners(
          context -> {
            if (context.currentState() == Resource.State.CLOSED) {
              closedCause.set(context.failureCause());
              closedLatch.countDown();
            }
          });

      builderConfigurator.accept(builder);
      publisher = builder.build();
      CountDownLatch acceptedLatch = new CountDownLatch(1);
      publisher.publish(publisher.message(), ctx -> acceptedLatch.countDown());
      assertThat(acceptedLatch).completes();
    } finally {
      delete.run();
    }

    Set<Publisher.Status> outboundMessageStatus = ConcurrentHashMap.newKeySet();
    try {
      int count = 0;
      while (count++ < 10) {
        publisher.publish(publisher.message(), ctx -> outboundMessageStatus.add(ctx.status()));
        Thread.sleep(100);
      }
      fail("The publisher should have been closed after entity deletion");
    } catch (ModelException ex) {
      // expected
    }
    assertThat(closedLatch).completes();
    Assertions.assertThat(outboundMessageStatus)
        .is(
            anyOf(
                new Condition<>(s -> outboundMessageStatus.isEmpty(), "no status"),
                new Condition<>(s -> outboundMessageStatus.contains(FAILED), "only failed")));
    //    Assertions.assertThat(outboundMessageStatus).containsOnly(Publisher.Status.FAILED);
    Assertions.assertThat(closedCause.get()).isNotNull().isInstanceOf(ModelException.class);
  }

  @Test
  void consumerIsClosedOnQueueDeletion(TestInfo info) {
    String q = name(info);
    CountDownLatch closedLatch = new CountDownLatch(1);
    AtomicReference<Throwable> closeCause = new AtomicReference<>();
    try {
      connection.management().queue(q).declare();
      CountDownLatch consumeLatch = new CountDownLatch(1);
      connection
          .consumerBuilder()
          .messageHandler((ctx, msg) -> consumeLatch.countDown())
          .queue(q)
          .listeners(
              ctx -> {
                if (ctx.currentState() == Resource.State.CLOSED) {
                  closeCause.set(ctx.failureCause());
                  closedLatch.countDown();
                }
              })
          .build();

      Publisher publisher = connection.publisherBuilder().queue(q).build();
      publisher.publish(publisher.message(), ctx -> {});
      assertThat(consumeLatch).completes();

    } finally {
      connection.management().queueDeletion().delete(q);
    }
    assertThat(closedLatch).completes();
    Assertions.assertThat(closeCause.get()).isNotNull().isInstanceOf(ModelException.class);
  }
}
