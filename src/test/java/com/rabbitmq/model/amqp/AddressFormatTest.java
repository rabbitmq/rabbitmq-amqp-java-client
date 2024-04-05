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
import static com.rabbitmq.model.amqp.TestUtils.assertThat;
import static com.rabbitmq.model.amqp.TestUtils.environmentBuilder;

import com.rabbitmq.model.Connection;
import com.rabbitmq.model.Environment;
import com.rabbitmq.model.Management;
import com.rabbitmq.model.Publisher;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.*;

public class AddressFormatTest {

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
  void exchangeKeyInAddress(TestInfo info) {
    String e = TestUtils.name(info);
    String q = TestUtils.name(info);
    String k = TestUtils.name(info);
    Management management = connection.management();
    try {
      management.exchange(e).type(DIRECT).declare();
      management.queue(q).declare();

      Publisher publisher = connection.publisherBuilder().exchange(e).key(k).build();

      CountDownLatch failedLatch = new CountDownLatch(1);
      publisher.publish(
          publisher.message(),
          ctx -> {
            if (ctx.status() == Publisher.Status.FAILED) {
              failedLatch.countDown();
            }
          });
      assertThat(failedLatch).completes();

      CountDownLatch consumeLatch = new CountDownLatch(1);
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, m) -> {
                ctx.accept();
                consumeLatch.countDown();
              })
          .build();

      management.binding().sourceExchange(e).key(k).destinationQueue(q).bind();
      publisher.publish(publisher.message(), ctx -> {});
      assertThat(consumeLatch).completes();
    } finally {
      management.queueDeletion().delete(q);
      management.exchangeDeletion().delete(e);
    }
  }

  @Test
  void exchangeInAddress(TestInfo info) {
    String e = TestUtils.name(info);
    String q = TestUtils.name(info);
    Management management = connection.management();
    try {
      management.exchange(e).type(FANOUT).declare();
      management.queue(q).declare();

      Publisher publisher = connection.publisherBuilder().exchange(e).build();

      CountDownLatch failedLatch = new CountDownLatch(1);
      publisher.publish(
          publisher.message(),
          ctx -> {
            if (ctx.status() == Publisher.Status.FAILED) {
              failedLatch.countDown();
            }
          });
      assertThat(failedLatch).completes();

      CountDownLatch consumeLatch = new CountDownLatch(1);
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, m) -> {
                ctx.accept();
                consumeLatch.countDown();
              })
          .build();

      management.binding().sourceExchange(e).destinationQueue(q).bind();
      publisher.publish(publisher.message(), ctx -> {});
      assertThat(consumeLatch).completes();
    } finally {
      management.queueDeletion().delete(q);
      management.exchangeDeletion().delete(e);
    }
  }

  @Test
  void queueInTargetAddress(TestInfo info) {
    String q = TestUtils.name(info);
    Management management = connection.management();
    try {
      management.queue(q).declare();

      Publisher publisher = connection.publisherBuilder().queue(q).build();

      CountDownLatch consumeLatch = new CountDownLatch(1);
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, m) -> {
                ctx.accept();
                consumeLatch.countDown();
              })
          .build();

      publisher.publish(publisher.message(), ctx -> {});
      assertThat(consumeLatch).completes();
    } finally {
      management.queueDeletion().delete(q);
    }
  }

  @Test
  void exchangeKeyInToField(TestInfo info) {
    String e = TestUtils.name(info);
    String q = TestUtils.name(info);
    String k = TestUtils.name(info);
    Management management = connection.management();
    try {
      management.exchange(e).type(DIRECT).declare();
      management.queue(q).declare();
      management.binding().sourceExchange(e).key(k).destinationQueue(q).bind();

      Publisher publisher = connection.publisherBuilder().build();

      CountDownLatch failedLatch = new CountDownLatch(2);
      publisher.publish(
          publisher.message().address().exchange(e).message(),
          ctx -> {
            if (ctx.status() == Publisher.Status.FAILED) {
              failedLatch.countDown();
            }
          });
      publisher.publish(
          publisher.message().address().exchange(e).key("foo").message(),
          ctx -> {
            if (ctx.status() == Publisher.Status.FAILED) {
              failedLatch.countDown();
            }
          });
      assertThat(failedLatch).completes();

      CountDownLatch consumeLatch = new CountDownLatch(2);
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, m) -> {
                ctx.accept();
                consumeLatch.countDown();
              })
          .build();

      publisher.publish(publisher.message().address().exchange(e).key(k).message(), ctx -> {});
      publisher.publish(publisher.message().address().queue(q).message(), ctx -> {});
      assertThat(consumeLatch).completes();
    } finally {
      management.queueDeletion().delete(q);
      management.exchangeDeletion().delete(e);
    }
  }
}
