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
import static com.rabbitmq.client.amqp.impl.TestUtils.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.*;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(AmqpTestInfrastructureExtension.class)
public class AddressFormatTest {

  Connection connection;

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
          publisher.message().toAddress().exchange(e).message(),
          ctx -> {
            if (ctx.status() == Publisher.Status.FAILED) {
              failedLatch.countDown();
            }
          });
      publisher.publish(
          publisher.message().toAddress().exchange(e).key("foo").message(),
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

      publisher.publish(publisher.message().toAddress().exchange(e).key(k).message(), ctx -> {});
      publisher.publish(publisher.message().toAddress().queue(q).message(), ctx -> {});
      assertThat(consumeLatch).completes();
    } finally {
      management.queueDeletion().delete(q);
      management.exchangeDeletion().delete(e);
    }
  }

  @Test
  void noToFieldDoesNotCloseAllConnectionPublishers() throws InterruptedException {
    Management management = connection.management();
    String q = management.queue().exclusive(true).declare().name();
    TestUtils.Sync sync = TestUtils.sync();
    Consumer consumer =
        connection
            .consumerBuilder()
            .queue(q)
            .messageHandler(
                (ctx, msg) -> {
                  ctx.accept();
                  sync.down();
                })
            .build();
    try {
      TestUtils.Sync pubClosed = TestUtils.sync(1);
      Publisher p1 =
          connection
              .publisherBuilder()
              .listeners(
                  ctx -> {
                    if (ctx.currentState() == Resource.State.CLOSED) {
                      pubClosed.down();
                    }
                  })
              .build();
      p1.publish(p1.message().toAddress().queue(q).message(), ctx -> {});
      assertThat(sync).completes();

      sync.reset();
      Publisher p2 = connection.publisherBuilder().queue(q).build();
      p2.publish(p2.message(), ctx -> {});
      assertThat(sync).completes();

      p1.publish(p1.message(), ctx -> {});

      sync.reset();
      p2.publish(p2.message(), ctx -> {});
      assertThat(sync).completes();
    } finally {
      consumer.close();
    }
  }

  @Test
  void defaultExchangeIsNotAllowed() {
    assertThatThrownBy(() -> connection.publisherBuilder().exchange("").build())
        .isInstanceOf(IllegalArgumentException.class);
  }
}
