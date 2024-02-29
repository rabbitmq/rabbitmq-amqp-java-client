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
package com.rabbitmq.model;

import static com.rabbitmq.model.Management.ExchangeType.DIRECT;
import static com.rabbitmq.model.Management.ExchangeType.FANOUT;
import static com.rabbitmq.model.Management.QueueType.QUORUM;
import static com.rabbitmq.model.TestUtils.CountDownLatchConditions.completed;
import static com.rabbitmq.model.TestUtils.environmentBuilder;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class AmqpTest {

  @Test
  void queueDeclareDeletePublishConsume(TestInfo info) {
    String q = TestUtils.name(info);
    Environment environment = environmentBuilder().build();
    try {
      environment.management().queue().name(q).quorum().queue().declare();
      String address = "/amq/queue/" + q;
      Publisher publisher = environment.publisherBuilder().address(address).build();

      int messageCount = 10;
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
                      if (context.status() == Publisher.ConfirmationStatus.CONFIRMED) {
                        confirmLatch.countDown();
                      }
                    });
              });

      assertThat(confirmLatch).is(completed());

      CountDownLatch consumeLatch = new CountDownLatch(messageCount);
      com.rabbitmq.model.Consumer consumer =
          environment
              .consumerBuilder()
              .address(address)
              .messageHandler(
                  (context, message) -> {
                    context.accept();
                    consumeLatch.countDown();
                  })
              .build();
      assertThat(consumeLatch).is(completed());
      consumer.close();
      publisher.close();
    } finally {
      environment.management().queueDeletion().delete(q);
      environment.close();
    }
  }

  @Test
  void binding(TestInfo info) throws InterruptedException {
    String e1 = TestUtils.name(info);
    String e2 = TestUtils.name(info);
    String q = TestUtils.name(info);
    String rk = "foo";
    Environment environment = environmentBuilder().build();
    Management management = environment.management();
    try {
      management.exchange().name(e1).type(DIRECT).declare();
      management.exchange().name(e2).type(FANOUT).declare();
      management.queue().name(q).type(QUORUM).declare();
      management.binding().sourceExchange(e1).destinationExchange(e2).key(rk).bind();
      management.binding().sourceExchange(e2).destinationQueue(q).bind();

      Publisher publisher1 = environment.publisherBuilder().address("/exchange/" + e1).build();
      Publisher publisher2 = environment.publisherBuilder().address("/exchange/" + e2).build();

      int messageCount = 1;
      CountDownLatch confirmLatch = new CountDownLatch(messageCount * 2);

      Consumer<Publisher> publish =
          publisher ->
              publisher.publish(
                  publisher.message().subject(rk).addData("hello".getBytes(StandardCharsets.UTF_8)),
                  context -> {
                    if (context.status() == Publisher.ConfirmationStatus.CONFIRMED) {
                      confirmLatch.countDown();
                    }
                  });

      range(0, messageCount).forEach(ignored -> publish.accept(publisher1));
      range(0, messageCount).forEach(ignored -> publish.accept(publisher2));

      assertThat(confirmLatch).is(completed());

      CountDownLatch consumeLatch = new CountDownLatch(messageCount * 2);
      com.rabbitmq.model.Consumer consumer =
          environment
              .consumerBuilder()
              .address(q)
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
      management.unbind().sourceExchange(e2).destinationQueue(q).unbind();
      management.unbind().sourceExchange(e1).destinationExchange(e2).key(rk).unbind();
      management.exchangeDeletion().delete(e2);
      management.exchangeDeletion().delete(e1);
      management.queueDeletion().delete(q);

      environment.close();
    }
  }
}
