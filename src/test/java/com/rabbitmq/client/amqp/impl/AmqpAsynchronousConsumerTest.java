// Copyright (c) 2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.name;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.SynchronousConsumer;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@AmqpTestInfrastructure
public class AmqpAsynchronousConsumerTest {

  Duration timeout = Duration.ofMillis(100);
  AmqpConnection connection;
  Publisher publisher;
  SynchronousConsumer consumer;
  String q;
  String connectionName;
  int messageCount = 10;
  List<SynchronousConsumer.Response> responses;

  @BeforeEach
  void init(TestInfo info) {
    this.q = name(info);
    this.connectionName = connection.name();
    this.connection.management().queue(this.q).exclusive(true).declare();
    this.publisher = connection.publisherBuilder().queue(this.q).build();
    this.consumer = new AmqpSynchronousConsumer(this.q, this.connection, List.of());
  }

  @AfterEach
  void tearDown() {
    this.consumer.close();
    this.publisher.close();
  }

  @Test
  void requeuedMessageShouldComeBackOnNextGet() {
    assertThat(consumer.get(timeout)).isNull();
    byte[] body = UUID.randomUUID().toString().getBytes(UTF_8);
    publisher.publish(publisher.message(body), ctx -> {});
    waitAtMost(() -> queueMessageCount() == 1);

    // get and requeue
    SynchronousConsumer.Response response = consumer.get(timeout);
    assertThat(response).isNotNull();
    assertThat(response.message()).hasBody(body);
    waitAtMost(() -> queueMessageCount() == 0);
    response.context().requeue();
    waitAtMost(() -> queueMessageCount() == 1);

    // get again and accept
    response = consumer.get(timeout);
    assertThat(response).isNotNull();
    assertThat(response.message()).hasBody(body);
    response.context().accept();
    waitAtMost(() -> queueMessageCount() == 0);
  }

  @Test
  void multipleGetShouldReturnRequestedNumberOfMessages() {
    publish(messageCount);
    responses = consumer.get(messageCount, timeout);
    assertThat(responses).hasSize(messageCount);
    accept(responses);
    waitAtMost(() -> queueMessageCount() == 0);
  }

  @Test
  void multipleGetShouldReturnAllQueueMessagesIfRequestedGreaterThanCount() {
    publish(messageCount);
    responses = consumer.get(messageCount + 2, timeout);
    assertThat(responses).hasSize(messageCount);
    accept(responses);
    waitAtMost(() -> queueMessageCount() == 0);
  }

  @Test
  void multipleGetShouldNotEmptyQueue() {
    publish(messageCount * 2);
    responses = consumer.get(messageCount, timeout);
    assertThat(responses).hasSize(messageCount);
    accept(responses);
    waitAtMost(() -> queueMessageCount() == messageCount);

    responses = consumer.get(messageCount, timeout);
    assertThat(responses).hasSize(messageCount);
    accept(responses);
    waitAtMost(() -> queueMessageCount() == 0);
  }

  @Test
  void multipleGetShouldReturnEmptyListIfNoMessages() {
    responses = consumer.get(messageCount, timeout);
    assertThat(responses).isEmpty();
  }

  @Test
  void multipleGetAndBatchAccept() {
    publish(messageCount * 2);
    responses = consumer.get(messageCount, timeout);
    assertThat(responses).hasSize(messageCount);
    Consumer.BatchContext batch = null;
    for (SynchronousConsumer.Response r : responses) {
      if (batch == null) {
        batch = r.context().batch(messageCount);
      }
      batch.add(r.context());
    }
    batch.accept();
    waitAtMost(() -> queueMessageCount() == messageCount);

    responses = consumer.get(messageCount, timeout);
    assertThat(responses).hasSize(messageCount);
    batch = null;
    for (SynchronousConsumer.Response r : responses) {
      if (batch == null) {
        batch = r.context().batch(messageCount);
      }
      batch.add(r.context());
    }
    batch.accept();
    waitAtMost(() -> queueMessageCount() == 0);
  }

  @Test
  void requeuedMessageNotInBatchShouldGoBackToQueue() {
    publish(messageCount);
    responses = consumer.get(messageCount, timeout);
    assertThat(responses).hasSize(messageCount);
    Consumer.BatchContext batch = null;
    int indexToRequeue = new Random().nextInt(messageCount);
    for (int i = 0; i < messageCount; i++) {
      if (i == indexToRequeue) {
        responses.get(i).context().requeue();
      } else {
        if (batch == null) {
          batch = responses.get(i).context().batch(messageCount - 1);
        }
        batch.add(responses.get(i).context());
      }
    }
    batch.accept();
    waitAtMost(() -> queueMessageCount() == 1);

    responses = consumer.get(messageCount, timeout);
    assertThat(responses).hasSize(1);
    responses.get(0).context().accept();
    waitAtMost(() -> queueMessageCount() == 0);
  }

  private void publish(int count) {
    IntStream.range(0, count).forEach(ignored -> publisher.publish(publisher.message(), ctx -> {}));
  }

  private void accept(List<SynchronousConsumer.Response> responses) {
    responses.forEach(response -> response.context().accept());
  }

  private long queueMessageCount() {
    return connection.management().queueInfo(q).messageCount();
  }
}
