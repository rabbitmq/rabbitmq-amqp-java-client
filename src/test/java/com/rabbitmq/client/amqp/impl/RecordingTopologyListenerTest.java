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

import static com.rabbitmq.client.amqp.impl.RecordingTopologyListenerTest.RecoveryAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.assertj.core.api.AbstractObjectAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RecordingTopologyListenerTest {

  RecordingTopologyListener recovery;
  ExecutorService executorService;

  @BeforeEach
  void init() {
    executorService = Executors.newSingleThreadExecutor();
    recovery = new RecordingTopologyListener("", new EventLoop(executorService));
  }

  @AfterEach
  void tearDown() {
    recovery.close();
    executorService.shutdownNow();
  }

  @Test
  void entitiesRemovedAfterDeletion() {
    recovery.exchangeDeclared(exchange("ex"));
    assertThat(recovery).hasExchangeCount(1).hasExchange("ex");
    recovery.exchangeDeleted("ex");
    assertThat(recovery).hasNoExchanges();

    recovery.queueDeclared(queue("q"));
    assertThat(recovery).hasQueueCount(1).hasQueue("q");
    recovery.queueDeleted("q");
    assertThat(recovery).hasNoQueues();

    recovery.bindingDeclared(binding("ex", "q"));
    assertThat(recovery).hasBindingCount(1);
    recovery.bindingDeleted(unbinding("ex", "q"));
    assertThat(recovery).hasNoBindings();
  }

  @Test
  void bindingsAreDeletedAfterExchangeDeletion() {
    recovery.exchangeDeclared(exchange("ex1"));
    recovery.exchangeDeclared(exchange("ex2"));
    recovery.bindingDeclared(binding("ex1", "q1"));
    recovery.bindingDeclared(binding("ex1", "q2"));
    recovery.bindingDeclared(binding("ex2", "q1"));

    assertThat(recovery).hasExchangeCount(2).hasBindingCount(3);

    recovery.exchangeDeleted("ex1");
    assertThat(recovery).hasExchangeCount(1).hasExchange("ex2").hasBindingCount(1);
  }

  @Test
  void bindingsAreDeletedAfterQueueDeletion() {
    recovery.queueDeclared(queue("q1"));
    recovery.queueDeclared(queue("q2"));
    recovery.bindingDeclared(binding("ex1", "q1"));
    recovery.bindingDeclared(binding("ex1", "q2"));
    recovery.bindingDeclared(binding("ex2", "q1"));

    assertThat(recovery).hasQueueCount(2).hasBindingCount(3);

    recovery.queueDeleted("q1");
    assertThat(recovery).hasQueueCount(1).hasQueue("q2").hasBindingCount(1);
  }

  @Test
  void autoDeleteQueueIsDeletedAfterLastConsumerIsClosed() {
    recovery.queueDeclared(queue("q1"));
    recovery.queueDeclared(queue("q2"));
    recovery.queueDeclared(autoDeleteQueue("ad-q"));

    recovery.consumerCreated(1, "q1");
    recovery.consumerCreated(2, "q2");
    recovery.consumerCreated(3, "ad-q");
    recovery.consumerCreated(4, "ad-q");

    assertThat(recovery).hasQueueCount(3).hasQueues("q1", "q2", "ad-q");

    recovery.consumerDeleted(1, "q1");
    assertThat(recovery).hasQueueCount(3).hasQueues("q1", "q2", "ad-q");

    recovery.consumerDeleted(3, "ad-q");
    assertThat(recovery).hasQueueCount(3).hasQueues("q1", "q2", "ad-q");

    recovery.consumerDeleted(4, "ad-q");
    assertThat(recovery).hasQueueCount(2).hasQueues("q1", "q2");
  }

  @Test
  void autoDeleteExchangeIsDeletedWhenLastBindingIsDeleted() {
    recovery.exchangeDeclared(exchange("ex1"));
    recovery.exchangeDeclared(exchange("ex2"));
    recovery.exchangeDeclared(autoDeleteExchange("ad-e"));

    recovery.bindingDeclared(binding("ex1", "q1"));
    recovery.bindingDeclared(binding("ex2", "q2"));

    recovery.bindingDeclared(binding("ad-e", "q1"));
    recovery.bindingDeclared(binding("ad-e", "q2"));

    assertThat(recovery).hasExchangeCount(3).hasExchanges("ex1", "ex2", "ad-e").hasBindingCount(4);

    recovery.bindingDeleted(unbinding("ad-e", "q1"));
    assertThat(recovery).hasExchangeCount(3).hasExchanges("ex1", "ex2", "ad-e").hasBindingCount(3);

    recovery.bindingDeleted(unbinding("ad-e", "q2"));
    assertThat(recovery).hasExchangeCount(2).hasExchanges("ex1", "ex2").hasBindingCount(2);

    recovery.exchangeDeclared(autoDeleteExchange("ad-e"));
    recovery.queueDeclared(queue("q1"));
    recovery.queueDeclared(queue("q2"));
    recovery.bindingDeclared(binding("ad-e", "q1"));
    recovery.bindingDeclared(binding("ad-e", "q2"));

    assertThat(recovery)
        .hasExchangeCount(3)
        .hasExchanges("ex1", "ex2", "ad-e")
        .hasQueueCount(2)
        .hasBindingCount(4);

    recovery.queueDeleted("q1");
    assertThat(recovery)
        .hasExchangeCount(3)
        .hasExchanges("ex1", "ex2", "ad-e")
        .hasQueueCount(1)
        .hasBindingCount(2);

    recovery.queueDeleted("q2");
    assertThat(recovery)
        .hasExchangeCount(2)
        .hasExchanges("ex1", "ex2")
        .hasNoQueues()
        .hasNoBindings();
  }

  static AmqpExchangeSpecification exchange(String name) {
    return (AmqpExchangeSpecification) new AmqpExchangeSpecification(null).name(name);
  }

  static AmqpExchangeSpecification autoDeleteExchange(String name) {
    return (AmqpExchangeSpecification)
        new AmqpExchangeSpecification(null).name(name).autoDelete(true);
  }

  static AmqpQueueSpecification queue(String name) {
    return (AmqpQueueSpecification) new AmqpQueueSpecification(null).name(name);
  }

  static AmqpQueueSpecification autoDeleteQueue(String name) {
    return (AmqpQueueSpecification) new AmqpQueueSpecification(null).name(name).autoDelete(true);
  }

  static AmqpBindingManagement.AmqpBindingSpecification binding(String source, String destination) {
    return (AmqpBindingManagement.AmqpBindingSpecification)
        new AmqpBindingManagement.AmqpBindingSpecification(null)
            .sourceExchange(source)
            .destinationQueue(destination);
  }

  static AmqpBindingManagement.AmqpUnbindSpecification unbinding(
      String source, String destination) {
    return (AmqpBindingManagement.AmqpUnbindSpecification)
        new AmqpBindingManagement.AmqpUnbindSpecification(null)
            .sourceExchange(source)
            .destinationQueue(destination);
  }

  static class RecoveryAssert
      extends AbstractObjectAssert<RecoveryAssert, RecordingTopologyListener> {

    private RecoveryAssert(RecordingTopologyListener defaultManagementRecovery) {
      super(defaultManagementRecovery, RecoveryAssert.class);
    }

    static RecoveryAssert assertThat(RecordingTopologyListener recovery) {
      return new RecoveryAssert(recovery);
    }

    private RecoveryAssert hasExchanges(String... exchanges) {
      for (String exchange : exchanges) {
        hasExchange(exchange);
      }
      return this;
    }

    private RecoveryAssert hasExchange(String exchange) {
      isNotNull();
      if (!actual.exchanges().containsKey(exchange)) {
        fail(
            "Should have exchange '%s' but does not: %s",
            exchange, String.join(", ", actual.exchanges().keySet()));
      }
      return this;
    }

    private RecoveryAssert hasExchangeCount(int expectedCount) {
      isNotNull();
      if (actual.exchangeCount() != expectedCount) {
        fail(
            "Exchange count is expected to be %d but is %d", expectedCount, actual.exchangeCount());
      }
      return this;
    }

    private RecoveryAssert hasNoExchanges() {
      return hasExchangeCount(0);
    }

    private RecoveryAssert hasQueue(String queue) {
      isNotNull();
      if (!actual.queues().containsKey(queue)) {
        fail(
            "Should have queue '%s' but does not: %s",
            queue, String.join(", ", actual.queues().keySet()));
      }
      return this;
    }

    private RecoveryAssert hasQueues(String... queues) {
      isNotNull();
      for (String queue : queues) {
        hasQueue(queue);
      }
      return this;
    }

    private RecoveryAssert hasQueueCount(int expectedCount) {
      isNotNull();
      if (actual.queueCount() != expectedCount) {
        fail("Queue count is expected to be %d but is %d", expectedCount, actual.queueCount());
      }
      return this;
    }

    private RecoveryAssert hasNoQueues() {
      return hasQueueCount(0);
    }

    private RecoveryAssert hasBindingCount(int expectedCount) {
      isNotNull();
      if (actual.bindingCount() != expectedCount) {
        fail("Binding count is expected to be %d but is %d", expectedCount, actual.bindingCount());
      }
      return this;
    }

    private RecoveryAssert hasNoBindings() {
      return hasBindingCount(0);
    }
  }
}
