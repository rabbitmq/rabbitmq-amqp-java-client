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

import static org.assertj.core.api.Assertions.fail;

import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Message;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.assertj.core.api.AbstractObjectAssert;

final class Assertions {

  private Assertions() {}

  static CountDownLatchAssert assertThat(CountDownLatch latch) {
    return new CountDownLatchAssert(latch);
  }

  static QueueInfoAssert assertThat(Management.QueueInfo queueInfo) {
    return new QueueInfoAssert(queueInfo);
  }

  static SyncAssert assertThat(TestUtils.Sync sync) {
    return new SyncAssert(sync);
  }

  static MessageAssert assertThat(Message message) {
    return new MessageAssert(message);
  }

  static ConnectionAssert assertThat(AmqpConnection connection) {
    return new ConnectionAssert(connection);
  }

  static class CountDownLatchAssert
      extends AbstractObjectAssert<CountDownLatchAssert, CountDownLatch> {

    private CountDownLatchAssert(CountDownLatch latch) {
      super(latch, CountDownLatchAssert.class);
    }

    private CountDownLatchAssert(AtomicReference<CountDownLatch> reference) {
      super(reference.get(), CountDownLatchAssert.class);
    }

    CountDownLatchAssert completes() {
      return this.completes(TestUtils.DEFAULT_CONDITION_TIMEOUT);
    }

    CountDownLatchAssert completes(Duration timeout) {
      try {
        boolean completed = actual.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!completed) {
          fail("Latch timed out after %d ms", timeout.toMillis());
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException(e);
      }
      return this;
    }
  }

  static class SyncAssert extends AbstractObjectAssert<SyncAssert, TestUtils.Sync> {

    private SyncAssert(TestUtils.Sync sync) {
      super(sync, SyncAssert.class);
    }

    SyncAssert completes() {
      return this.completes(TestUtils.DEFAULT_CONDITION_TIMEOUT);
    }

    SyncAssert completes(Duration timeout) {
      boolean completed = actual.await(timeout);
      if (!completed) {
        fail("Sync '%s' timed out after %d ms", this.actual.toString(), timeout.toMillis());
      }
      return this;
    }

    SyncAssert hasNotCompleted() {
      if (actual.hasCompleted()) {
        fail("Sync '%s' should not have completed", this.actual.toString());
      }
      return this;
    }
  }

  static class QueueInfoAssert extends AbstractObjectAssert<QueueInfoAssert, Management.QueueInfo> {

    private QueueInfoAssert(Management.QueueInfo queueInfo) {
      super(queueInfo, QueueInfoAssert.class);
    }

    QueueInfoAssert hasName(String name) {
      isNotNull();
      if (!actual.name().equals(name)) {
        fail("Queue should be named '%s' but is '%s'", name, actual.name());
      }
      return this;
    }

    QueueInfoAssert hasConsumerCount(int consumerCount) {
      isNotNull();
      if (Integer.compareUnsigned(actual.consumerCount(), consumerCount) != 0) {
        fail(
            "Queue should have %s consumer(s) but has %s",
            Integer.toUnsignedString(consumerCount),
            Integer.toUnsignedString(actual.consumerCount()));
      }
      return this;
    }

    QueueInfoAssert hasNoConsumers() {
      return this.hasConsumerCount(0);
    }

    QueueInfoAssert hasMessageCount(long messageCount) {
      isNotNull();
      if (Long.compareUnsigned(actual.messageCount(), messageCount) != 0) {
        fail(
            "Queue should contains %s messages(s) but contains %s",
            Long.toUnsignedString(messageCount), Long.toUnsignedString(actual.messageCount()));
      }
      return this;
    }

    QueueInfoAssert isEmpty() {
      return this.hasMessageCount(0);
    }

    QueueInfoAssert is(Management.QueueType type) {
      isNotNull();
      if (actual.type() != type) {
        fail("Queue should be of type %s but is %s", type.name(), actual.type().name());
      }
      return this;
    }

    QueueInfoAssert hasLeader(String leader) {
      Assert.notNull(leader, "Expected leader cannot be null");
      isNotNull();
      if (!leader.equals(actual.leader())) {
        fail("Queue leader should be '%s' but is '%s'", leader, actual.leader());
      }
      return this;
    }

    QueueInfoAssert doesNotHaveLeader(String leader) {
      Assert.notNull(leader, "Leader cannot be null");
      isNotNull();
      if (leader.equals(actual.leader())) {
        fail("Queue leader should not be '%s'", leader);
      }
      return this;
    }

    QueueInfoAssert hasArgument(String key, Object value) {
      isNotNull();
      if (!actual.arguments().containsKey(key) || !actual.arguments().get(key).equals(value)) {
        fail(
            "Queue should have argument %s = %s, but does not (arguments: %s)",
            key, value.toString(), actual.arguments().toString());
      }
      return this;
    }

    QueueInfoAssert isDurable() {
      return this.flag("durable", actual.durable(), true);
    }

    QueueInfoAssert isNotDurable() {
      return this.flag("durable", actual.durable(), false);
    }

    QueueInfoAssert isAutoDelete() {
      return this.flag("auto-delete", actual.autoDelete(), true);
    }

    QueueInfoAssert isNotAutoDelete() {
      return this.flag("auto-delete", actual.autoDelete(), false);
    }

    QueueInfoAssert isExclusive() {
      return this.flag("exclusive", actual.exclusive(), true);
    }

    QueueInfoAssert isNotExclusive() {
      return this.flag("exclusive", actual.exclusive(), false);
    }

    private QueueInfoAssert flag(String label, boolean expected, boolean actual) {
      isNotNull();
      if (expected != actual) {
        fail("Queue should have %s = %b but does not", label, actual);
      }
      return this;
    }
  }

  static class MessageAssert extends AbstractObjectAssert<MessageAssert, Message> {

    private MessageAssert(Message message) {
      super(message, MessageAssert.class);
    }

    MessageAssert hasId(Object id) {
      isNotNull();
      if (!actual.messageId().equals(id)) {
        fail("Message ID should be '%s' but is '%s'", id, actual.messageId());
      }
      return this;
    }

    MessageAssert hasAnnotation(String key) {
      isNotNull();
      if (!actual.hasAnnotation(key)) {
        fail("Message should have annotation '%s' but does not", key);
      }
      return this;
    }

    MessageAssert hasAnnotation(String key, Object value) {
      if (key == null || value == null) {
        throw new IllegalArgumentException();
      }
      isNotNull();
      hasAnnotation(key);
      if (!value.equals(this.actual.annotation(key))) {
        fail(
            "Message should have annotation '%s = %s' but has '%s = %s'",
            key, value, key, this.actual.annotation(key));
      }
      return this;
    }

    MessageAssert doesNotHaveAnnotation(String key) {
      isNotNull();
      if (actual.hasAnnotation(key)) {
        fail("Message should not have annotation '%s' but has it", key);
      }
      return this;
    }
  }

  static class ConnectionAssert extends AbstractObjectAssert<ConnectionAssert, AmqpConnection> {

    private ConnectionAssert(AmqpConnection connection) {
      super(connection, ConnectionAssert.class);
    }

    ConnectionAssert hasNodename(String nodename) {
      Assert.notNull(nodename, "Expected nodename cannot be null");
      isNotNull();
      if (!actual.connectionNodename().equals(nodename)) {
        fail(
            "Connection should be on node '%s' but is on node '%s'",
            nodename, actual.connectionNodename());
      }
      return this;
    }

    ConnectionAssert isOnLeader(Management.QueueInfo info) {
      Assert.notNull(info, "Queue info cannot be null");
      String actualLeader = info.leader();
      if (!actualLeader.equals(actual.connectionNodename())) {
        fail(
            "Connection is expected to be on leader node '%s' but is on '%s'",
            actualLeader, actual.connectionNodename());
      }
      return this;
    }

    ConnectionAssert isOnFollower(Management.QueueInfo info) {
      Assert.notNull(info, "Queue info cannot be null");
      List<String> followers =
          info.replicas().stream()
              .filter(n -> !n.equals(info.leader()))
              .collect(Collectors.toList());
      if (!followers.contains(actual.connectionNodename())) {
        fail(
            "Connection is expected to be on follower node(s) '%s' but is on '%s'",
            followers, actual.connectionNodename());
      }
      return this;
    }
  }
}
