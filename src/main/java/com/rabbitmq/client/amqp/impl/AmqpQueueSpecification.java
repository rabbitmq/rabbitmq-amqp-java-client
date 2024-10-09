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

import static com.rabbitmq.client.amqp.Management.QueueLeaderLocator.BALANCED;
import static java.lang.String.format;

import com.rabbitmq.client.amqp.ByteCapacity;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Management.QueueType;
import java.time.Duration;
import java.util.*;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpQueueSpecification implements Management.QueueSpecification {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpQueueSpecification.class);
  private static final Duration TEN_YEARS = Duration.ofDays(365 * 10);
  private static final boolean DURABLE = true;

  private final AmqpManagement management;

  // flag to detect no "shortcut" has been called and that no validation should occur
  // this would allow to declare a queue with the plain arguments, and without
  // validation on the client side, in case the validation blocked a given combination
  // of parameters (that would be considered a bug, but this would at least unlock
  // the user until the fix arrives)
  private boolean shortcutArguments = false;
  private String name;
  private boolean exclusive = false;
  private boolean autoDelete = false;
  private final Map<String, Object> arguments = new LinkedHashMap<>();

  AmqpQueueSpecification(AmqpManagement management) {
    this.management = management;
  }

  @Override
  public Management.QueueSpecification name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public Management.QueueSpecification exclusive(boolean exclusive) {
    this.exclusive = exclusive;
    return this;
  }

  @Override
  public Management.QueueSpecification autoDelete(boolean autoDelete) {
    this.autoDelete = autoDelete;
    return this;
  }

  @Override
  public Management.QueueSpecification type(QueueType type) {
    if (type == QueueType.QUORUM || type == QueueType.STREAM) {
      this.exclusive(false).autoDelete(false);
    }
    this.arg("x-queue-type", type.name().toLowerCase(Locale.ENGLISH));
    return this;
  }

  @Override
  public Management.QueueSpecification deadLetterExchange(String dlx) {
    this.arg("x-dead-letter-exchange", dlx);
    return this;
  }

  @Override
  public Management.QueueSpecification deadLetterRoutingKey(String dlrk) {
    this.arg("x-dead-letter-routing-key", dlrk);
    return this;
  }

  @Override
  public Management.QueueSpecification overflowStrategy(String overflow) {
    this.arg("x-overflow", overflow);
    return this;
  }

  @Override
  public Management.QueueSpecification overflowStrategy(Management.OverflowStrategy overflow) {
    this.arg("x-overflow", overflow.strategy());
    return this;
  }

  @Override
  public Management.QueueSpecification expires(Duration expiration) {
    validatePositive("x-expires", expiration.toMillis(), TEN_YEARS.toMillis());
    this.arg("x-expires", expiration.toMillis());
    return this;
  }

  @Override
  public Management.QueueSpecification maxLength(long maxLength) {
    validatePositive("x-max-length", maxLength);
    this.arg("x-max-length", maxLength);
    return this;
  }

  @Override
  public Management.QueueSpecification maxLengthBytes(ByteCapacity maxLengthBytes) {
    validatePositive("x-max-length-bytes", maxLengthBytes.toBytes());
    this.arg("x-max-length-bytes", maxLengthBytes.toBytes());
    return this;
  }

  @Override
  public Management.QueueSpecification singleActiveConsumer(boolean singleActiveConsumer) {
    this.arg("x-single-active-consumer", singleActiveConsumer);
    return this;
  }

  @Override
  public Management.QueueSpecification messageTtl(Duration ttl) {
    validateNonNegative("x-message-ttl", ttl.toMillis(), TEN_YEARS.toMillis());
    this.arg("x-message-ttl", ttl.toMillis());
    return this;
  }

  @Override
  public Management.QueueSpecification leaderLocator(Management.QueueLeaderLocator locator) {
    if (locator == null) {
      arg("x-queue-master-locator", null);
      arg("x-queue-leader-locator", null);
    } else {
      QueueType type = type();
      if (type == QueueType.CLASSIC) {
        arg("x-queue-master-locator", locator == BALANCED ? "min-masters" : locator.locator());
      } else if (type == QueueType.QUORUM || type == QueueType.STREAM) {
        arg("x-queue-leader-locator", locator.locator());
      } else {
        LOGGER.warn("Set queue type before setting leader locator");
      }
    }
    return this;
  }

  @Override
  public Management.QuorumQueueSpecification quorum() {
    this.type(QueueType.QUORUM);
    return new AmqpQuorumQueueSpecification(this);
  }

  @Override
  public Management.ClassicQueueSpecification classic() {
    this.type(QueueType.CLASSIC);
    return new AmqpClassicQueueSpecification(this);
  }

  @Override
  public Management.StreamSpecification stream() {
    this.type(QueueType.STREAM);
    return new AmqpStreamSpecification(this);
  }

  @Override
  public Management.QueueSpecification argument(String key, Object value) {
    if (value == null) {
      this.arguments.remove(key);
    } else {
      this.arguments.put(key, value);
    }
    return this;
  }

  @Override
  public Management.QueueInfo declare() {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("durable", DURABLE);
    body.put("exclusive", this.exclusive);
    body.put("auto_delete", this.autoDelete);
    body.put("arguments", this.arguments);
    Management.QueueInfo info = this.management.declareQueue(this.name, body);
    AmqpQueueSpecification copy = this.duplicate();
    copy.name(info.name());
    this.management.recovery().queueDeclared(copy);
    return info;
  }

  private Map<String, Object> arg(String key, Object value) {
    if (value == null) {
      this.arguments.remove(key);
    } else {
      this.arguments.put(key, value);
    }
    this.shortcutArguments = true;
    return this.arguments;
  }

  private static void validatePositive(String label, long value) {
    validatePositive(label, value, 0);
  }

  private static void validatePositive(String label, long value, long max) {
    if (value <= 0) {
      throw new IllegalArgumentException(format("'%s' must be positive", label));
    }
    if (max > 0) {
      if (value > max) {
        throw new IllegalArgumentException(format("'%s' must be lesser than %d", label, max));
      }
    }
  }

  private static void validateNonNegative(String label, long value, long max) {
    if (value < 0) {
      throw new IllegalArgumentException(format("'%s' must be greater than or equal to 0", label));
    }
    if (max > 0) {
      if (value > max) {
        throw new IllegalArgumentException(format("'%s' must be lesser than %d", label, max));
      }
    }
  }

  QueueType type() {
    String type = (String) this.arguments.get("x-queue-type");
    for (QueueType value : QueueType.values()) {
      if (value.toString().toLowerCase(Locale.ENGLISH).equals(type)) {
        return value;
      }
    }
    return null;
  }

  private static class AmqpQuorumQueueSpecification implements Management.QuorumQueueSpecification {

    private final AmqpQueueSpecification parent;

    private AmqpQuorumQueueSpecification(AmqpQueueSpecification parent) {
      this.parent = parent;
    }

    @Override
    public Management.QuorumQueueSpecification deadLetterStrategy(String strategy) {
      this.parent.arg("x-dead-letter-strategy", strategy);
      return this;
    }

    @Override
    public Management.QuorumQueueSpecification deadLetterStrategy(
        Management.QuorumQueueDeadLetterStrategy strategy) {
      this.parent.arg("x-dead-letter-strategy", strategy.strategy());
      return this;
    }

    @Override
    public Management.QuorumQueueSpecification deliveryLimit(int limit) {
      validatePositive("x-delivery-limit", limit);
      this.parent.arg("x-delivery-limit", limit);
      return this;
    }

    @Override
    @SuppressWarnings("removal")
    public Management.QuorumQueueSpecification quorumInitialGroupSize(int size) {
      return this.initialMemberCount(size);
    }

    @Override
    public Management.QuorumQueueSpecification initialMemberCount(int initialMemberCount) {
      validatePositive("x-quorum-initial-group-size", initialMemberCount);
      this.parent.arg("x-quorum-initial-group-size", initialMemberCount);
      return this;
    }

    @Override
    public Management.QueueSpecification queue() {
      return parent;
    }
  }

  private static class AmqpClassicQueueSpecification
      implements Management.ClassicQueueSpecification {

    private final AmqpQueueSpecification parent;

    private AmqpClassicQueueSpecification(AmqpQueueSpecification parent) {
      this.parent = parent;
    }

    @Override
    public Management.ClassicQueueSpecification maxPriority(int maxPriority) {
      validateNonNegative("x-max-priority", maxPriority, 256);
      this.parent.arg("x-max-priority", maxPriority);
      return this;
    }

    @Override
    public Management.ClassicQueueSpecification version(Management.ClassicQueueVersion version) {
      if (version == null) {
        this.parent.arg("x-queue-version", null);
      } else {
        this.parent.arg("x-queue-version", version.version());
      }
      return this;
    }

    @Override
    public Management.QueueSpecification queue() {
      return this.parent;
    }
  }

  private static class AmqpStreamSpecification implements Management.StreamSpecification {

    private final AmqpQueueSpecification parent;

    private AmqpStreamSpecification(AmqpQueueSpecification parent) {
      this.parent = parent;
    }

    @Override
    public Management.StreamSpecification maxAge(Duration maxAge) {
      validatePositive("x-max-age", maxAge.getSeconds());
      this.parent.arg("x-max-age", maxAge.getSeconds() + "s");
      return this;
    }

    @Override
    public Management.StreamSpecification maxSegmentSizeBytes(ByteCapacity maxSegmentSize) {
      validatePositive("x-stream-max-segment-size-bytes", maxSegmentSize.toBytes());
      this.parent.arg("x-stream-max-segment-size-bytes", maxSegmentSize.toBytes());
      return this;
    }

    @Override
    @SuppressWarnings("removal")
    public Management.StreamSpecification initialClusterSize(int initialClusterSize) {
      return this.initialMemberCount(initialClusterSize);
    }

    @Override
    public Management.StreamSpecification initialMemberCount(int initialMemberCount) {
      validatePositive("x-initial-cluster-size", initialMemberCount);
      this.parent.arg("x-initial-cluster-size", initialMemberCount);
      return this;
    }

    @Override
    public Management.QueueSpecification queue() {
      return this.parent;
    }
  }

  String name() {
    return this.name;
  }

  boolean exclusive() {
    return this.exclusive;
  }

  boolean autoDelete() {
    return this.autoDelete;
  }

  void arguments(BiConsumer<String, Object> consumer) {
    this.arguments.forEach(consumer);
  }

  AmqpQueueSpecification duplicate() {
    AmqpQueueSpecification copy = new AmqpQueueSpecification(this.management);
    copy.name(this.name).exclusive(this.exclusive).autoDelete(this.autoDelete);
    copy.shortcutArguments = this.shortcutArguments;
    copy.arguments.putAll(this.arguments);
    return copy;
  }
}
