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

import static com.rabbitmq.model.Management.QueueLeaderLocator.BALANCED;
import static java.lang.String.format;

import com.rabbitmq.model.ByteCapacity;
import com.rabbitmq.model.Management;
import com.rabbitmq.model.Management.QueueType;
import com.rabbitmq.model.ModelException;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpQueueSpecification implements Management.QueueSpecification {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpQueueSpecification.class);

  private static final Duration TEN_YEARS = Duration.ofDays(365 * 10);

  private final AmqpManagement management;

  // flag to detect no "shortcut" has been called and that no validation should occur
  // this would allow to declare a queue with the plain arguments, and without
  // validation on the client side, in case the validation blocked a given combination
  // of parameters (that would be considered a bug, but this would at least unlock
  // the user until the fix arrives)
  private boolean shortcutArguments = false;
  private String name;
  private final boolean durable = true;
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
  public Management.QueueSpecification overflowStrategy(Management.OverFlowStrategy overflow) {
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
    return new AmqpQuorumQueueSpecification(this);
  }

  @Override
  public Management.ClassicQueueSpecification classic() {
    return null;
  }

  @Override
  public Management.StreamSpecification stream() {
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
  public void declare() {
    try {
      this.management
          .channel()
          .queueDeclare(this.name, this.durable, this.exclusive, this.autoDelete, this.arguments);
    } catch (IOException e) {
      throw new ModelException(e);
    }
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
    public Management.QuorumQueueSpecification maxInMemoryLength(long maxInMemoryLength) {
      validatePositive("x-max-in-memory-length", maxInMemoryLength);
      this.parent.arg("x-max-in-memory-length", maxInMemoryLength);
      return this;
    }

    @Override
    public Management.QuorumQueueSpecification maxInMemoryBytes(ByteCapacity maxInMemoryBytes) {
      validatePositive("x-max-in-memory-bytes", maxInMemoryBytes.toBytes());
      this.parent.arg("x-max-in-memory-bytes", maxInMemoryBytes.toBytes());
      return this;
    }

    @Override
    public Management.QuorumQueueSpecification deliveryLimit(int limit) {
      validatePositive("x-max-delivery-limit", limit);
      this.parent.arg("x-max-delivery-limit", limit);
      return this;
    }

    @Override
    public Management.QuorumQueueSpecification quorumInitialGroupSize(int size) {
      validatePositive("x-quorum-initial-group-size", size);
      this.parent.arg("x-quorum-initial-group-size", size);
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
    public Management.ClassicQueueSpecification mode(Management.ClassicQueueMode mode) {
      if (mode == null) {
        this.parent.arg("x-queue-mode", null);
      } else {
        this.parent.arg("x-queue-mode", mode.name().toLowerCase(Locale.ENGLISH));
      }
      return this;
    }

    @Override
    public Management.ClassicQueueSpecification version(Management.ClassicQueueVersion version) {
      if (version == null) {
        this.parent.arg("x-queue-mode", null);
      } else {
        this.parent.arg("x-queue-mode", version.version());
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
    public Management.StreamSpecification initialClusterSize(int initialClusterSize) {
      validatePositive("x-initial-cluster-size", initialClusterSize);
      this.parent.arg("x-initial-cluster-size", initialClusterSize);
      return this;
    }

    @Override
    public Management.QueueSpecification specification() {
      return this.parent;
    }
  }
}
