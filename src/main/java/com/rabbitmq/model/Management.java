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

import java.time.Duration;

public interface Management extends AutoCloseable {

  QueueSpecification queue();

  QueueDeletion queueDeletion();

  @Override
  void close();

  interface QueueSpecification {

    QueueSpecification name(String name);

    QueueSpecification durable(boolean durable);

    QueueSpecification exclusive(boolean exclusive);

    QueueSpecification autoDelete(boolean autoDelete);

    QueueSpecification type(QueueType type);

    QueueSpecification deadLetterExchange(String dlx);

    QueueSpecification deadLetterRoutingKey(String dlrk);

    QueueSpecification overflowStrategy(String overflow);

    QueueSpecification overflowStrategy(OverFlowStrategy overflow);

    // Server-side check: > 0, not more than 10 years
    // in milliseconds
    QueueSpecification expires(Duration expiration);

    // > 0
    QueueSpecification maxLength(long maxLength);

    QueueSpecification maxLengthBytes(ByteCapacity maxLengthBytes);

    QueueSpecification singleActiveConsumer(boolean singleActiveConsumer);

    // Server-side check: > 0, not more than 10 years
    // in milliseconds
    QueueSpecification messageTtl(Duration ttl);

    // x-queue-master-locator for classic queues
    // x-queue-leader-locator for quorum queues and streams
    // (balanced = min-masters for classic queues)
    QueueSpecification leaderLocator(QueueLeaderLocator locator);

    QuorumQueueSpecification quorum();

    ClassicQueueSpecification classic();

    StreamSpecification stream();

    QueueSpecification argument(String key, Object value);

    void declare();
  }

  interface QuorumQueueSpecification {

    QuorumQueueSpecification deadLetterStrategy(String strategy);

    QuorumQueueSpecification deadLetterStrategy(QuorumQueueDeadLetterStrategy strategy);

    QuorumQueueSpecification maxInMemoryLength(long maxInMemoryLength);

    QuorumQueueSpecification maxInMemoryBytes(ByteCapacity maxInMemoryBytes);

    QuorumQueueSpecification deliveryLimit(int limit);

    QuorumQueueSpecification quorumInitialGroupSize(int size);

    QueueSpecification queue();
  }

  interface ClassicQueueSpecification {

    // 1 <= maxPriority <= 255
    ClassicQueueSpecification maxPriority(int maxPriority);

    ClassicQueueSpecification mode(ClassicQueueMode mode);

    ClassicQueueSpecification version(ClassicQueueVersion version);

    QueueSpecification queue();
  }

  interface StreamSpecification {

    StreamSpecification maxAge(Duration maxAge);

    StreamSpecification maxSegmentSizeBytes(ByteCapacity maxSegmentSize);

    StreamSpecification initialClusterSize(int initialClusterSize);

    QueueSpecification specification();
  }

  enum QuorumQueueDeadLetterStrategy {
    AT_MOST_ONCE("at-most-once"),
    AT_LEAST_ONCE("at-least-once");

    private final String strategy;

    QuorumQueueDeadLetterStrategy(String strategy) {
      this.strategy = strategy;
    }

    public String strategy() {
      return strategy;
    }
  }

  enum OverFlowStrategy {
    DROP_HEAD("drop-head"),
    REJECT_PUBLISH("reject-publish"),
    REJECT_PUBLISH_DLX("reject-publish-dlx");

    private final String strategy;

    OverFlowStrategy(String strategy) {
      this.strategy = strategy;
    }

    public String strategy() {
      return strategy;
    }
  }

  enum QueueType {
    QUORUM,
    CLASSIC,
    STREAM
  }

  enum QueueLeaderLocator {
    CLIENT_LOCAL("client-local"),
    BALANCED("balanced");

    private final String locator;

    QueueLeaderLocator(String locator) {
      this.locator = locator;
    }

    public String locator() {
      return locator;
    }
  }

  enum ClassicQueueMode {
    DEFAULT,
    LAZY
  }

  enum ClassicQueueVersion {
    V1(1),
    V2(2);

    private final int version;

    ClassicQueueVersion(int version) {
      this.version = version;
    }

    public int version() {
      return version;
    }
  }

  interface QueueDeletion {

    void delete(String name);
  }
}
