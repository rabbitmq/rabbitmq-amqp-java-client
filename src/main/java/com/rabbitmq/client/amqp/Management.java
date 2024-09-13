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
package com.rabbitmq.client.amqp;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * API to manage AMQ 0.9.1 topology (exchanges, queues, and bindings).
 *
 * @see <a href="https://www.rabbitmq.com/tutorials/amqp-concepts">AMQ 0.9.1 Model</a>
 */
public interface Management extends AutoCloseable {

  /**
   * Start queue specification.
   *
   * @return the queue specification
   */
  QueueSpecification queue();

  /**
   * Start queue specification.
   *
   * @param name the name of the queue
   * @return the queue specification
   */
  QueueSpecification queue(String name);

  /**
   * Query information on a queue.
   *
   * @param name the name of the queue
   * @return the queue information
   */
  QueueInfo queueInfo(String name);

  /**
   * Delete a queue.
   *
   * @return the queue deletion
   */
  QueueDeletion queueDeletion();

  /**
   * Start exchange specification.
   *
   * @return the exchange specification
   */
  ExchangeSpecification exchange();

  /**
   * Start exchange specification.
   *
   * @param name the name of the exchange
   * @return the exchange specification
   */
  ExchangeSpecification exchange(String name);

  /**
   * Delete an exchange.
   *
   * @return the exchange deletion
   */
  ExchangeDeletion exchangeDeletion();

  /**
   * Start binding specification.
   *
   * @return the binding definition
   */
  BindingSpecification binding();

  /**
   * Start unbinding specification.
   *
   * @return the unbinding specification
   */
  UnbindSpecification unbind();

  /** Close the management instance and release its resources. */
  @Override
  void close();

  /**
   * Specification to create a queue.
   *
   * @see <a href="https://www.rabbitmq.com/docs/queues">Queues</a>
   */
  interface QueueSpecification {

    /**
     * The name of the queue.
     *
     * <p>The library will generate a random name if no name is specified.
     *
     * @param name name
     * @return the queue specification
     */
    QueueSpecification name(String name);

    /**
     * Whether the queue is exclusive or not.
     *
     * @param exclusive exclusive flag
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/queues#properties">Queue Properties</a>
     */
    QueueSpecification exclusive(boolean exclusive);

    /**
     * Whether the queue get automatically deleted when its last consumer unsubscribes.
     *
     * @param autoDelete auto-delete flag
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/queues#properties">Queue Properties</a>
     */
    QueueSpecification autoDelete(boolean autoDelete);

    /**
     * The type of the queue.
     *
     * @param type queue type
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/queues#optional-arguments">Queue Arguments</a>
     */
    QueueSpecification type(QueueType type);

    /**
     * The dead letter exchange.
     *
     * @param dlx dead letter exchange
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/dlx">Dead Letter Exchange</a>
     */
    QueueSpecification deadLetterExchange(String dlx);

    /**
     * The dead letter routing key.
     *
     * @param dlrk dead letter exchange
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/dlx">Dead Letter Exchange</a>
     */
    QueueSpecification deadLetterRoutingKey(String dlrk);

    /**
     * The overflow strategy (as a string).
     *
     * @param overflow the overflow strategy
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/queues#ttl-and-limits">TTL and Length Limit</a>
     */
    QueueSpecification overflowStrategy(String overflow);

    /**
     * The overflow strategy.
     *
     * @param overflow overflow strategy
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/queues#ttl-and-limits">TTL and Length Limit</a>
     * @see <a href="https://www.rabbitmq.com/docs/maxlength#overflow-behaviour">Overflow
     *     Behavior</a>
     */
    QueueSpecification overflowStrategy(OverFlowStrategy overflow);

    /**
     * Set TTL for a queue.
     *
     * @param expiration expiration
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/ttl#queue-ttl">Queue TTL</a>
     */
    QueueSpecification expires(Duration expiration);

    /**
     * Maximum length for a queue.
     *
     * @param maxLength maximum length
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/maxlength">Queue Length Limit</a>
     */
    QueueSpecification maxLength(long maxLength);

    /**
     * Maximum length in bytes for a queue.
     *
     * @param maxLengthBytes maximum length in bytes
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/maxlength">Queue Length Limit</a>
     */
    QueueSpecification maxLengthBytes(ByteCapacity maxLengthBytes);

    /**
     * Activate "single active consumer" on the queue.
     *
     * @param singleActiveConsumer activation flag
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/consumers#single-active-consumer">Single Active
     *     Consumer</a>
     */
    QueueSpecification singleActiveConsumer(boolean singleActiveConsumer);

    /**
     * Set the message TTL for the queue.
     *
     * @param ttl message TTL
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/ttl#message-ttl-using-x-args">Message TTL for
     *     Queues</a>
     */
    QueueSpecification messageTtl(Duration ttl);

    /**
     * Leader-locator strategy (replica placement) for the queue.
     *
     * @param locator locator strategy
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/clustering#replica-placement">Replica
     *     Placement</a>
     */
    QueueSpecification leaderLocator(QueueLeaderLocator locator);

    /**
     * Set the type to {@link QueueType#QUORUM} and return quorum-queue-specific specification.
     *
     * @return quorum queue specification
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues">Quorum Queues</a>
     */
    QuorumQueueSpecification quorum();

    /**
     * Set the type to {@link QueueType#CLASSIC} and return classic-queue-specific specification.
     *
     * @return classic queue specification
     * @see <a href="https://www.rabbitmq.com/docs/classic-queues">Classic Queues</a>
     */
    ClassicQueueSpecification classic();

    /**
     * Set the type to {@link QueueType#STREAM} and return stream-specific specification.
     *
     * @return stream specification
     * @see <a href="https://www.rabbitmq.com/docs/streams">Streams</a>
     */
    StreamSpecification stream();

    /**
     * Set a queue argument.
     *
     * @param key argument name
     * @param value argument value
     * @return the queue specification
     * @see <a href="https://www.rabbitmq.com/docs/queues#optional-arguments">Queue Arguments</a>
     */
    QueueSpecification argument(String key, Object value);

    /**
     * Declare the queue.
     *
     * @return information on the queue
     */
    QueueInfo declare();
  }

  /**
   * Specification of a quorum queue.
   *
   * @see <a href="https://www.rabbitmq.com/docs/quorum-queues">Quorum Queues</a>
   */
  interface QuorumQueueSpecification {

    /**
     * The dead letter strategy (as a string).
     *
     * @param strategy dead letter strategy
     * @return quorum queue specification
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#dead-lettering">Quorum Queue Dead
     *     Lettering</a>
     */
    QuorumQueueSpecification deadLetterStrategy(String strategy);

    /**
     * The dead letter strategy.
     *
     * @param strategy dead letter strategy
     * @return quorum queue specification
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#dead-lettering">Quorum Queue Dead
     *     Lettering</a>
     */
    QuorumQueueSpecification deadLetterStrategy(QuorumQueueDeadLetterStrategy strategy);

    /**
     * Set the delivery limit (for poison message handling).
     *
     * @param limit delivery limit
     * @return quorum queue specification
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#poison-message-handling">Poison
     *     Message Handling</a>
     */
    QuorumQueueSpecification deliveryLimit(int limit);

    /**
     * Set the number of quorum queue members.
     *
     * @param size group size
     * @return quorum queue specification
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#replication-factor">Initial
     *     Replication Factor</a>
     */
    QuorumQueueSpecification quorumInitialGroupSize(int size);

    /**
     * Go back to the queue specification.
     *
     * @return queue specification
     */
    QueueSpecification queue();
  }

  /**
   * Specification of a classic queue.
   *
   * @see <a href="https://www.rabbitmq.com/docs/classic-queues">Classic Queues</a>
   */
  interface ClassicQueueSpecification {

    /**
     * Declare a priority queue and set the maximum priority.
     *
     * @param maxPriority maximum priority
     * @return classic queue specification
     * @see <a href="https://www.rabbitmq.com/docs/priority">Priority Queue</a>
     */
    ClassicQueueSpecification maxPriority(int maxPriority);

    /**
     * Set the version of the classic queue implementation.
     *
     * @param version implementation version
     * @return classic queue specification
     * @see <a
     *     href="https://www.rabbitmq.com/blog/2023/05/17/rabbitmq-3.12-performance-improvements#classic-queues-massively-improved-classic-queues-v2-cqv2">Classic
     *     Queue Version 2</a>
     * @see <a href="https://www.rabbitmq.com/docs/persistence-conf#queue-version">Classic Queue
     *     Version</a>
     */
    ClassicQueueSpecification version(ClassicQueueVersion version);

    /**
     * Go back to the queue specification.
     *
     * @return queue specification
     */
    QueueSpecification queue();
  }

  /**
   * Specification of a stream.
   *
   * @see <a href="https://www.rabbitmq.com/docs/streams">Streams</a>
   */
  interface StreamSpecification {

    /**
     * Set the maximum age of a stream before it gets truncated.
     *
     * @param maxAge maximum age
     * @return the stream specification
     * @see <a href="https://www.rabbitmq.com/docs/streams#retention">Data Retention</a>
     */
    StreamSpecification maxAge(Duration maxAge);

    /**
     * Set the maximum size for the stream segment files.
     *
     * @param maxSegmentSize
     * @return the stream specification
     * @see <a href="https://www.rabbitmq.com/docs/streams#declaring">Declaring a Stream</a>
     */
    StreamSpecification maxSegmentSizeBytes(ByteCapacity maxSegmentSize);

    /**
     * Set the number of nodes the initial stream cluster should span.
     *
     * @param initialClusterSize initial number of nodes
     * @return the stream specification
     * @see <a href="https://www.rabbitmq.com/docs/streams#replication-factor">Initial Replication
     *     Factor</a>
     */
    StreamSpecification initialClusterSize(int initialClusterSize);

    /**
     * Go back to the queue specification.
     *
     * @return queue specification
     */
    QueueSpecification queue();
  }

  /**
   * Dead letter strategy for quorum queues.
   *
   * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#dead-lettering">Quorum Queue Dead
   *     Lettering</a>
   * @see <a href="https://www.rabbitmq.com/docs/dlx#safety">Dead Lettering Safety</a>
   */
  enum QuorumQueueDeadLetterStrategy {
    /**
     * At-most-once strategy, dead-lettered messages can be lost in transit.
     *
     * <p>The default strategy.
     */
    AT_MOST_ONCE("at-most-once"),
    /**
     * At-least-once strategy, guarantees for the message transfer between queues (with some
     * caveats).
     *
     * @see <a
     *     href="https://www.rabbitmq.com/docs/quorum-queues#activating-at-least-once-dead-lettering">At-least-once
     *     Dead Lettering</a>
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#limitations">Limitions</a>
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#caveats">Caveats</a>
     */
    AT_LEAST_ONCE("at-least-once");

    private final String strategy;

    QuorumQueueDeadLetterStrategy(String strategy) {
      this.strategy = strategy;
    }

    public String strategy() {
      return strategy;
    }
  }

  /**
   * Overflow strategy (when a queue reaches its maximum length limit).
   *
   * @see <a href="https://www.rabbitmq.com/docs/maxlength#overflow-behaviour">Overflow Behavior</a>
   */
  enum OverFlowStrategy {
    /** Drop the messages at the head of the queue. The default strategy. */
    DROP_HEAD("drop-head"),
    /** Discard the most recent published messages. */
    REJECT_PUBLISH("reject-publish"),
    /** Discard the most recent published messages and dead-letter them. */
    REJECT_PUBLISH_DLX("reject-publish-dlx");

    private final String strategy;

    OverFlowStrategy(String strategy) {
      this.strategy = strategy;
    }

    public String strategy() {
      return strategy;
    }
  }

  /** Queue Type. */
  enum QueueType {
    /**
     * Quorum queue.
     *
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues">Quorum Queues</a>
     */
    QUORUM,
    /**
     * Classic queue.
     *
     * @see <a href="https://www.rabbitmq.com/docs/classic-queues">Classic Queues</a>
     */
    CLASSIC,
    /**
     * Stream.
     *
     * @see <a href="https://www.rabbitmq.com/docs/streams">Streams</a>
     */
    STREAM
  }

  /**
   * Queue leader locator.
   *
   * @see <a href="https://www.rabbitmq.com/docs/clustering#replica-placement">Replica Placement</a>
   */
  enum QueueLeaderLocator {
    /**
     * Pick the node the client is connected to.
     *
     * <p>The default.
     */
    CLIENT_LOCAL("client-local"),
    /**
     * Takes into account the number of queues/leaders already running on each node in the cluster.
     */
    BALANCED("balanced");

    private final String locator;

    QueueLeaderLocator(String locator) {
      this.locator = locator;
    }

    public String locator() {
      return locator;
    }
  }

  /**
   * Classic queue version.
   *
   * @see <a
   *     href="https://www.rabbitmq.com/blog/2023/05/17/rabbitmq-3.12-performance-improvements#classic-queues-massively-improved-classic-queues-v2-cqv2">Classic
   *     Queue Version 2</a>
   * @see <a href="https://www.rabbitmq.com/docs/persistence-conf#queue-version">Classic Queue
   *     Version</a>
   */
  enum ClassicQueueVersion {
    /** Classic queue version 1. */
    V1(1),
    /** Classic queue version 2. */
    V2(2);

    private final int version;

    ClassicQueueVersion(int version) {
      this.version = version;
    }

    public int version() {
      return version;
    }
  }

  /** Queue deletion. */
  interface QueueDeletion {

    /**
     * Delete the queue.
     *
     * @param name queue name
     */
    void delete(String name);
  }

  /**
   * Specification of an exchange.
   *
   * @see <a href="https://www.rabbitmq.com/tutorials/amqp-concepts#exchanges">Exchanges</a>
   */
  interface ExchangeSpecification {

    /**
     * The name of the exchange.
     *
     * @param name exchange name
     * @return the exchange specification
     */
    ExchangeSpecification name(String name);

    /**
     * Whether the exchange is deleted when last queue is unbound from it.
     *
     * @param autoDelete auto-delete flag.
     * @return the exchange specification
     */
    ExchangeSpecification autoDelete(boolean autoDelete);

    /**
     * Type of the exchange.
     *
     * @param type exchange type
     * @return the exchange specification
     */
    ExchangeSpecification type(ExchangeType type);

    /**
     * Type of the exchange (as a string, for non-built-in exchange types).
     *
     * @param type exchange type
     * @return the exchange specification
     */
    ExchangeSpecification type(String type);

    /**
     * Exchange argument.
     *
     * @param key argument name
     * @param value argument value
     * @return the exchange specification
     */
    ExchangeSpecification argument(String key, Object value);

    /** Declare the exchange. */
    void declare();
  }

  /**
   * Exchange type.
   *
   * @see <a href="https://www.rabbitmq.com/tutorials/amqp-concepts#exchanges">Exchange Types</a>
   */
  enum ExchangeType {
    /**
     * Direct exchange type.
     *
     * @see <a href="https://www.rabbitmq.com/tutorials/amqp-concepts#exchange-direct">Direct
     *     Exchange Type</a>
     */
    DIRECT,
    /**
     * Fanout exchange type.
     *
     * @see <a href="https://www.rabbitmq.com/tutorials/amqp-concepts#exchange-fanout">Fanout
     *     exchange type</a>
     */
    FANOUT,
    /**
     * Topic exchange type.
     *
     * @see <a href="https://www.rabbitmq.com/tutorials/amqp-concepts#exchange-topic">Topic Exchange
     *     Type</a>
     */
    TOPIC,
    /**
     * Headers Exchange Type.
     *
     * @see <a href="https://www.rabbitmq.com/tutorials/amqp-concepts#exchange-headers">Headers
     *     Exchange Type</a>
     */
    HEADERS
  }

  /** Exchange deletion. */
  interface ExchangeDeletion {

    /**
     * Delete the exchange.
     *
     * @param name exchange name
     */
    void delete(String name);
  }

  /**
   * Specification of a binding.
   *
   * @see <a href="https://www.rabbitmq.com/tutorials/amqp-concepts#bindings">Bindings</a>
   * @see <a href="https://www.rabbitmq.com/docs/e2e">Exchange-to-exchange binding</a>
   */
  interface BindingSpecification {

    /**
     * The source exchange.
     *
     * @param exchange exchange
     * @return the binding specification
     */
    BindingSpecification sourceExchange(String exchange);

    /**
     * The destination queue.
     *
     * @param queue queue
     * @return the binding specification
     */
    BindingSpecification destinationQueue(String queue);

    /**
     * The destination exchange.
     *
     * @param exchange exchange
     * @return the binding specification
     * @see <a href="https://www.rabbitmq.com/docs/e2e">Exchange-to-exchange binding</a>
     */
    BindingSpecification destinationExchange(String exchange);

    /**
     * The binding key.
     *
     * @param key binding key
     * @return the binding specification
     */
    BindingSpecification key(String key);

    /**
     * Binding argument.
     *
     * @param key argument name
     * @param value argument value
     * @return the binding specification
     */
    BindingSpecification argument(String key, Object value);

    /**
     * Binding arguments.
     *
     * @param arguments arguments
     * @return the binding specification
     */
    BindingSpecification arguments(Map<String, Object> arguments);

    /** Create the binding. */
    void bind();
  }

  /** Unbind specification. */
  interface UnbindSpecification {

    /**
     * The source exchange.
     *
     * @param exchange exchange
     * @return the unbind specification
     */
    UnbindSpecification sourceExchange(String exchange);

    /**
     * The destination queue.
     *
     * @param queue queue
     * @return the unbind specification
     */
    UnbindSpecification destinationQueue(String queue);

    /**
     * The destination exchange.
     *
     * @param exchange
     * @return the unbind specification
     * @see <a href="https://www.rabbitmq.com/docs/e2e">Exchange-to-exchange binding</a>
     */
    UnbindSpecification destinationExchange(String exchange);

    /**
     * The binding key.
     *
     * @param key binding key
     * @return the unbind specification
     */
    UnbindSpecification key(String key);

    /**
     * A binding argument.
     *
     * @param key argument name
     * @param value argument value
     * @return the unbind specification
     */
    UnbindSpecification argument(String key, Object value);

    /**
     * Binding arguments.
     *
     * @param arguments arguments
     * @return the unbind specification
     */
    UnbindSpecification arguments(Map<String, Object> arguments);

    /** Delete the binding. */
    void unbind();
  }

  /**
   * Queue information.
   *
   * @see <a href="https://www.rabbitmq.com/tutorials/amqp-concepts#queues">Queues</a>
   * @see <a href="https://www.rabbitmq.com/docs/queues#properties">Queue Properties</a>
   */
  interface QueueInfo {

    /**
     * The name of the queue.
     *
     * @return queue name
     */
    String name();

    /**
     * Whether the queue is durable (will survive a server restart).
     *
     * @return the durable flag
     */
    boolean durable();

    /**
     * Whether the queue is deleted when last consumer unsubscribes.
     *
     * @return the auto-delete flag
     */
    boolean autoDelete();

    /**
     * Whether the queue is used by only one connection and will be deleted when that connection
     * closes.
     *
     * @return the exclusive flag
     */
    boolean exclusive();

    /**
     * The type of the queue.
     *
     * @return the queue type
     */
    QueueType type();

    /**
     * The arguments of the queue.
     *
     * @return the queue arguments
     */
    Map<String, Object> arguments();

    /**
     * The node the leader of the queue is on.
     *
     * @return the node of the queue leader
     */
    String leader();

    /**
     * The nodes the queue has replicas (members) on.
     *
     * @return the nodes of the queue replicas (members)
     */
    List<String> replicas();

    /**
     * The number of messages in the queue.
     *
     * @return the queue message count
     */
    long messageCount();

    /**
     * The number of consumers the queue has.
     *
     * @return the queue consumer count
     */
    int consumerCount();
  }
}
