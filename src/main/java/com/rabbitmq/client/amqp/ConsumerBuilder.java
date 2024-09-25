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

import java.time.Instant;

/** API to configure and create a {@link Consumer}. */
public interface ConsumerBuilder {

  /**
   * The queue to consume from.
   *
   * @param queue queue
   * @return this builder instance
   */
  ConsumerBuilder queue(String queue);

  /**
   * The callback for inbound messages.
   *
   * @param handler callback
   * @return this builder instance
   */
  ConsumerBuilder messageHandler(Consumer.MessageHandler handler);

  /**
   * The initial number credits to grant to the AMQP receiver.
   *
   * <p>The default is 100.
   *
   * @param initialCredits number of initial credits
   * @return this buidler instance
   */
  ConsumerBuilder initialCredits(int initialCredits);

  /**
   * The consumer priority.
   *
   * @param priority consumer priority
   * @return this builder instance
   * @see <a href="https://www.rabbitmq.com/docs/consumer-priority">Consumer Priorities</a>
   */
  ConsumerBuilder priority(int priority);

  /**
   * Add {@link com.rabbitmq.client.amqp.Resource.StateListener}s to the consumer.
   *
   * @param listeners listeners
   * @return this builder instance
   */
  ConsumerBuilder listeners(Resource.StateListener... listeners);

  /**
   * Options for a consumer consuming from a stream.
   *
   * @return stream options
   * @see <a href="https://www.rabbitmq.com/docs/streams">Streams</a>
   * @see <a href="https://www.rabbitmq.com/docs/streams#consuming">Stream Consumers</a>
   */
  StreamOptions stream();

  /**
   * Set a listener to customize the subscription before the consumer is created (or recovered).
   *
   * <p>This callback is available for stream consumers.
   *
   * @param subscriptionListener subscription listener
   * @return this builder instance
   * @see SubscriptionListener
   */
  ConsumerBuilder subscriptionListener(SubscriptionListener subscriptionListener);

  /**
   * Build the consumer.
   *
   * @return the configured consumer instance
   */
  Consumer build();

  /**
   * Options for a consumer consuming from a stream.
   *
   * @see <a href="https://www.rabbitmq.com/docs/streams">Streams</a>
   * @see <a href="https://www.rabbitmq.com/docs/streams#consuming">Stream Consumers</a>
   */
  interface StreamOptions {

    /**
     * The offset to start consuming from.
     *
     * @param offset offset
     * @return stream options
     */
    StreamOptions offset(long offset);

    /**
     * A point in time to start consuming from.
     *
     * <p>Be aware consumers can receive messages published a bit before the specified timestamp.
     *
     * @param timestamp the timestamp
     * @return stream options
     */
    StreamOptions offset(Instant timestamp);

    /**
     * The offset to start consuming from.
     *
     * @param specification offset specification
     * @return stream options
     * @see StreamOffsetSpecification
     */
    StreamOptions offset(StreamOffsetSpecification specification);

    /**
     * The offset to start consuming from as an interval string value.
     *
     * <p>Valid units are Y, M, D, h, m, s. Examples: <code>7D</code> (7 days), <code>12h</code> (12
     * hours).
     *
     * @param interval the interval
     * @return stream options
     * @see <a href="https://www.rabbitmq.com/docs/streams#retention">Interval Syntax</a>
     */
    StreamOptions offset(String interval);

    /**
     * Filter values.
     *
     * @param values filter values
     * @return stream options
     * @see <a href="https://www.rabbitmq.com/docs/streams#filtering">Stream Filtering</a>
     */
    StreamOptions filterValues(String... values);

    /**
     * Whether messages without a filter value should be sent.
     *
     * <p>Default is <code>false</code> (messages without a filter value are not sent).
     *
     * @param matchUnfiltered true to send messages without a filter value
     * @return stream options
     */
    StreamOptions filterMatchUnfiltered(boolean matchUnfiltered);

    /**
     * Return the consumer builder.
     *
     * @return the consumer builder
     */
    ConsumerBuilder builder();
  }

  /** Offset specification to start consuming from. */
  enum StreamOffsetSpecification {
    /** Beginning of the stream. */
    FIRST,
    /** Last chunk of the stream. */
    LAST,
    /** Very end of the stream (new chunks). */
    NEXT
  }

  /**
   * Callback to modify a consumer subscription before the link creation.
   *
   * <p>This allows looking up the last processed offset for a stream consumer and attaching to this
   * offset.
   */
  interface SubscriptionListener {

    /**
     * Pre-subscription callback.
     *
     * <p>It is called before the link is created but also every time it recovers, e.g. after a
     * connection failure.
     *
     * <p>Configuration set with {@link Context#streamOptions()} overrides the one set with {@link
     * ConsumerBuilder#stream()}.
     *
     * @param context subscription context
     */
    void preSubscribe(Context context);

    /** Subscription context. */
    interface Context {

      /**
       * Stream options, to set the offset to start consuming from.
       *
       * <p>Only the {@link StreamOptions} are accessible, the {@link StreamOptions#builder()}
       * method returns <code>null</code>
       *
       * @return the stream options
       * @see StreamOptions
       */
      ConsumerBuilder.StreamOptions streamOptions();
    }
  }
}
