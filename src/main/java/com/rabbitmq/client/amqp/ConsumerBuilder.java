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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.apache.qpid.protonj2.types.*;

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
     * Filter values for stream filtering.
     *
     * <p>This a different filtering mechanism from AMQP filter expressions. Both mechanisms can be
     * used together.
     *
     * @param values filter values
     * @return stream options
     * @see <a href="https://www.rabbitmq.com/docs/streams#filtering">Stream Filtering</a>
     * @see #filter()
     */
    StreamOptions filterValues(String... values);

    /**
     * Whether messages without a filter value should be sent.
     *
     * <p>Default is <code>false</code> (messages without a filter value are not sent).
     *
     * <p>This a different filtering mechanism from AMQP filter expressions. Both mechanisms can be
     * used together.
     *
     * @param matchUnfiltered true to send messages without a filter value
     * @return stream options
     * @see #filter()
     */
    StreamOptions filterMatchUnfiltered(boolean matchUnfiltered);

    /**
     * Options for AMQP filter expressions.
     *
     * <p>Requires RabbitMQ 4.1 or more.
     *
     * <p>This a different filtering mechanism from stream filtering. Both mechanisms can be used
     * together.
     *
     * @return the filter options
     * @see #filterValues(String...)
     * @see #filterMatchUnfiltered(boolean)
     */
    StreamFilterOptions filter();

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
   * Filter options for support of AMQP filter expressions.
   *
   * <p>AMQP filter expressions are supported only with streams.
   *
   * <p>This a different filtering mechanism from stream filtering. Both mechanisms can be used
   * together.
   *
   * <p>Requires RabbitMQ 4.1 or more.
   *
   * @param <T> type of the object returned by methods
   * @see <a
   *     href="https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=66227">AMQP
   *     Filter Expressions</a>
   */
  interface FilterOptions<T> {

    /**
     * Filter on message ID.
     *
     * @param id message ID
     * @return type-parameter object
     */
    T messageId(Object id);

    /**
     * Filter on message ID.
     *
     * @param id message ID
     * @return type-parameter object
     */
    T messageId(String id);

    /**
     * Filter on message ID.
     *
     * @param id message ID
     * @return type-parameter object
     */
    T messageId(long id);

    /**
     * Filter on message ID.
     *
     * @param id message ID
     * @return type-parameter object
     */
    T messageId(byte[] id);

    /**
     * Filter on message ID.
     *
     * @param id message ID
     * @return type-parameter object
     */
    T messageId(UUID id);

    /**
     * Filter on correlation ID.
     *
     * @param correlationId correlation ID
     * @return type-parameter object
     */
    T correlationId(Object correlationId);

    /**
     * Filter on correlation ID.
     *
     * @param correlationId correlation ID
     * @return type-parameter object
     */
    T correlationId(String correlationId);

    /**
     * Filter on correlation ID.
     *
     * @param correlationId correlation ID
     * @return type-parameter object
     */
    T correlationId(long correlationId);

    /**
     * Filter on correlation ID.
     *
     * @param correlationId correlation ID
     * @return type-parameter object
     */
    T correlationId(byte[] correlationId);

    /**
     * Filter on correlation ID.
     *
     * @param correlationId correlation ID
     * @return type-parameter object
     */
    T correlationId(UUID correlationId);

    /**
     * Filter on user ID.
     *
     * @param userId user ID
     * @return type-parameter object
     */
    T userId(byte[] userId);

    /**
     * Filter on to field.
     *
     * @param to to
     * @return type-parameter object
     */
    T to(String to);

    /**
     * Filter on subject field.
     *
     * @param subject subject
     * @return type-parameter object
     */
    T subject(String subject);

    /**
     * Filter on reply-to field.
     *
     * @param replyTo reply-to
     * @return type-parameter object
     */
    T replyTo(String replyTo);

    /**
     * Filter on content-type field.
     *
     * @param contentType content-type
     * @return type-parameter object
     */
    T contentType(String contentType);

    /**
     * Filter on content-encoding field.
     *
     * @param contentEncoding content-encoding
     * @return type-parameter object
     */
    T contentEncoding(String contentEncoding);

    /**
     * Filter on absolute expiry time field.
     *
     * @param absoluteExpiryTime absolute expiry time
     * @return type-parameter object
     */
    T absoluteExpiryTime(long absoluteExpiryTime);

    /**
     * Filter on creation time field.
     *
     * @param creationTime creation time
     * @return type-parameter object
     */
    T creationTime(long creationTime);

    /**
     * Filter on group ID.
     *
     * @param groupId group ID
     * @return type-parameter object
     */
    T groupId(String groupId);

    /**
     * Filter on group sequence.
     *
     * @param groupSequence group sequence
     * @return type-parameter object
     */
    T groupSequence(int groupSequence);

    /**
     * Filter on reply-to group.
     *
     * @param groupId group ID
     * @return type-parameter object
     */
    T replyToGroupId(String groupId);

    /**
     * Filter on boolean application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, boolean value);

    /**
     * Filter on byte application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, byte value);

    /**
     * Filter on short application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, short value);

    /**
     * Filter on integer application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, int value);

    /**
     * Filter on long application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, long value);

    /**
     * Filter on unsigned byte application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T propertyUnsigned(String key, byte value);

    /**
     * Filter on unsigned short application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T propertyUnsigned(String key, short value);

    /**
     * Filter on unsigned integer application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T propertyUnsigned(String key, int value);

    /**
     * Filter on unsigned long application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T propertyUnsigned(String key, long value);

    /**
     * Filter on float application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, float value);

    /**
     * Filter on double application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, double value);

    /**
     * Filter on 32-bit decimal number application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T propertyDecimal32(String key, BigDecimal value);

    /**
     * Filter on 64-bit decimal number application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T propertyDecimal64(String key, BigDecimal value);

    /**
     * Filter on 128-bit decimal number application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T propertyDecimal128(String key, BigDecimal value);

    /**
     * Filter on character application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, char value);

    /**
     * Filter on timestamp application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T propertyTimestamp(String key, long value);

    /**
     * Filter on UUID application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, UUID value);

    /**
     * Filter on byte array application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, byte[] value);

    /**
     * Filter on string application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T property(String key, String value);

    /**
     * Filter on symbol application property.
     *
     * @param key application property key
     * @param value application property value
     * @return type-parameter object
     */
    T propertySymbol(String key, String value);
  }

  /**
   * Filter options for support of AMQP filter expressions.
   *
   * <p>Specialized {@link FilterOptions} in the context of the configuration of a stream consumer.
   */
  interface StreamFilterOptions extends FilterOptions<StreamFilterOptions> {

    /**
     * Go back to the stream options.
     *
     * @return the stream options
     */
    StreamOptions stream();
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
