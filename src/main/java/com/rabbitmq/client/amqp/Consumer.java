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
import java.time.Instant;
import java.util.Map;

/**
 * API to consume messages from a RabbitMQ queue.
 *
 * <p>Instances are configured and created with a {@link ConsumerBuilder}.
 *
 * @see Connection#consumerBuilder()
 * @see ConsumerBuilder
 */
public interface Consumer extends AutoCloseable, Resource {

  /**
   * Pause the consumer to stop receiving messages.
   *
   * <p>Messages already in flight when this method is called may still be delivered after it
   * returns.
   *
   * @throws AmqpException if the consumer is not open
   */
  void pause();

  /**
   * Return the number of unsettled messages.
   *
   * @return unsettled message count
   */
  long unsettledMessageCount();

  /**
   * Request to receive messages again.
   *
   * @throws AmqpException if the consumer is not open
   */
  void unpause();

  /** Close the consumer with its resources. */
  @Override
  void close();

  /**
   * Claim messages previously deferred with a {@link Context#delayedRetry(Duration, boolean,
   * String)} token, so they are redelivered ahead of their scheduled delivery time.
   *
   * <p>A token may have been used for more than one message, in which case claiming it can deliver
   * more than one message, oldest first. Claiming an unknown token has no effect: it is silently
   * ignored by the broker. A claim does not grant extra credit: claimed messages are delivered as
   * the consumer's existing credit allows.
   *
   * <p>Claims do not survive connection recovery: the application must claim tokens again after the
   * consumer recovers.
   *
   * <p><b>Only quorum queues support deferral tokens.</b>
   *
   * <p><b>Requires RabbitMQ 4.4 or more.</b>
   *
   * @param tokens the tokens to claim
   * @throws AmqpException if the consumer is not open
   * @throws AmqpException if the queue this consumer is attached to does not support deferral
   *     tokens
   * @see Context#delayedRetry(Duration, boolean, String)
   * @see Context#delayedRetry(Instant, boolean, String)
   * @since 1.6.0
   */
  void claimDeferred(String... tokens);

  /** Contract to process a message. */
  @FunctionalInterface
  interface MessageHandler {

    /**
     * Process a message
     *
     * @param context message context
     * @param message message
     */
    void handle(Context context, Message message);
  }

  /** Context for message processing. */
  interface Context {

    /**
     * Accept the message (AMQP 1.0 <code>accepted</code> outcome).
     *
     * <p>This means the message has been processed and the broker can delete it.
     */
    void accept();

    /**
     * Discard the message (AMQP 1.0 <code>rejected</code> outcome).
     *
     * <p>This means the message cannot be processed because it is invalid, the broker can drop it
     * or dead-letter it if it is configured.
     */
    void discard();

    /**
     * Discard the message with annotations to combine with the existing message annotations.
     *
     * <p>This means the message cannot be processed because it is invalid, the broker can drop it
     * or dead-letter it if it is configured.
     *
     * <p>Custom application-specific annotation keys should start with the <code>x-opt-</code>
     * prefix so they are safely ignored if not understood. Keys starting with <code>x-</code> (but
     * not <code>x-opt-</code>) are reserved for broker-specific features, and unrecognized keys
     * will cause a protocol error.
     *
     * <p>This maps to the AMQP 1.0 <code>
     * modified{delivery-failed = true, undeliverable-here = true}</code> outcome.
     *
     * <p><b>Only quorum queues support the modification of message annotations with the <code>
     * modified</code> outcome.</b>
     *
     * @param annotations message annotations to combine with existing ones
     * @see <a
     *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
     *     1.0 <code>modified</code> outcome</a>
     * @see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support
     *     in RabbitMQ</a>
     */
    void discard(Map<String, Object> annotations);

    /**
     * Requeue the message (AMQP 1.0 <code>released</code> outcome).
     *
     * <p>This means the message has not been processed and the broker can requeue it and deliver it
     * to the same or a different consumer.
     */
    void requeue();

    /**
     * Requeue the message with annotations to combine with the existing message annotations.
     *
     * <p>This means the message has not been processed and the broker can requeue it and deliver it
     * to the same or a different consumer.
     *
     * <p>Custom application-specific annotation keys should start with the <code>x-opt-</code>
     * prefix so they are safely ignored if not understood. Keys starting with <code>x-</code> (but
     * not <code>x-opt-</code>) are reserved for broker-specific features, and unrecognized keys
     * will cause a protocol error.
     *
     * <p>This maps to the AMQP 1.0 <code>
     * modified{delivery-failed = false, undeliverable-here = false}</code> outcome.
     *
     * <p><b>Only quorum queues support the modification of message annotations with the <code>
     * modified</code> outcome.</b>
     *
     * @param annotations message annotations to combine with existing ones
     * @see <a
     *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
     *     1.0 <code>modified</code> outcome</a>
     * @see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support
     *     in RabbitMQ</a>
     */
    void requeue(Map<String, Object> annotations);

    /**
     * Requeue the message with annotations to combine with the existing message annotations.
     *
     * <p>This means the message has not been processed and the broker can requeue it and deliver it
     * to the same or a different consumer.
     *
     * <p>Custom application-specific annotation keys should start with the <code>x-opt-</code>
     * prefix so they are safely ignored if not understood. Keys starting with <code>x-</code> (but
     * not <code>x-opt-</code>) are reserved for broker-specific features, and unrecognized keys
     * will cause a protocol error.
     *
     * <p>This maps to the AMQP 1.0 <code>
     * modified{delivery-failed = deliveryFailed, undeliverable-here = false}</code> outcome.
     *
     * <p><b>Only quorum queues support the modification of message annotations with the <code>
     * modified</code> outcome.</b>
     *
     * @param annotations message annotations to combine with existing ones
     * @param deliveryFailed if true, the delivery count of the message is incremented
     * @see <a
     *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
     *     1.0 <code>modified</code> outcome</a>
     * @see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support
     *     in RabbitMQ</a>
     */
    void requeue(Map<String, Object> annotations, boolean deliveryFailed);

    /**
     * Requeue the message for redelivery after the specified delay.
     *
     * <p>This maps to the AMQP 1.0 <code>
     * modified{delivery-failed = false, undeliverable-here = false}</code> outcome with the <code>
     * x-opt-delivery-time</code> annotation set to <code>now + delay</code>.
     *
     * <p><b>Only quorum queues support the modification of message annotations with the <code>
     * modified</code> outcome.</b>
     *
     * @param delay delivery delay from now
     * @see <a
     *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
     *     1.0 <code>modified</code> outcome</a>
     * @see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support
     *     in RabbitMQ</a>
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#delayed-retry">Delayed Retry in
     *     RabbitMQ</a>
     */
    void delayedRetry(Duration delay);

    /**
     * Requeue the message for redelivery after the specified delay.
     *
     * <p>This maps to the AMQP 1.0 <code>
     * modified{delivery-failed = false, undeliverable-here = false}</code> outcome with the <code>
     * x-opt-delivery-time</code> annotation set to <code>now + delay</code>.
     *
     * <p><b>Only quorum queues support the modification of message annotations with the <code>
     * modified</code> outcome.</b>
     *
     * @param delay delivery delay from now
     * @param deliveryFailed if true, the delivery count of the message is incremented
     * @see <a
     *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
     *     1.0 <code>modified</code> outcome</a>
     * @see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support
     *     in RabbitMQ</a>
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#delayed-retry">Delayed Retry in
     *     RabbitMQ</a>
     */
    void delayedRetry(Duration delay, boolean deliveryFailed);

    /**
     * Requeue the message for redelivery at the specified time.
     *
     * <p>This maps to the AMQP 1.0 <code>
     * modified{delivery-failed = false, undeliverable-here = false}</code> outcome with the <code>
     * x-opt-delivery-time</code> annotation set to the specified time.
     *
     * <p><b>Only quorum queues support the modification of message annotations with the <code>
     * modified</code> outcome.</b>
     *
     * @param deliveryTime absolute delivery time
     * @see <a
     *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
     *     1.0 <code>modified</code> outcome</a>
     * @see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support
     *     in RabbitMQ</a>
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#delayed-retry">Delayed Retry in
     *     RabbitMQ</a>
     */
    void delayedRetry(Instant deliveryTime);

    /**
     * Requeue the message for redelivery at the specified time.
     *
     * <p>This maps to the AMQP 1.0 <code>
     * modified{delivery-failed = false, undeliverable-here = false}</code> outcome with the <code>
     * x-opt-delivery-time</code> annotation set to the specified time.
     *
     * <p><b>Only quorum queues support the modification of message annotations with the <code>
     * modified</code> outcome.</b>
     *
     * @param deliveryTime absolute delivery time
     * @param deliveryFailed if true, the delivery count of the message is incremented
     * @see <a
     *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
     *     1.0 <code>modified</code> outcome</a>
     * @see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support
     *     in RabbitMQ</a>
     * @see <a href="https://www.rabbitmq.com/docs/quorum-queues#delayed-retry">Delayed Retry in
     *     RabbitMQ</a>
     */
    void delayedRetry(Instant deliveryTime, boolean deliveryFailed);

    /**
     * Requeue the message for redelivery after the specified delay, and park it under a deferral
     * token so it can also be retrieved early with {@link Consumer#claimDeferred(String...)}.
     *
     * <p>This maps to the AMQP 1.0 <code>
     * modified{delivery-failed = deliveryFailed, undeliverable-here = false}</code> outcome with
     * the <code>x-opt-delivery-time</code> annotation set to <code>now + delay</code> and the
     * <code>x-opt-deferral-token</code> annotation set to <code>deferralToken</code>.
     *
     * <p>The token is honoured only because a delivery time is also set: a deferral token cannot be
     * supplied without one, since the broker would otherwise silently ignore it. The message still
     * becomes available at its delivery time even if the token is never claimed. If the token has
     * already been used for another message, claiming it delivers both, oldest first. The token is
     * readable on the redelivered message through its <code>x-opt-deferral-token
     * </code> annotation.
     *
     * <p><b>Only quorum queues support deferral tokens.</b>
     *
     * <p><b>Requires RabbitMQ 4.4 or more.</b>
     *
     * @param delay delivery delay from now
     * @param deliveryFailed if true, the delivery count of the message is incremented
     * @param deferralToken the token the message is parked under, at most 256 UTF-8 bytes
     * @see <a
     *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
     *     1.0 <code>modified</code> outcome</a>
     * @see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support
     *     in RabbitMQ</a>
     * @see Consumer#claimDeferred(String...)
     * @since 1.6.0
     */
    void delayedRetry(Duration delay, boolean deliveryFailed, String deferralToken);

    /**
     * Requeue the message for redelivery at the specified time, and park it under a deferral token
     * so it can also be retrieved early with {@link Consumer#claimDeferred(String...)}.
     *
     * <p>This maps to the AMQP 1.0 <code>
     * modified{delivery-failed = deliveryFailed, undeliverable-here = false}</code> outcome with
     * the <code>x-opt-delivery-time</code> annotation set to the specified time and the <code>
     * x-opt-deferral-token</code> annotation set to <code>deferralToken</code>.
     *
     * <p>The token is honoured only if <code>deliveryTime</code> is in the future: a deferral token
     * cannot be supplied without one, since the broker would otherwise silently ignore it. The
     * message still becomes available at its delivery time even if the token is never claimed. If
     * the token has already been used for another message, claiming it delivers both, oldest first.
     * The token is readable on the redelivered message through its <code>
     * x-opt-deferral-token</code> annotation.
     *
     * <p><b>Only quorum queues support deferral tokens.</b>
     *
     * <p><b>Requires RabbitMQ 4.4 or more.</b>
     *
     * @param deliveryTime absolute delivery time, must be in the future
     * @param deliveryFailed if true, the delivery count of the message is incremented
     * @param deferralToken the token the message is parked under, at most 256 UTF-8 bytes
     * @see <a
     *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
     *     1.0 <code>modified</code> outcome</a>
     * @see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support
     *     in RabbitMQ</a>
     * @see Consumer#claimDeferred(String...)
     * @since 1.6.0
     */
    void delayedRetry(Instant deliveryTime, boolean deliveryFailed, String deferralToken);

    /**
     * Create a batch context to accumulate message contexts and settle them at once.
     *
     * <p>The message context the batch context is created from is <b>not</b> added to the batch
     * context.
     *
     * @return the created batch context
     */
    BatchContext batch(int batchSizeHint);
  }

  /**
   * Context to accumulate message contexts and settle them at once.
   *
   * <p>A {@link BatchContext} is also a {@link Context}: the same methods are available to settle
   * the messages.
   *
   * <p>Only "simple" (not batch) message contexts can be added to a batch context. Calling {@link
   * Context#batch(int)} on a batch context returns the instance itself.
   *
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-disposition">AMQP
   *     1.0 Disposition performative</a>
   */
  interface BatchContext extends Context {

    /**
     * Add a message context to the batch context.
     *
     * @param context the message context to add
     */
    void add(Context context);

    /**
     * Get the current number of message contexts in the batch context.
     *
     * @return current number of message contexts in the batch
     */
    int size();
  }
}
