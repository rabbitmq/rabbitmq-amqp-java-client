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

  /** Pause the consumer to stop receiving messages. */
  void pause();

  /**
   * Return the number of unsettled messages.
   *
   * @return unsettled message count
   */
  long unsettledMessageCount();

  /** Request to receive messages again. */
  void unpause();

  /** Close the consumer with its resources. */
  @Override
  void close();

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
     * <p>Application-specific annotation keys must start with the <code>x-opt-</code> prefix.
     * Annotation keys the broker understands start with <code>x-</code>, but not with <code>x-opt-
     * </code>.
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
     * <p>Application-specific annotation keys must start with the <code>x-opt-</code> prefix.
     * Annotation keys the broker understands start with <code>x-</code>, but not with <code>x-opt-
     * </code>.
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
   * Context#batch()} on a batch context returns the instance itself.
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
