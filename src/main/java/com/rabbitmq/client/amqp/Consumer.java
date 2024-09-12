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

/**
 * API to consume messages from a RabbitMQ queue.
 *
 * <p>Instances are configured and created with a {@link ConsumerBuilder}.
 *
 * @see Connection#consumerBuilder()
 * @see ConsumerBuilder
 */
public interface Consumer extends AutoCloseable {

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
     * Requeue the message (AMQP 1.0 <code>released</code> outcome).
     *
     * <p>This means the message has not been processed and the broker can requeue it and deliver it
     * to the same or a different consumer.
     */
    void requeue();
  }
}