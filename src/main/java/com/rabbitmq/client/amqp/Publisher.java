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
 * API to send messages.
 *
 * <p>Instances are created and configured with a {@link PublisherBuilder}.
 *
 * <p>Implementations must be thread-safe.
 *
 * @see PublisherBuilder
 * @see Connection#publisherBuilder()
 */
public interface Publisher extends AutoCloseable, Resource {

  /**
   * Create a message meant to be published by the publisher instance.
   *
   * <p>Once published with the {@link #publish(Message, Callback)} the message instance should be
   * not be modified or even reused.
   *
   * @return a message
   */
  Message message();

  /**
   * Create a message meant to be published by the publisher instance.
   *
   * <p>Once published with the {@link #publish(Message, Callback)} the message instance should be
   * not be modified or even reused.
   *
   * @param body message body
   * @return a message with the provided body
   */
  Message message(byte[] body);

  /**
   * Publish a message.
   *
   * @param message message to publish
   * @param callback asynchronous callback for broker message processing feedback
   */
  void publish(Message message, Callback callback);

  /** Close the publisher and release its resources. */
  @Override
  void close();

  /**
   * API to deal with the feedback the broker provides after its processing of a published message.
   */
  interface Callback {

    /**
     * Handle broker feedback.
     *
     * @param context context of the outbound message and the broker feedback
     */
    void handle(Context context);
  }

  /** Feedback context. */
  interface Context {

    /**
     * The message.
     *
     * @return the message
     */
    Message message();

    /**
     * The status returned by the broker.
     *
     * @return status of the message
     * @see <a href="https://www.rabbitmq.com/docs/amqp#outcomes">AMQP Outcomes</a>
     */
    Status status();

    /**
     * Failure cause (null, unless for some cases of {@link Status#REJECTED}).
     *
     * @return failure cause, null if no cause for failure
     */
    Throwable failureCause();
  }

  /**
   * Message status.
   *
   * @see <a href="https://www.rabbitmq.com/docs/amqp#outcomes">AMQP Outcomes</a>
   */
  enum Status {
    /** The message has been accepted by the broker. */
    ACCEPTED,
    /**
     * At least one queue the message was routed to rejected the message. This happens when the
     * queue length is exceeded and the queue's overflow behaviour is set to reject-publish or when
     * a target classic queue is unavailable.
     */
    REJECTED,
    /**
     * The broker could not route the message to any queue.
     *
     * <p>This is likely to be due to a topology misconfiguration.
     */
    RELEASED
  }
}
