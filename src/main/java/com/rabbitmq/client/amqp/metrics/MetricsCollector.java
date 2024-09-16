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
package com.rabbitmq.client.amqp.metrics;

/** Interface to collect execution data of the client. */
public interface MetricsCollector {

  /** Called when a new {@link com.rabbitmq.client.amqp.Connection} is opened. */
  void openConnection();

  /** Called when a {@link com.rabbitmq.client.amqp.Connection} is closed. */
  void closeConnection();

  /** Called when a new {@link com.rabbitmq.client.amqp.Publisher} is opened. */
  void openPublisher();

  /** Called when a {@link com.rabbitmq.client.amqp.Publisher} is closed. */
  void closePublisher();

  /** Called when a new {@link com.rabbitmq.client.amqp.Consumer} is opened. */
  void openConsumer();

  /** Called when a {@link com.rabbitmq.client.amqp.Consumer} is closed. */
  void closeConsumer();

  /** Called when a {@link com.rabbitmq.client.amqp.Message} is published. */
  void publish();

  /**
   * Called when a {@link com.rabbitmq.client.amqp.Message} is settled by the broker.
   *
   * @param disposition disposition (outcome)
   */
  void publishDisposition(PublishDisposition disposition);

  /**
   * Called when a {@link com.rabbitmq.client.amqp.Message} is dispatched to a {@link
   * com.rabbitmq.client.amqp.Consumer}.
   */
  void consume();

  /**
   * Called when a {@link com.rabbitmq.client.amqp.Message} is settled by a {@link
   * com.rabbitmq.client.amqp.Consumer}.
   *
   * @param disposition
   */
  void consumeDisposition(ConsumeDisposition disposition);

  /** The broker-to-client dispositions. */
  enum PublishDisposition {
    /** see {@link com.rabbitmq.client.amqp.Publisher.Status#ACCEPTED} */
    ACCEPTED,
    /** see {@link com.rabbitmq.client.amqp.Publisher.Status#REJECTED} */
    REJECTED,
    /** see {@link com.rabbitmq.client.amqp.Publisher.Status#RELEASED} */
    RELEASED
  }

  /** The client-to-broker dispositions. */
  enum ConsumeDisposition {
    /** see {@link com.rabbitmq.client.amqp.Consumer.Context#accept()} */
    ACCEPTED,
    /** see {@link com.rabbitmq.client.amqp.Consumer.Context#discard()} */
    DISCARDED,
    /** see {@link com.rabbitmq.client.amqp.Consumer.Context#requeue()} */
    REQUEUED
  }
}
