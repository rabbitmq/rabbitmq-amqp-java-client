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

import java.util.function.Function;

/**
 * API to observe common message operations.
 *
 * @see com.rabbitmq.client.amqp.observation.micrometer.MicrometerObservationCollectorBuilder
 */
public interface ObservationCollector {

  /**
   * Observe the publishing of a message.
   *
   * <p>Implementations must perform the publish call with the provided argument and make their
   * observations "around" it.
   *
   * @param exchange the exchange the message is published to
   * @param routingKey the routing key
   * @param message the published message
   * @param connectionInfo information on the connection
   * @param publishCall the actual publish call
   * @return some context returned by the publish call
   * @param <T> type of the context returned by the publish call
   */
  <T> T publish(
      String exchange,
      String routingKey,
      Message message,
      ConnectionInfo connectionInfo,
      Function<Message, T> publishCall);

  /**
   * Decorate a {@link com.rabbitmq.client.amqp.Consumer.MessageHandler} to observe the delivery of
   * messages.
   *
   * @param queue the queue the consumer subscribes to
   * @param handler the application message delivery handler
   * @return the decorated handler
   */
  Consumer.MessageHandler subscribe(String queue, Consumer.MessageHandler handler);

  /** Information on a connection. */
  interface ConnectionInfo {

    /**
     * Remote (broker) address.
     *
     * @return
     */
    String peerAddress();

    /**
     * Remote (broker) port.
     *
     * @return
     */
    int peerPort();
  }
}
