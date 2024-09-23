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
 * Builder for a <a href="https://www.rabbitmq.com/docs/amqp#address-v2">AMQP target address format
 * v2.</a>
 *
 * @param <T> the type of object returned by methods, usually the object itself
 */
public interface AddressBuilder<T> {

  /**
   * Set the exchange.
   *
   * @param exchange exchange
   * @return type-parameter object
   */
  T exchange(String exchange);

  /**
   * Set the routing key.
   *
   * @param key routing key
   * @return type-parameter object
   */
  T key(String key);

  /**
   * Set the queue.
   *
   * @param queue queue
   * @return type-parameter object
   */
  T queue(String queue);
}
