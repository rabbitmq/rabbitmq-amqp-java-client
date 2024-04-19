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
package com.rabbitmq.model.amqp;

import com.rabbitmq.model.Consumer;
import com.rabbitmq.model.ConsumerBuilder;
import com.rabbitmq.model.Resource;
import java.util.ArrayList;
import java.util.List;

class AmqpConsumerBuilder implements ConsumerBuilder {

  private final AmqpConnection connection;
  private String queue;
  private Consumer.MessageHandler messageHandler;
  private int initialCredits = 10;
  private final List<Resource.StateListener> listeners = new ArrayList<>();

  AmqpConsumerBuilder(AmqpConnection connection) {
    this.connection = connection;
  }

  @Override
  public ConsumerBuilder queue(String queue) {
    this.queue = queue;
    return this;
  }

  @Override
  public ConsumerBuilder messageHandler(Consumer.MessageHandler handler) {
    this.messageHandler = handler;
    return this;
  }

  @Override
  public ConsumerBuilder initialCredits(int initialCredits) {
    this.initialCredits = initialCredits;
    return this;
  }

  @Override
  public ConsumerBuilder listeners(Resource.StateListener... listeners) {
    if (listeners == null || listeners.length == 0) {
      this.listeners.clear();
    } else {
      this.listeners.addAll(List.of(listeners));
    }
    return this;
  }

  AmqpConnection connection() {
    return connection;
  }

  String queue() {
    return queue;
  }

  Consumer.MessageHandler messageHandler() {
    return messageHandler;
  }

  int initialCredits() {
    return initialCredits;
  }

  List<Resource.StateListener> listeners() {
    return listeners;
  }

  @Override
  public Consumer build() {
    if (this.queue == null || this.queue.isBlank()) {
      throw new IllegalArgumentException("A queue must be specified");
    }
    return this.connection.createConsumer(this);
  }
}
