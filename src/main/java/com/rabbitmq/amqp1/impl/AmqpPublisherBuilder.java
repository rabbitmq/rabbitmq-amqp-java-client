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
package com.rabbitmq.amqp1.impl;

import com.rabbitmq.amqp1.AddressBuilder;
import com.rabbitmq.amqp1.Publisher;
import com.rabbitmq.amqp1.PublisherBuilder;
import com.rabbitmq.amqp1.Resource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

class AmqpPublisherBuilder implements PublisherBuilder {

  private final AmqpConnection connection;
  private final List<Resource.StateListener> listeners = new ArrayList<>();
  private final DefaultAddressBuilder<PublisherBuilder> addressBuilder =
      new DefaultAddressBuilder<>(this) {};
  private Duration publishTimeout = Duration.ofSeconds(60);

  AmqpPublisherBuilder(AmqpConnection connection) {
    this.connection = connection;
  }

  @Override
  public PublisherBuilder exchange(String exchange) {
    return this.addressBuilder.exchange(exchange);
  }

  @Override
  public PublisherBuilder key(String key) {
    return this.addressBuilder.key(key);
  }

  @Override
  public PublisherBuilder queue(String queue) {
    return this.addressBuilder.queue(queue);
  }

  @Override
  public PublisherBuilder listeners(Resource.StateListener... listeners) {
    if (listeners == null || listeners.length == 0) {
      this.listeners.clear();
    } else {
      this.listeners.addAll(List.of(listeners));
    }
    return this;
  }

  public AmqpPublisherBuilder publishTimeout(Duration publishTimeout) {
    this.publishTimeout = publishTimeout;
    return this;
  }

  @Override
  public Publisher build() {
    return this.connection.createPublisher(this);
  }

  AmqpConnection connection() {
    return connection;
  }

  List<Resource.StateListener> listeners() {
    return listeners;
  }

  AddressBuilder<?> addressBuilder() {
    return this.addressBuilder;
  }

  String address() {
    return this.addressBuilder.address();
  }

  DefaultAddressBuilder.DestinationSpec destination() {
    return this.addressBuilder.destination();
  }

  Duration publishTimeout() {
    return this.publishTimeout;
  }
}
