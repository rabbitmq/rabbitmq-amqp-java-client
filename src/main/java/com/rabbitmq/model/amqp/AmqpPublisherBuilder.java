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

import com.rabbitmq.model.Publisher;
import com.rabbitmq.model.PublisherBuilder;
import com.rabbitmq.model.Resource;
import java.util.ArrayList;
import java.util.List;

class AmqpPublisherBuilder implements PublisherBuilder {

  private final AmqpConnection connection;
  private final List<Resource.StateListener> listeners = new ArrayList<>();

  private String address;

  AmqpPublisherBuilder(AmqpConnection connection) {
    this.connection = connection;
  }

  @Override
  public PublisherBuilder address(String address) {
    this.address = address;
    return this;
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

  String address() {
    return address;
  }
}
