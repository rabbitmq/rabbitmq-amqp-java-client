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
package com.rabbitmq.client.amqp.impl;

import static com.rabbitmq.client.amqp.impl.UriUtils.encodePathSegment;

import com.rabbitmq.client.amqp.AddressBuilder;

abstract class DefaultAddressBuilder<T> implements AddressBuilder<T> {

  private final T owner;
  private String exchange, key, queue;

  DefaultAddressBuilder(T owner) {
    this.owner = owner;
  }

  @Override
  public T exchange(String exchange) {
    if ("".equals(exchange)) {
      throw new IllegalArgumentException(
          "The default exchange is not allowed, use the queue format instead");
    }
    this.exchange = exchange;
    this.queue = null;
    return result();
  }

  @Override
  public T key(String key) {
    this.key = key;
    this.queue = null;
    return result();
  }

  @Override
  public T queue(String queue) {
    this.queue = queue;
    this.exchange = null;
    this.key = null;
    return result();
  }

  String address() {
    if (this.exchange != null) {
      if (this.key != null && !this.key.isEmpty()) {
        return "/e/" + encodePathSegment(this.exchange) + "/" + encodePathSegment(this.key);
      } else {
        return "/e/" + encodePathSegment(this.exchange);
      }
    } else if (this.queue != null) {
      return "/q/" + encodePathSegment(this.queue);
    } else {
      return null;
    }
  }

  T result() {
    return this.owner;
  }

  void copyTo(AddressBuilder<?> copy) {
    copy.exchange(this.exchange);
    copy.key(this.key);
    copy.queue(this.queue);
  }

  DestinationSpec destination() {
    String ex = null, rk = null;
    if (this.exchange != null) {
      ex = this.exchange;
      ex = "amq.default".equals(ex) ? "" : ex;
      rk = this.key == null ? "" : this.key;
    } else if (this.queue != null) {
      ex = "";
      rk = this.queue;
    }
    return new DestinationSpec(ex, rk);
  }

  static class DestinationSpec {

    private final String exchange, routingKey;

    DestinationSpec(String exchange, String routingKey) {
      this.exchange = exchange;
      this.routingKey = routingKey;
    }

    String exchange() {
      return this.exchange;
    }

    String routingKey() {
      return this.routingKey;
    }
  }
}
