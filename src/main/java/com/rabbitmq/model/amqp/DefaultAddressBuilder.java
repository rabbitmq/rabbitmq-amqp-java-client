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

import com.rabbitmq.model.AddressBuilder;

abstract class DefaultAddressBuilder<T> implements AddressBuilder<T> {

  private final T owner;
  private String exchange, key, queue;

  DefaultAddressBuilder(T owner) {
    this.owner = owner;
  }

  @Override
  public T exchange(String exchange) {
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
        return "/exchange/" + this.exchange + "/key/" + this.key;
      } else {
        return "/exchange/" + this.exchange;
      }
    } else if (this.queue != null) {
      return "/queue/" + this.queue;
    } else {
      return null;
    }
  }

  T result() {
    return this.owner;
  }
  ;
}
