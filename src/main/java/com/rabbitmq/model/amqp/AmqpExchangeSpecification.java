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

import com.rabbitmq.model.Management;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

class AmqpExchangeSpecification implements Management.ExchangeSpecification {

  private final AmqpManagement management;

  private String name;
  private String type = Management.ExchangeType.DIRECT.name().toLowerCase(Locale.ENGLISH);
  private final boolean durable = true;
  private final boolean internal = false;
  private boolean autoDelete = false;
  private final Map<String, Object> arguments = new LinkedHashMap<>();

  AmqpExchangeSpecification(AmqpManagement management) {
    this.management = management;
  }

  @Override
  public Management.ExchangeSpecification name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public Management.ExchangeSpecification autoDelete(boolean autoDelete) {
    this.autoDelete = autoDelete;
    return this;
  }

  @Override
  public Management.ExchangeSpecification type(Management.ExchangeType type) {
    if (type == null) {
      this.type = null;
    } else {
      this.type = type.name().toLowerCase(Locale.ENGLISH);
    }
    return this;
  }

  @Override
  public Management.ExchangeSpecification type(String type) {
    this.type = type;
    return this;
  }

  @Override
  public Management.ExchangeSpecification argument(String key, Object value) {
    this.arguments.put(key, value);
    return this;
  }

  @Override
  public void declare() {
    // TODO check name is specified (server-named entities not allowed)
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("type", this.type);
    body.put("durable", this.durable);
    body.put("auto_delete", this.autoDelete);
    body.put("internal", this.internal);
    body.put("arguments", this.arguments);
    this.management.declareExchange(this.name, body);
    this.management.recovery().exchangeDeclared(this);
  }

  String name() {
    return this.name;
  }

  String type() {
    return this.type;
  }

  boolean autoDelete() {
    return this.autoDelete;
  }

  void arguments(BiConsumer<String, Object> consumer) {
    this.arguments.forEach(consumer);
  }
}
