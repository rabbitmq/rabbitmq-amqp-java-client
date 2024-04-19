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

import java.util.function.Supplier;

class AmqpManagementParameters {

  private final AmqpConnection connection;
  private TopologyListener topologyListener;
  private Supplier<String> nameSupplier = Utils.NAME_SUPPLIER;

  AmqpManagementParameters(AmqpConnection connection) {
    this.connection = connection;
  }

  AmqpManagementParameters topologyListener(TopologyListener topologyListener) {
    this.topologyListener = topologyListener;
    return this;
  }

  AmqpManagementParameters nameSupplier(Supplier<String> nameSupplier) {
    this.nameSupplier = nameSupplier;
    return this;
  }

  AmqpConnection connection() {
    return this.connection;
  }

  TopologyListener topologyListener() {
    return this.topologyListener;
  }

  public Supplier<String> nameSupplier() {
    return nameSupplier;
  }
}
