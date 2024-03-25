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

import java.util.Map;
import java.util.Set;

class TopologyRecovery {

  private final RecordingTopologyListener listener;
  private final AmqpConnection connection;
  private final RecordingTopologyListener.Visitor recoveryVisitor;

  TopologyRecovery(AmqpConnection connection, RecordingTopologyListener listener) {
    this.connection = connection;
    this.listener = listener;
    this.recoveryVisitor =
        new RecordingTopologyListener.Visitor() {
          @Override
          public void visitExchanges(
              Map<String, RecordingTopologyListener.ExchangeSpec> exchanges) {}

          @Override
          public void visitQueues(Map<String, RecordingTopologyListener.QueueSpec> exchanges) {}

          @Override
          public void visitBindings(Set<RecordingTopologyListener.BindingSpec> bindings) {}
        };
  }

  void recover() {
    this.listener.accept(this.recoveryVisitor);
  }
}
