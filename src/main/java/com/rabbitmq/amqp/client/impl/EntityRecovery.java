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
package com.rabbitmq.amqp.client.impl;

import com.rabbitmq.amqp.client.Management;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EntityRecovery {

  private static final Logger LOGGER = LoggerFactory.getLogger(EntityRecovery.class);

  private final RecordingTopologyListener listener;
  private final AmqpConnection connection;
  private final RecordingTopologyListener.Visitor recoveryVisitor;

  EntityRecovery(AmqpConnection connection, RecordingTopologyListener listener) {
    this.connection = connection;
    this.listener = listener;
    this.recoveryVisitor =
        new RecordingTopologyListener.Visitor() {
          @Override
          public void visitExchanges(List<RecordingTopologyListener.ExchangeSpec> exchanges) {
            if (exchanges.isEmpty()) {
              LOGGER.debug("No exchanges to recover.");
            } else {
              LOGGER.debug("Recovering {} exchange(s)...", exchanges.size());
              for (RecordingTopologyListener.ExchangeSpec spec : exchanges) {
                recoverExchange(spec);
              }
              LOGGER.debug("Exchanges recovered");
            }
          }

          @Override
          public void visitQueues(List<RecordingTopologyListener.QueueSpec> queues) {
            if (queues.isEmpty()) {
              LOGGER.debug("No queues to recover");
            } else {
              LOGGER.debug("Recovering {} queue(s)...", queues.size());
              for (RecordingTopologyListener.QueueSpec spec : queues) {
                recoverQueue(spec);
              }
              LOGGER.debug("Queues recovered");
            }
          }

          @Override
          public void visitBindings(Collection<RecordingTopologyListener.BindingSpec> bindings) {
            if (bindings.isEmpty()) {
              LOGGER.debug("No bindings to recover");
            } else {
              LOGGER.debug("Recovering {} binding(s)...", bindings.size());
              for (RecordingTopologyListener.BindingSpec binding : bindings) {
                recoverBinding(binding);
              }
              LOGGER.debug("Bindings recovered");
            }
          }
        };
  }

  void recover() {
    LOGGER.debug("Starting topology recovery");
    this.listener.accept(this.recoveryVisitor);
    LOGGER.debug("Topology recovered");
  }

  private void recoverExchange(RecordingTopologyListener.ExchangeSpec exchange) {
    LOGGER.debug("Recovering exchange {}", exchange.name());

    try {
      Management.ExchangeSpecification spec =
          this.connection
              .managementNoCheck()
              .exchange()
              .name(exchange.name())
              .autoDelete(exchange.autoDelete())
              .type(exchange.type());
      exchange.arguments().forEach(spec::argument);
      spec.declare();
      LOGGER.debug("Exchange {} recovered", exchange.name());
    } catch (Exception e) {
      LOGGER.warn("Error while recovering exchange {}", exchange.name(), e);
    }
  }

  private void recoverQueue(RecordingTopologyListener.QueueSpec queue) {
    LOGGER.debug("Recovering queue {}", queue.name());
    try {
      Management.QueueSpecification spec =
          this.connection
              .managementNoCheck()
              .queue()
              .name(queue.name())
              .exclusive(queue.exclusive())
              .autoDelete(queue.autoDelete());
      queue.arguments().forEach(spec::argument);
      spec.declare();
      LOGGER.debug("Queue {} recovered", queue.name());
    } catch (Exception e) {
      LOGGER.warn("Error while recovering queue {}", queue.name(), e);
    }
  }

  private void recoverBinding(RecordingTopologyListener.BindingSpec binding) {
    try {
      Management.BindingSpecification spec =
          this.connection
              .managementNoCheck()
              .binding()
              .sourceExchange(binding.source())
              .key(binding.key());
      if (binding.toQueue()) {
        spec.destinationQueue(binding.destination());
      } else {
        spec.destinationExchange(binding.destination());
      }
      binding.arguments().forEach(spec::argument);
      spec.bind();
    } catch (Exception e) {
      LOGGER.warn(
          "Error while recovering binding from {} to {} {} with binding key {}",
          binding.source(),
          binding.toQueue() ? "queue" : "exchange",
          binding.destination(),
          binding.key(),
          e);
    }
  }
}
