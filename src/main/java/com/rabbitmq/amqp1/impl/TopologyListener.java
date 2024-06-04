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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface TopologyListener {

  TopologyListener NO_OP = new NoOpTopologyListener();

  void exchangeDeclared(AmqpExchangeSpecification specification);

  void exchangeDeleted(String name);

  void queueDeclared(AmqpQueueSpecification specification);

  void queueDeleted(String name);

  void bindingDeclared(AmqpBindingManagement.AmqpBindingSpecification specification);

  void bindingDeleted(AmqpBindingManagement.AmqpUnbindSpecification specification);

  // TODO: use queue instead of address
  void consumerCreated(long id, String address);

  // TODO: use queue instead of address
  void consumerDeleted(long id, String address);

  static TopologyListener compose(List<TopologyListener> listeners) {
    return new CompositeTopologyListener(listeners);
  }

  class NoOpTopologyListener implements TopologyListener {

    @Override
    public void exchangeDeclared(AmqpExchangeSpecification specification) {}

    @Override
    public void exchangeDeleted(String name) {}

    @Override
    public void queueDeclared(AmqpQueueSpecification specification) {}

    @Override
    public void queueDeleted(String name) {}

    @Override
    public void bindingDeclared(AmqpBindingManagement.AmqpBindingSpecification specification) {}

    @Override
    public void bindingDeleted(AmqpBindingManagement.AmqpUnbindSpecification specification) {}

    @Override
    public void consumerCreated(long id, String address) {}

    @Override
    public void consumerDeleted(long id, String address) {}
  }

  class CompositeTopologyListener implements TopologyListener, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeTopologyListener.class);

    private final List<TopologyListener> listeners;

    public CompositeTopologyListener(List<TopologyListener> listeners) {
      this.listeners = new CopyOnWriteArrayList<>(listeners);
    }

    @Override
    public void exchangeDeclared(AmqpExchangeSpecification specification) {
      listeners.forEach(mr -> mr.exchangeDeclared(specification));
    }

    @Override
    public void exchangeDeleted(String name) {
      listeners.forEach(mr -> mr.exchangeDeleted(name));
    }

    @Override
    public void queueDeclared(AmqpQueueSpecification specification) {
      listeners.forEach(mr -> mr.queueDeclared(specification));
    }

    @Override
    public void queueDeleted(String name) {
      listeners.forEach(mr -> mr.queueDeleted(name));
    }

    @Override
    public void bindingDeclared(AmqpBindingManagement.AmqpBindingSpecification specification) {
      listeners.forEach(mr -> mr.bindingDeclared(specification));
    }

    @Override
    public void bindingDeleted(AmqpBindingManagement.AmqpUnbindSpecification specification) {
      listeners.forEach(mr -> mr.bindingDeleted(specification));
    }

    @Override
    public void consumerCreated(long id, String address) {
      listeners.forEach(mr -> mr.consumerCreated(id, address));
    }

    @Override
    public void consumerDeleted(long id, String address) {
      listeners.forEach(mr -> mr.consumerDeleted(id, address));
    }

    @Override
    public void close() throws Exception {
      for (TopologyListener listener : this.listeners) {
        if (listener instanceof AutoCloseable) {
          try {
            ((AutoCloseable) listener).close();
          } catch (Exception e) {
            LOGGER.info("Error while closing topology listener", e);
          }
        }
      }
    }
  }
}
