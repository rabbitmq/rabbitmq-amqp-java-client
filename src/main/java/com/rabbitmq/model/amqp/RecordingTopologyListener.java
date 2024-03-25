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

import static com.rabbitmq.model.amqp.Utils.extractQueueFromSourceAddress;
import static java.lang.Boolean.TRUE;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class RecordingTopologyListener implements TopologyListener {

  private final Map<String, ExchangeSpec> exchanges = new ConcurrentHashMap<>();
  private final Map<String, QueueSpec> queues = new ConcurrentHashMap<>();
  private final Set<BindingSpec> bindings = Collections.synchronizedSet(new LinkedHashSet<>());
  private final ConcurrentHashMap<Long, ConsumerSpec> consumers = new ConcurrentHashMap<>();

  @Override
  public void exchangeDeclared(AmqpExchangeSpecification specification) {
    this.exchanges.put(specification.name(), new ExchangeSpec(specification));
  }

  @Override
  public void exchangeDeleted(String name) {
    this.exchanges.remove(name);
    Set<BindingSpec> deletedBindings = this.deleteBindings(name, true);
    this.deleteAutoDeleteExchanges(deletedBindings);
  }

  @Override
  public void queueDeclared(AmqpQueueSpecification specification) {
    this.queues.put(specification.name(), new QueueSpec(specification));
  }

  @Override
  public void queueDeleted(String name) {
    this.queues.remove(name);
    Set<BindingSpec> deletedBindings = this.deleteBindings(name, false);
    this.deleteAutoDeleteExchanges(deletedBindings);
  }

  @Override
  public void bindingDeclared(AmqpBindingManagement.AmqpBindingSpecification specification) {
    this.bindings.add(new BindingSpec(specification.state()));
  }

  @Override
  public void bindingDeleted(AmqpBindingManagement.AmqpUnbindSpecification specification) {
    BindingSpec spec = new BindingSpec(specification.state());
    this.bindings.remove(spec);
    this.deleteAutoDeleteExchanges(Collections.singleton(spec));
  }

  @Override
  public void consumerCreated(long id, String address) {
    String queue = extractQueueFromSourceAddress(address);
    if (queue != null) {
      this.consumers.put(id, new ConsumerSpec(id, queue));
    }
  }

  @Override
  public void consumerDeleted(long id, String address) {
    String queue = extractQueueFromSourceAddress(address);
    if (queue != null) {
      this.consumers.remove(id);
      // if there's no consumer anymore on the queue, delete it if it's auto-delete
      Boolean atLeastOneConsumerOnQueue =
          this.consumers.searchValues(
              Long.MAX_VALUE, spec -> spec.queue.equals(queue) ? TRUE : null);
      if (!TRUE.equals(atLeastOneConsumerOnQueue)) {
        QueueSpec queueSpec = this.queues.get(queue);
        if (queueSpec != null && queueSpec.autoDelete) {
          this.queueDeleted(queue);
        }
      }
    }
  }

  private Set<BindingSpec> deleteBindings(String name, boolean exchange) {
    Set<BindingSpec> deletedBindings = new LinkedHashSet<>();
    synchronized (this.bindings) {
      // delete bindings that depend on this exchange or queue
      Iterator<BindingSpec> iterator = this.bindings.iterator();
      while (iterator.hasNext()) {
        BindingSpec spec = iterator.next();
        if (spec.isInvolved(name, exchange)) {
          iterator.remove();
          deletedBindings.add(spec);
        }
      }
    }
    return deletedBindings;
  }

  private void deleteAutoDeleteExchanges(Set<BindingSpec> deletedBindings) {
    // delete auto-delete exchanges which are no longer sources in any bindings
    for (BindingSpec binding : deletedBindings) {
      String source = binding.source;
      boolean exchangeStillSource;
      synchronized (this.bindings) {
        exchangeStillSource = this.bindings.stream().anyMatch(b -> b.source.equals(source));
      }

      if (!exchangeStillSource) {
        ExchangeSpec exchange = this.exchanges.get(source);
        if (exchange != null && exchange.autoDelete) {
          exchangeDeleted(exchange.name);
        }
      }
    }
  }

  Map<String, ExchangeSpec> exchanges() {
    return new LinkedHashMap<>(this.exchanges);
  }

  Map<String, QueueSpec> queues() {
    return new LinkedHashMap<>(this.queues);
  }

  void accept(Visitor visitor) {
    visitor.visitExchanges(this.exchanges);
    visitor.visitQueues(this.queues);
    visitor.visitBindings(this.bindings);
  }

  int bindingCount() {
    return this.bindings.size();
  }

  int exchangeCount() {
    return this.exchanges.size();
  }

  int queueCount() {
    return this.queues.size();
  }

  static class ExchangeSpec {

    private final String name;
    private final String type;
    private final boolean durable = true;
    private final boolean internal = false;
    private final boolean autoDelete;
    private final Map<String, Object> arguments = new LinkedHashMap<>();

    private ExchangeSpec(AmqpExchangeSpecification specification) {
      this.name = specification.name();
      this.type = specification.type();
      this.autoDelete = specification.autoDelete();
      specification.arguments(this.arguments::put);
    }
  }

  static class QueueSpec {

    private final String name;
    private final boolean durable = true;
    private final boolean exclusive;
    private final boolean autoDelete;
    private final Map<String, Object> arguments = new LinkedHashMap<>();

    private QueueSpec(AmqpQueueSpecification specification) {
      this.name = specification.name();
      this.exclusive = specification.exclusive();
      this.autoDelete = specification.autoDelete();
      specification.arguments(this.arguments::put);
    }
  }

  static class BindingSpec {

    private final String source;
    private final String destination;
    private final String key;
    private final Map<String, Object> arguments = new LinkedHashMap<>();
    private final boolean toQueue;

    private BindingSpec(AmqpBindingManagement.BindingState state) {
      this.source = state.source();
      this.destination = state.destination();
      this.key = state.key() == null ? "" : state.key();
      this.toQueue = state.toQueue();
      state.arguments(this.arguments::put);
    }

    private boolean isInvolved(String entityName, boolean exchange) {
      return exchange ? isExchangeInvolved(entityName) : isQueueInvolved(entityName);
    }

    private boolean isExchangeInvolved(String exchange) {
      return source.equals(exchange) || (!toQueue && destination.equals(exchange));
    }

    private boolean isQueueInvolved(String queue) {
      return toQueue && destination.equals(queue);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BindingSpec that = (BindingSpec) o;
      return toQueue == that.toQueue
          && Objects.equals(source, that.source)
          && Objects.equals(destination, that.destination)
          && Objects.equals(key, that.key)
          && Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
      return Objects.hash(source, destination, key, arguments, toQueue);
    }
  }

  static class ConsumerSpec {

    private final long id;
    private final String queue;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ConsumerSpec that = (ConsumerSpec) o;
      return id == that.id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }

    private ConsumerSpec(long id, String queue) {
      this.id = id;
      this.queue = queue;
    }
  }

  interface Visitor {

    void visitExchanges(Map<String, ExchangeSpec> exchanges);

    void visitQueues(Map<String, QueueSpec> exchanges);

    void visitBindings(Set<BindingSpec> bindings);
  }
}
