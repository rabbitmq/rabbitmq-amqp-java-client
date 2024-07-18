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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

final class RecordingTopologyListener implements TopologyListener, AutoCloseable {

  private final EventLoop eventLoop;
  private final Map<String, ExchangeSpec> exchanges = new LinkedHashMap<>();
  private final Map<String, QueueSpec> queues = new LinkedHashMap<>();
  private final Set<BindingSpec> bindings = new LinkedHashSet<>();
  private final Map<Long, ConsumerSpec> consumers = new LinkedHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  RecordingTopologyListener(ExecutorService executorService) {
    this.eventLoop = new EventLoop("topology", executorService);
  }

  @Override
  public void exchangeDeclared(AmqpExchangeSpecification specification) {
    this.submit(() -> this.exchanges.put(specification.name(), new ExchangeSpec(specification)));
  }

  @Override
  public void exchangeDeleted(String name) {
    this.submit(
        () -> {
          this.exchanges.remove(name);
          Set<BindingSpec> deletedBindings = this.deleteBindings(name, true);
          this.deleteAutoDeleteExchanges(deletedBindings);
        });
  }

  @Override
  public void queueDeclared(AmqpQueueSpecification specification) {
    this.submit(() -> this.queues.put(specification.name(), new QueueSpec(specification)));
  }

  @Override
  public void queueDeleted(String name) {
    this.submit(
        () -> {
          this.queues.remove(name);
          Set<BindingSpec> deletedBindings = this.deleteBindings(name, false);
          this.deleteAutoDeleteExchanges(deletedBindings);
        });
  }

  @Override
  public void bindingDeclared(AmqpBindingManagement.AmqpBindingSpecification specification) {
    this.submit(() -> this.bindings.add(new BindingSpec(specification.state())));
  }

  @Override
  public void bindingDeleted(AmqpBindingManagement.AmqpUnbindSpecification specification) {
    this.submit(
        () -> {
          BindingSpec spec = new BindingSpec(specification.state());
          this.bindings.remove(spec);
          this.deleteAutoDeleteExchanges(Collections.singleton(spec));
        });
  }

  @Override
  public void consumerCreated(long id, String queue) {
    this.submit(() -> this.consumers.put(id, new ConsumerSpec(id, queue)));
  }

  @Override
  public void consumerDeleted(long id, String queue) {
    this.submit(
        () -> {
          this.consumers.remove(id);
          // if there's no consumer anymore on the queue, delete it if it's auto-delete
          boolean atLeastOneConsumerOnQueue =
              this.consumers.values().stream().anyMatch(spec -> spec.queue.equals(queue));
          if (!atLeastOneConsumerOnQueue) {
            QueueSpec queueSpec = this.queues.get(queue);
            if (queueSpec != null && queueSpec.autoDelete) {
              this.queueDeleted(queue);
            }
          }
        });
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.eventLoop.close();
    }
  }

  private void submit(Runnable task) {
    if (!this.closed.get()) {
      this.eventLoop.submit(task);
    }
  }

  private Set<BindingSpec> deleteBindings(String name, boolean exchange) {
    Set<BindingSpec> deletedBindings = new LinkedHashSet<>();
    // delete bindings that depend on this exchange or queue
    Iterator<BindingSpec> iterator = this.bindings.iterator();
    while (iterator.hasNext()) {
      BindingSpec spec = iterator.next();
      if (spec.isInvolved(name, exchange)) {
        iterator.remove();
        deletedBindings.add(spec);
      }
    }
    return deletedBindings;
  }

  private void deleteAutoDeleteExchanges(Set<BindingSpec> deletedBindings) {
    // delete auto-delete exchanges which are no longer sources in any bindings
    for (BindingSpec binding : deletedBindings) {
      String source = binding.source;
      boolean exchangeStillSource;
      exchangeStillSource = this.bindings.stream().anyMatch(b -> b.source.equals(source));

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
    AtomicReference<List<ExchangeSpec>> exchangeCopy = new AtomicReference<>();
    AtomicReference<List<QueueSpec>> queueCopy = new AtomicReference<>();
    AtomicReference<Set<BindingSpec>> bindingCopy = new AtomicReference<>();
    submit(
        () -> {
          exchangeCopy.set(new ArrayList<>(this.exchanges.values()));
          queueCopy.set(new ArrayList<>(this.queues.values()));
          bindingCopy.set(new LinkedHashSet<>(this.bindings));
        });
    visitor.visitExchanges(exchangeCopy.get());
    visitor.visitQueues(queueCopy.get());
    visitor.visitBindings(bindingCopy.get());
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
    private final boolean autoDelete;
    private final Map<String, Object> arguments = new LinkedHashMap<>();

    private ExchangeSpec(AmqpExchangeSpecification specification) {
      this.name = specification.name();
      this.type = specification.type();
      this.autoDelete = specification.autoDelete();
      specification.arguments(this.arguments::put);
    }

    String name() {
      return name;
    }

    String type() {
      return type;
    }

    boolean autoDelete() {
      return autoDelete;
    }

    public Map<String, Object> arguments() {
      return arguments;
    }
  }

  static class QueueSpec {

    private final String name;
    private final boolean exclusive;
    private final boolean autoDelete;
    private final Map<String, Object> arguments = new LinkedHashMap<>();

    private QueueSpec(AmqpQueueSpecification specification) {
      this.name = specification.name();
      this.exclusive = specification.exclusive();
      this.autoDelete = specification.autoDelete();
      specification.arguments(this.arguments::put);
    }

    String name() {
      return name;
    }

    boolean exclusive() {
      return exclusive;
    }

    boolean autoDelete() {
      return autoDelete;
    }

    Map<String, Object> arguments() {
      return arguments;
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

    String source() {
      return source;
    }

    String destination() {
      return destination;
    }

    String key() {
      return key;
    }

    Map<String, Object> arguments() {
      return arguments;
    }

    boolean toQueue() {
      return toQueue;
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

    void visitExchanges(List<ExchangeSpec> exchanges);

    void visitQueues(List<QueueSpec> queues);

    void visitBindings(Collection<BindingSpec> bindings);
  }
}
