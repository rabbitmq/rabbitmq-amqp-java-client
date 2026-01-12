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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RecordingTopologyListener implements TopologyListener, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordingTopologyListener.class);

  private final String label;
  private final EventLoop.Client<State> eventLoopClient;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  RecordingTopologyListener(String label, EventLoop eventLoop) {
    this.label = label;
    this.eventLoopClient = eventLoop.register(State::new);
  }

  private static class State {

    private final Map<String, ExchangeSpec> exchanges = new LinkedHashMap<>();
    private final Map<String, QueueSpec> queues = new LinkedHashMap<>();
    private final Set<BindingSpec> bindings = new LinkedHashSet<>();
    private final Map<Long, ConsumerSpec> consumers = new LinkedHashMap<>();
  }

  @Override
  public void exchangeDeclared(AmqpExchangeSpecification specification) {
    this.submit(s -> s.exchanges.put(specification.name(), new ExchangeSpec(specification)));
  }

  @Override
  public void exchangeDeleted(String name) {
    this.submit(
        s -> {
          s.exchanges.remove(name);
          Set<BindingSpec> deletedBindings = this.deleteBindings(s, name, true);
          this.deleteAutoDeleteExchanges(s, deletedBindings);
        });
  }

  @Override
  public void queueDeclared(AmqpQueueSpecification specification) {
    this.submit(s -> s.queues.put(specification.name(), new QueueSpec(specification)));
  }

  @Override
  public void queueDeleted(String name) {
    this.submit(
        s -> {
          s.queues.remove(name);
          Set<BindingSpec> deletedBindings = this.deleteBindings(s, name, false);
          this.deleteAutoDeleteExchanges(s, deletedBindings);
        });
  }

  @Override
  public void bindingDeclared(AmqpBindingManagement.AmqpBindingSpecification specification) {
    this.submit(s -> s.bindings.add(new BindingSpec(specification.state())));
  }

  @Override
  public void bindingDeleted(AmqpBindingManagement.AmqpUnbindSpecification specification) {
    this.submit(
        s -> {
          BindingSpec spec = new BindingSpec(specification.state());
          s.bindings.remove(spec);
          this.deleteAutoDeleteExchanges(s, Collections.singleton(spec));
        });
  }

  @Override
  public void consumerCreated(long id, String queue) {
    this.submit(s -> s.consumers.put(id, new ConsumerSpec(id, queue)));
  }

  @Override
  public void consumerDeleted(long id, String queue) {
    this.submit(
        s -> {
          s.consumers.remove(id);
          // if there's no consumer anymore on the queue, delete it if it's auto-delete
          boolean atLeastOneConsumerOnQueue =
              s.consumers.values().stream().anyMatch(spec -> spec.queue.equals(queue));
          if (!atLeastOneConsumerOnQueue) {
            QueueSpec queueSpec = s.queues.get(queue);
            if (queueSpec != null && queueSpec.autoDelete) {
              this.queueDeleted(queue);
            }
          }
        });
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.eventLoopClient.close();
    }
  }

  private void submit(Consumer<State> task) {
    if (!this.closed.get()) {
      this.eventLoopClient.submit(task);
    }
  }

  private Set<BindingSpec> deleteBindings(State s, String name, boolean exchange) {
    Set<BindingSpec> deletedBindings = new LinkedHashSet<>();
    // delete bindings that depend on this exchange or queue
    Iterator<BindingSpec> iterator = s.bindings.iterator();
    while (iterator.hasNext()) {
      BindingSpec spec = iterator.next();
      if (spec.isInvolved(name, exchange)) {
        iterator.remove();
        deletedBindings.add(spec);
      }
    }
    return deletedBindings;
  }

  private void deleteAutoDeleteExchanges(State s, Set<BindingSpec> deletedBindings) {
    // delete auto-delete exchanges which are no longer sources in any bindings
    for (BindingSpec binding : deletedBindings) {
      String source = binding.source;
      boolean exchangeStillSource;
      exchangeStillSource = s.bindings.stream().anyMatch(b -> b.source.equals(source));

      if (!exchangeStillSource) {
        ExchangeSpec exchange = s.exchanges.get(source);
        if (exchange != null && exchange.autoDelete) {
          exchangeDeleted(exchange.name);
        }
      }
    }
  }

  void accept(Visitor visitor) {
    LOGGER.debug("Topology listener '{}' visitor, retrieving state...", this.label);
    AtomicReference<List<ExchangeSpec>> exchangeCopy =
        new AtomicReference<>(Collections.emptyList());
    AtomicReference<List<QueueSpec>> queueCopy = new AtomicReference<>(Collections.emptyList());
    AtomicReference<Set<BindingSpec>> bindingCopy = new AtomicReference<>(Collections.emptySet());
    submit(
        s -> {
          exchangeCopy.set(new ArrayList<>(s.exchanges.values()));
          queueCopy.set(new ArrayList<>(s.queues.values()));
          bindingCopy.set(new LinkedHashSet<>(s.bindings));
        });
    LOGGER.debug(
        "Topology listener '{}' visitor, state retrieved, visiting topology...", this.label);
    visitor.visitExchanges(exchangeCopy.get());
    LOGGER.debug("Topology listener '{}' visitor, exchanges visited...", this.label);
    visitor.visitQueues(queueCopy.get());
    LOGGER.debug("Topology listener '{}' visitor, queues visited...", this.label);
    visitor.visitBindings(bindingCopy.get());
    LOGGER.debug("Topology listener '{}' visitor, topology visited...", this.label);
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

  // for test assertions

  private State state() {
    return this.eventLoopClient.state();
  }

  Map<String, ExchangeSpec> exchanges() {
    return new LinkedHashMap<>(state().exchanges);
  }

  Map<String, QueueSpec> queues() {
    return new LinkedHashMap<>(state().queues);
  }

  int bindingCount() {
    return state().bindings.size();
  }

  int exchangeCount() {
    return state().exchanges.size();
  }

  int queueCount() {
    return state().queues.size();
  }
}
