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

import com.rabbitmq.model.ModelException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RecordingTopologyListener implements TopologyListener, AutoCloseable {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordingTopologyListener.class);

  private final Map<String, ExchangeSpec> exchanges = new LinkedHashMap<>();
  private final Map<String, QueueSpec> queues = new LinkedHashMap<>();
  private final Set<BindingSpec> bindings = new LinkedHashSet<>();
  private final Map<Long, ConsumerSpec> consumers = new LinkedHashMap<>();
  private final BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<>(100);
  private final Future<?> loop;
  private final AtomicReference<Thread> loopThread = new AtomicReference<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  RecordingTopologyListener(ExecutorService executorService) {
    CountDownLatch loopThreadSetLatch = new CountDownLatch(1);
    this.loop =
        executorService.submit(
            () -> {
              loopThread.set(Thread.currentThread());
              loopThreadSetLatch.countDown();
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  Runnable task = this.taskQueue.take();
                  task.run();
                } catch (InterruptedException e) {
                  return;
                } catch (Exception e) {
                  LOGGER.warn("Error during processing of topology recording task", e);
                }
              }
            });
    try {
      if (!loopThreadSetLatch.await(10, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Recording topology loop could not start");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ModelException("Error while creating recording topology listener", e);
    }
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
  public void consumerCreated(long id, String address) {
    this.submit(
        () -> {
          String queue = extractQueueFromSourceAddress(address);
          if (queue != null) {
            this.consumers.put(id, new ConsumerSpec(id, queue));
          }
        });
  }

  @Override
  public void consumerDeleted(long id, String address) {
    this.submit(
        () -> {
          String queue = extractQueueFromSourceAddress(address);
          if (queue != null) {
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
          }
        });
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.loop.cancel(true);
    }
  }

  private void submit(Runnable task) {
    if (!this.closed.get()) {
      if (Thread.currentThread().equals(this.loopThread.get())) {
        task.run();
      } else {
        CountDownLatch latch = new CountDownLatch(1);
        try {
          boolean added =
              this.taskQueue.offer(
                  () -> {
                    try {
                      task.run();
                    } catch (Exception e) {
                      LOGGER.info("Error during topology recording task", e);
                    } finally {
                      latch.countDown();
                    }
                  },
                  TIMEOUT.toMillis(),
                  TimeUnit.MILLISECONDS);
          if (!added) {
            throw new ModelException("Enqueueing of topology task timed out");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ModelException("Topology task enqueueing has been interrupted", e);
        }
        try {
          latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ModelException("Topology task processing has been interrupted", e);
        }
      }
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
