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

import com.rabbitmq.client.amqp.AmqpException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class EventLoop implements AutoCloseable {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final Logger LOGGER = LoggerFactory.getLogger(EventLoop.class);

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Future<?> loop;
  private final AtomicReference<Thread> loopThread = new AtomicReference<>();
  private final BlockingQueue<ClientTaskContext<?>> taskQueue = new LinkedBlockingQueue<>(1000);

  EventLoop(ExecutorService executorService) {
    CountDownLatch loopThreadSetLatch = new CountDownLatch(1);
    this.loop =
        executorService.submit(
            () -> {
              loopThread.set(Thread.currentThread());
              loopThreadSetLatch.countDown();
              Map<Long, Object> activeClients = new HashMap<>();
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  ClientTaskContext<?> context = this.taskQueue.poll(1000, TimeUnit.MILLISECONDS);
                  if (context != null) {
                    if (context.registration) {
                      context.task.apply(null);
                      activeClients.put(context.client.id, context.client.stateReference.get());
                      context.complete();
                      continue;
                    }
                    Object clientState = activeClients.get(context.client.id);
                    if (clientState == null) {
                      context.complete();
                      continue;
                    }
                    TaskResult result = context.task.apply(clientState);
                    if (result == TaskResult.STOP) {
                      activeClients.remove(context.client.id);
                    }
                    context.complete();
                  }
                } catch (InterruptedException e) {
                  LOGGER.debug("Event loop has been interrupted.");
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
      throw new AmqpException("Error while creating recording topology listener", e);
    }
  }

  <S> Client<S> register(Supplier<S> stateSupplier) {
    if (this.closed.get()) {
      throw new IllegalStateException("Event loop is closed");
    }
    Client<S> client = new Client<>(this);
    CountDownLatch latch = new CountDownLatch(1);
    try {
      ClientTaskContext context =
          new ClientTaskContext(
              client,
              s -> {
                S state = stateSupplier.get();
                client.stateReference.set(state);
                return TaskResult.CONTINUE;
              },
              latch,
              true);
      boolean added = this.taskQueue.offer(context, TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      if (!added) {
        throw new AmqpException("Enqueueing of registration timed out");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AmqpException("Registration enqueueing has been interrupted", e);
    }
    try {
      boolean completed = latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      if (!completed) {
        LOGGER.warn(
            "Event loop registration did not complete in {} second(s), queue size is {}",
            TIMEOUT.toSeconds(),
            this.taskQueue.size());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AmqpException("Registration processing has been interrupted", e);
    }
    return client;
  }

  private <S> void submit(Client<S> client, Function<S, TaskResult> task) {
    if (this.closed.get()) {
      throw new IllegalStateException("Event loop is closed");
    }
    if (Thread.currentThread().equals(this.loopThread.get())) {
      try {
        task.apply(client.stateReference.get());
      } catch (Exception e) {
        LOGGER.warn("Error during task", e);
      }
    } else {
      CountDownLatch latch = new CountDownLatch(1);
      try {
        @SuppressWarnings("unchecked")
        ClientTaskContext context =
            new ClientTaskContext(
                client,
                s -> {
                  try {
                    return task.apply((S) s);
                  } catch (Exception e) {
                    LOGGER.warn("Error during task", e);
                    return TaskResult.CONTINUE;
                  }
                },
                latch,
                false);
        boolean added = this.taskQueue.offer(context, TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (!added) {
          throw new AmqpException("Enqueueing of task timed out");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AmqpException("Task enqueueing has been interrupted", e);
      }
      try {
        boolean completed = latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (!completed) {
          LOGGER.warn(
              "Event loop task did not complete in {} second(s), queue size is {}",
              TIMEOUT.toSeconds(),
              this.taskQueue.size());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AmqpException("Topology task processing has been interrupted", e);
      }
    }
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.loop.cancel(true);
    }
  }

  enum TaskResult {
    CONTINUE,
    STOP
  }

  private static final AtomicLong CLIENT_ID_SEQUENCE = new AtomicLong();

  static class Client<S> implements AutoCloseable {

    private final long id;
    private final AtomicReference<S> stateReference = new AtomicReference<>();
    private final EventLoop loop;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Client(EventLoop loop) {
      this.id = CLIENT_ID_SEQUENCE.getAndIncrement();
      this.loop = loop;
    }

    void submit(Consumer<S> task) {
      if (this.closed.get()) {
        throw new IllegalStateException("Client is closed");
      }
      this.loop.submit(
          this,
          s -> {
            task.accept(s);
            return TaskResult.CONTINUE;
          });
    }

    @Override
    public void close() {
      if (this.closed.compareAndSet(false, true)) {
        this.loop.submit(this, s -> TaskResult.STOP);
      }
    }

    S state() {
      return this.stateReference.get();
    }
  }

  private static class ClientTaskContext<S> {

    private final Client<S> client;
    private final Function<Object, TaskResult> task;
    private final CountDownLatch latch;
    private final boolean registration;

    private ClientTaskContext(
        Client<S> client,
        Function<Object, TaskResult> task,
        CountDownLatch latch,
        boolean registration) {
      this.client = client;
      this.task = task;
      this.latch = latch;
      this.registration = registration;
    }

    void complete() {
      if (this.latch != null) {
        this.latch.countDown();
      }
    }
  }
}
