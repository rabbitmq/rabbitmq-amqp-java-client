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
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class EventLoop implements AutoCloseable {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final Logger LOGGER = LoggerFactory.getLogger(EventLoop.class);

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Future<?> loop;
  private final AtomicReference<Thread> loopThread = new AtomicReference<>();
  private final BlockingQueue<ClientTaskContext<Object>> taskQueue =
      new LinkedBlockingQueue<>(1000);

  EventLoop(ExecutorService executorService) {
    CountDownLatch loopThreadSetLatch = new CountDownLatch(1);
    this.loop =
        executorService.submit(
            () -> {
              loopThread.set(Thread.currentThread());
              loopThreadSetLatch.countDown();
              Map<Long, Object> states = new HashMap<>();
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  ClientTaskContext<Object> context =
                      this.taskQueue.poll(1000, TimeUnit.MILLISECONDS);
                  if (context != null) {
                    Object state = states.get(context.client.id);
                    state = context.task.apply(state);
                    if (state == null) {
                      states.remove(context.client.id);
                    } else {
                      states.put(context.client.id, state);
                    }
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
    Client<S> client = new Client<>(this);
    this.submit(
        client,
        nullState -> {
          S state = stateSupplier.get();
          client.stateReference.set(state);
          return state;
        });
    return client;
  }

  private <ST> void submit(Client<ST> client, UnaryOperator<ST> task) {
    if (this.closed.get()) {
      throw new IllegalStateException("Event loop is closed");
    } else {
      if (Thread.currentThread().equals(this.loopThread.get())) {
        task.apply(client.stateReference.get());
      } else {
        CountDownLatch latch = new CountDownLatch(1);
        try {
          ClientTaskContext<ST> context =
              new ClientTaskContext<>(
                  client,
                  state -> {
                    try {
                      return task.apply(state);
                    } catch (Exception e) {
                      LOGGER.info("Error during task", e);
                    } finally {
                      latch.countDown();
                    }
                    return null;
                  });
          @SuppressWarnings("unchecked")
          boolean added =
              this.taskQueue.offer(
                  (ClientTaskContext<Object>) context, TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.loop.cancel(true);
    }
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
        throw new IllegalStateException("Event loop is closed");
      } else {
        this.loop.submit(
            this,
            s -> {
              task.accept(s);
              return s;
            });
      }
    }

    @Override
    public void close() {
      if (this.closed.compareAndSet(false, true)) {
        this.loop.submit(this, s -> null);
      }
    }

    S state() {
      return this.stateReference.get();
    }
  }

  private static class ClientTaskContext<S> {

    private final Client<S> client;
    private final UnaryOperator<S> task;

    private ClientTaskContext(Client<S> client, UnaryOperator<S> task) {
      this.client = client;
      this.task = task;
    }
  }
}
