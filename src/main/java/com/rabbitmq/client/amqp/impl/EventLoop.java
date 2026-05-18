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
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
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
  private final io.netty.channel.EventLoop nettyLoop;
  private final Map<Long, Object> activeClients = new HashMap<>();

  EventLoop(EventLoopGroup eventLoopGroup) {
    this.nettyLoop = eventLoopGroup.next();
  }

  <S> Client<S> register(Supplier<S> stateSupplier) {
    if (this.closed.get()) {
      throw new IllegalStateException("Event loop is closed");
    }
    Client<S> client = new Client<>(this);
    CountDownLatch latch = new CountDownLatch(1);

    nettyLoop.execute(
        () -> {
          try {
            S state = stateSupplier.get();
            client.stateReference.set(state);
            activeClients.put(client.id, state);
          } catch (Exception e) {
            LOGGER.warn("Error during registration", e);
          } finally {
            latch.countDown();
          }
        });

    try {
      boolean completed = latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      if (!completed) {
        LOGGER.warn(
            "Event loop registration did not complete in {} second(s)", TIMEOUT.toSeconds());
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
    if (nettyLoop.inEventLoop()) {
      if (!client.closed.get()) {
        try {
          @SuppressWarnings("unchecked")
          S clientState = (S) activeClients.get(client.id);
          if (clientState != null) {
            TaskResult result = task.apply(clientState);
            if (result == TaskResult.STOP) {
              activeClients.remove(client.id);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error during task", e);
        }
      }
    } else {
      CountDownLatch latch = new CountDownLatch(1);

      nettyLoop.execute(
          () -> {
            try {
              if (!client.closed.get()) {
                @SuppressWarnings("unchecked")
                S clientState = (S) activeClients.get(client.id);
                if (clientState != null) {
                  TaskResult result = task.apply(clientState);
                  if (result == TaskResult.STOP) {
                    activeClients.remove(client.id);
                  }
                }
              }
            } catch (Exception e) {
              LOGGER.warn("Error during task", e);
            } finally {
              latch.countDown();
            }
          });

      try {
        boolean completed = latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (!completed) {
          LOGGER.warn("Event loop task did not complete in {} second(s)", TIMEOUT.toSeconds());
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
      nettyLoop.execute(activeClients::clear);
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

    // for testing
    <R> R query(Function<S, R> queryFunction) {
      AtomicReference<R> result = new AtomicReference<>();
      this.loop.submit(
          this,
          s -> {
            result.set(queryFunction.apply(s));
            return TaskResult.CONTINUE;
          });
      return result.get();
    }

    @Override
    public void close() {
      if (this.closed.compareAndSet(false, true)) {
        try {
          this.loop.submit(this, s -> TaskResult.STOP);
        } catch (IllegalStateException e) {
          // event loop already closed
        }
      }
    }
  }
}
