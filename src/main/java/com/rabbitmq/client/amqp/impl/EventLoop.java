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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class EventLoop<S> implements AutoCloseable {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final Logger LOGGER = LoggerFactory.getLogger(EventLoop.class);

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final String label;
  private final Future<?> loop;
  private final AtomicReference<Thread> loopThread = new AtomicReference<>();
  private final AtomicReference<S> stateReference = new AtomicReference<>();
  private final BlockingQueue<Consumer<S>> taskQueue = new ArrayBlockingQueue<>(100);

  EventLoop(Supplier<S> stateSupplier, String label, ExecutorService executorService) {
    this.label = label;
    CountDownLatch loopThreadSetLatch = new CountDownLatch(1);
    this.loop =
        executorService.submit(
            () -> {
              S state = stateSupplier.get();
              loopThread.set(Thread.currentThread());
              stateReference.set(state);
              loopThreadSetLatch.countDown();
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  Consumer<S> task = this.taskQueue.take();
                  task.accept(state);
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
      throw new AmqpException("Error while creating recording topology listener", e);
    }
  }

  void submit(Consumer<S> task) {
    if (this.closed.get()) {
      throw new IllegalStateException("Event loop is closed");
    } else {
      if (Thread.currentThread().equals(this.loopThread.get())) {
        task.accept(this.stateReference.get());
      } else {
        CountDownLatch latch = new CountDownLatch(1);
        try {
          boolean added =
              this.taskQueue.offer(
                  state -> {
                    try {
                      task.accept(state);
                    } catch (Exception e) {
                      LOGGER.info("Error during {} task", this.label, e);
                    } finally {
                      latch.countDown();
                    }
                  },
                  TIMEOUT.toMillis(),
                  TimeUnit.MILLISECONDS);
          if (!added) {
            throw new AmqpException("Enqueueing of %s task timed out", this.label);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AmqpException(this.label + " task enqueueing has been interrupted", e);
        }
        try {
          boolean completed = latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
          if (!completed) {
            LOGGER.warn("Event loop task did not complete in {} second(s)", TIMEOUT.toSeconds());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AmqpException(this.label + " Topology task processing has been interrupted", e);
        }
      }
    }
  }

  S state() {
    return this.stateReference.get();
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.loop.cancel(true);
    }
  }
}
