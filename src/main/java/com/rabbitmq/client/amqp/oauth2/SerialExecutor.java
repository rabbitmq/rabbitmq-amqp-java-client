// Copyright (c) 2024-2025 Broadcom. All Rights Reserved.
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
package com.rabbitmq.client.amqp.oauth2;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs submitted tasks one at a time, in submission order, on top of a delegate {@link Executor}.
 *
 * <p>Has no thread of its own. State confined to tasks run through the same instance can use plain
 * fields: the queue and the scheduling flag provide the happens-before relationship between
 * consecutive tasks.
 */
final class SerialExecutor implements Executor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerialExecutor.class);
  private static final int MAX_BATCH = 64;

  private final Executor delegate;
  private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean scheduled = new AtomicBoolean(false);
  private volatile Thread runner;

  SerialExecutor(Executor delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public void execute(Runnable task) {
    tasks.add(Objects.requireNonNull(task));
    schedule();
  }

  boolean inExecutor() {
    return runner == Thread.currentThread();
  }

  private void schedule() {
    if (scheduled.compareAndSet(false, true)) {
      try {
        delegate.execute(this::drain);
      } catch (RejectedExecutionException e) {
        scheduled.set(false);
        throw e;
      }
    }
  }

  private void drain() {
    runner = Thread.currentThread();
    try {
      Runnable task;
      int count = 0;
      while (count++ < MAX_BATCH && (task = tasks.poll()) != null) {
        try {
          task.run();
        } catch (Throwable t) {
          LOGGER.warn("Error in serial executor task", t);
        }
      }
    } finally {
      runner = null;
      scheduled.set(false);
      if (!tasks.isEmpty()) {
        try {
          schedule();
        } catch (RejectedExecutionException e) {
          LOGGER.debug("Could not reschedule serial executor, delegate rejected it", e);
        }
      }
    }
  }
}
