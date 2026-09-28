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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class SerialExecutorTest {

  ExecutorService pool;

  @AfterEach
  void tearDown() {
    if (this.pool != null) {
      this.pool.shutdownNow();
    }
  }

  @Test
  void tasksShouldRunInSubmissionOrder() {
    this.pool = Executors.newSingleThreadExecutor();
    SerialExecutor executor = new SerialExecutor(this.pool);
    int taskCount = 10_000;
    List<Integer> results = new CopyOnWriteArrayList<>();
    for (int i = 0; i < taskCount; i++) {
      int value = i;
      executor.execute(() -> results.add(value));
    }
    awaitQueueDrain(executor, taskCount, results);
    assertThat(results).hasSize(taskCount);
    for (int i = 0; i < taskCount; i++) {
      assertThat(results.get(i)).isEqualTo(i);
    }
  }

  @Test
  void tasksShouldNeverRunConcurrently() throws Exception {
    this.pool = Executors.newFixedThreadPool(8);
    SerialExecutor executor = new SerialExecutor(this.pool);
    int threadCount = 8;
    int taskCountPerThread = 10_000;
    AtomicInteger active = new AtomicInteger(0);
    AtomicInteger max = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(threadCount * taskCountPerThread);
    ExecutorService submitters = Executors.newFixedThreadPool(threadCount);
    try {
      for (int t = 0; t < threadCount; t++) {
        submitters.submit(
            () -> {
              for (int i = 0; i < taskCountPerThread; i++) {
                executor.execute(
                    () -> {
                      int current = active.incrementAndGet();
                      max.updateAndGet(m -> Math.max(m, current));
                      active.decrementAndGet();
                      latch.countDown();
                    });
              }
            });
      }
      assertThat(latch.await(30, SECONDS)).isTrue();
    } finally {
      submitters.shutdownNow();
    }
    assertThat(max.get()).isEqualTo(1);
  }

  @Test
  void stateShouldBeVisibleAcrossTasks() throws Exception {
    this.pool = Executors.newFixedThreadPool(8);
    SerialExecutor executor = new SerialExecutor(this.pool);
    int[] counter = {0};
    int threadCount = 8;
    int taskCountPerThread = 5_000;
    int totalTasks = threadCount * taskCountPerThread;
    CountDownLatch latch = new CountDownLatch(totalTasks);
    ExecutorService submitters = Executors.newFixedThreadPool(threadCount);
    try {
      for (int t = 0; t < threadCount; t++) {
        submitters.submit(
            () -> {
              for (int i = 0; i < taskCountPerThread; i++) {
                executor.execute(
                    () -> {
                      counter[0]++;
                      latch.countDown();
                    });
              }
            });
      }
      assertThat(latch.await(30, SECONDS)).isTrue();
    } finally {
      submitters.shutdownNow();
    }
    BlockingQueue<Integer> result = new ArrayBlockingQueue<>(1);
    executor.execute(() -> result.add(counter[0]));
    assertThat(result.poll(10, SECONDS)).isEqualTo(totalTasks);
  }

  @Test
  void taskExceptionShouldNotStopSubsequentTasks() throws Exception {
    this.pool = Executors.newSingleThreadExecutor();
    SerialExecutor executor = new SerialExecutor(this.pool);
    BlockingQueue<String> result = new ArrayBlockingQueue<>(1);
    executor.execute(
        () -> {
          throw new RuntimeException("expected failure");
        });
    executor.execute(() -> result.add("ok"));
    assertThat(result.poll(10, SECONDS)).isEqualTo("ok");
  }

  @Test
  void inExecutorShouldBeTrueOnlyInsideTasks() throws Exception {
    this.pool = Executors.newSingleThreadExecutor();
    SerialExecutor executor = new SerialExecutor(this.pool);
    assertThat(executor.inExecutor()).isFalse();
    BlockingQueue<Boolean> result = new ArrayBlockingQueue<>(1);
    executor.execute(() -> result.add(executor.inExecutor()));
    assertThat(result.poll(10, SECONDS)).isTrue();
    assertThat(executor.inExecutor()).isFalse();
  }

  @Test
  void taskSubmittedFromTaskShouldRunAfterCurrentTask() throws Exception {
    this.pool = Executors.newSingleThreadExecutor();
    SerialExecutor executor = new SerialExecutor(this.pool);
    List<String> order = new CopyOnWriteArrayList<>();
    BlockingQueue<Boolean> done = new ArrayBlockingQueue<>(1);
    executor.execute(
        () -> {
          order.add("first");
          executor.execute(
              () -> {
                order.add("nested");
                done.add(true);
              });
          order.add("first-after-nested-submit");
        });
    assertThat(done.poll(10, SECONDS)).isTrue();
    assertThat(order).containsExactly("first", "first-after-nested-submit", "nested");
  }

  @Test
  void longQueueShouldYieldDelegateThread() throws Exception {
    // MAX_BATCH is 64: hold the delegate thread with a gate so submission of the serial tasks,
    // then of the direct task, is fully ordered before any of them runs; this makes the resulting
    // execution order on the single delegate thread deterministic.
    this.pool = Executors.newSingleThreadExecutor();
    CountDownLatch gate = new CountDownLatch(1);
    this.pool.execute(() -> awaitUninterruptibly(gate));
    SerialExecutor executor = new SerialExecutor(this.pool);
    List<String> order = new CopyOnWriteArrayList<>();
    CountDownLatch serialTasksDone = new CountDownLatch(1);
    int serialTaskCount = 200;
    for (int i = 0; i < serialTaskCount; i++) {
      boolean last = i == serialTaskCount - 1;
      executor.execute(
          () -> {
            order.add("serial");
            if (last) {
              serialTasksDone.countDown();
            }
          });
    }
    this.pool.execute(() -> order.add("direct"));
    gate.countDown();
    assertThat(serialTasksDone.await(30, SECONDS)).isTrue();
    assertThat(order).contains("direct");
    assertThat(order.indexOf("direct")).isLessThan(order.size() - 1);
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    try {
      latch.await(30, SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  void rejectionFromDelegateShouldPropagate() {
    ExecutorService rejecting = Executors.newSingleThreadExecutor();
    rejecting.shutdownNow();
    SerialExecutor executor = new SerialExecutor(rejecting);
    assertThatThrownBy(() -> executor.execute(() -> {}))
        .isInstanceOf(RejectedExecutionException.class);
  }

  private static void awaitQueueDrain(
      SerialExecutor executor, int expectedSize, List<Integer> results) {
    BlockingQueue<Boolean> done = new ArrayBlockingQueue<>(1);
    executor.execute(() -> done.add(true));
    try {
      done.poll(30, SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
