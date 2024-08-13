// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AsyncRetryTest {

  ScheduledExecutorService scheduler;
  @Mock Callable<Integer> task;
  AutoCloseable mocks;

  @BeforeEach
  void init() {
    mocks = MockitoAnnotations.openMocks(this);
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterEach
  void tearDown() throws Exception {
    this.scheduler.shutdownNow();
    mocks.close();
  }

  @Test
  void callbackCalledIfCompletedImmediately() throws Exception {
    when(task.call()).thenReturn(42);
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task)
            .delayPolicy(
                BackOffDelayPolicy.fixedWithInitialDelay(Duration.ZERO, Duration.ofMillis(10)))
            .scheduler(scheduler)
            .build();
    AtomicInteger result = new AtomicInteger(0);
    completableFuture.thenAccept(value -> result.set(value));
    assertThat(result.get()).isEqualTo(42);
    verify(task, times(1)).call();
  }

  @Test
  void shouldRetryWhenExecutionFails() throws Exception {
    when(task.call())
        .thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException())
        .thenReturn(42);
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task).scheduler(scheduler).delay(Duration.ofMillis(50)).build();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger result = new AtomicInteger(0);
    completableFuture.thenAccept(
        value -> {
          result.set(value);
          latch.countDown();
        });
    assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(result.get()).isEqualTo(42);
    verify(task, times(3)).call();
  }

  @Test
  void shouldTimeoutWhenExecutionFailsForTooLong() throws Exception {
    when(task.call()).thenThrow(new RuntimeException());
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task)
            .scheduler(this.scheduler)
            .delayPolicy(
                BackOffDelayPolicy.fixedWithInitialDelay(
                    Duration.ofMillis(50), Duration.ofMillis(50), Duration.ofMillis(500)))
            .build();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean acceptCalled = new AtomicBoolean(false);
    AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
    completableFuture
        .thenAccept(
            value -> {
              acceptCalled.set(true);
            })
        .exceptionally(
            e -> {
              exceptionallyCalled.set(true);
              latch.countDown();
              return null;
            });
    assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(acceptCalled.get()).isFalse();
    assertThat(exceptionallyCalled.get()).isTrue();
    verify(task, atLeast(5)).call();
  }

  @Test
  void shouldRetryWhenPredicateAllowsIt() throws Exception {
    when(task.call())
        .thenThrow(new IllegalStateException())
        .thenThrow(new IllegalStateException())
        .thenReturn(42);
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task)
            .scheduler(scheduler)
            .retry(e -> e instanceof IllegalStateException)
            .delay(Duration.ofMillis(50))
            .build();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger result = new AtomicInteger(0);
    completableFuture.thenAccept(
        value -> {
          result.set(value);
          latch.countDown();
        });
    assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(result.get()).isEqualTo(42);
    verify(task, times(3)).call();
  }

  @Test
  void shouldFailWhenPredicateDoesNotAllowRetry() throws Exception {
    when(task.call())
        .thenThrow(new IllegalStateException())
        .thenThrow(new IllegalStateException())
        .thenThrow(new IllegalArgumentException());
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task)
            .scheduler(scheduler)
            .retry(e -> !(e instanceof IllegalArgumentException))
            .delay(Duration.ofMillis(50))
            .build();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean acceptCalled = new AtomicBoolean(false);
    AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
    completableFuture
        .thenAccept(
            value -> {
              acceptCalled.set(true);
            })
        .exceptionally(
            e -> {
              exceptionallyCalled.set(true);
              latch.countDown();
              return null;
            });
    assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(acceptCalled.get()).isFalse();
    assertThat(exceptionallyCalled.get()).isTrue();
    verify(task, times(3)).call();
  }
}
