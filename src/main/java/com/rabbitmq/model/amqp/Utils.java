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

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.*;

abstract class Utils {

  private Utils() {}

  static <T> CompletableFuture<T> makeCompletableFuture(Future<T> future) {
    if (future.isDone()) return transformDoneFuture(future);
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!future.isDone()) {
              awaitFutureIsDoneInForkJoinPool(future);
            }
            return future.get();
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new RuntimeException(e);
          }
        });
  }

  private static <T> CompletableFuture<T> transformDoneFuture(Future<T> future) {
    CompletableFuture<T> cf = new CompletableFuture<>();
    T result;
    try {
      result = future.get();
    } catch (Throwable ex) {
      cf.completeExceptionally(ex);
      return cf;
    }
    cf.complete(result);
    return cf;
  }

  private static void awaitFutureIsDoneInForkJoinPool(Future<?> future)
      throws InterruptedException {
    ForkJoinPool.managedBlock(
        new ForkJoinPool.ManagedBlocker() {
          @Override
          public boolean block() throws InterruptedException {
            try {
              future.get();
            } catch (ExecutionException e) {
              throw new RuntimeException(e);
            }
            return true;
          }

          @Override
          public boolean isReleasable() {
            return future.isDone();
          }
        });
  }

  static ExecutorService virtualThreadExecutorServiceIfAvailable() {
    boolean java21OrMore = Runtime.version().compareTo(Runtime.Version.parse("21")) >= 0;
    if (java21OrMore) {
      try {
        return (ExecutorService)
            Executors.class.getDeclaredMethod("newVirtualThreadPerTaskExecutor").invoke(null);
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    } else {
      return Executors.newCachedThreadPool();
    }
  }
}
