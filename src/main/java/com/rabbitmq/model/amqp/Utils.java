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
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  private static final Function<String, ExecutorService> EXECUTOR_SERVICE_FACTORY;

  private static final BiFunction<String, Runnable, Thread> THREAD_FACTORY;

  static {
    if (isJava21OrMore()) {
      LOGGER.debug("Running Java 21 or more, using virtual threads");
      Class<?> builderClass =
          Arrays.stream(Thread.class.getDeclaredClasses())
              .filter(c -> "Builder".equals(c.getSimpleName()))
              .findFirst()
              .get();
      EXECUTOR_SERVICE_FACTORY =
          prefix -> {
            try {
              // Reflection code is the same as the 2 following lines:
              // ThreadFactory factory = Thread.ofVirtual().name(prefix, 0).factory();
              // Executors.newThreadPerTaskExecutor(factory);
              Object builder = Thread.class.getDeclaredMethod("ofVirtual").invoke(null);
              if (prefix != null) {
                builder =
                    builderClass
                        .getDeclaredMethod("name", String.class, Long.TYPE)
                        .invoke(builder, prefix, 0L);
              }
              ThreadFactory factory =
                  (ThreadFactory) builderClass.getDeclaredMethod("factory").invoke(builder);
              return (ExecutorService)
                  Executors.class
                      .getDeclaredMethod("newThreadPerTaskExecutor", ThreadFactory.class)
                      .invoke(null, factory);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
              throw new RuntimeException(e);
            }
          };
      THREAD_FACTORY =
          (name, operation) -> {
            // Reflection code is the same as the following line:
            // Thread.ofVirtual().name(name).unstarted(operation);
            try {
              Object builder = Thread.class.getDeclaredMethod("ofVirtual").invoke(null);
              builder = builderClass.getDeclaredMethod("name", String.class).invoke(builder, name);
              return (Thread)
                  builderClass
                      .getDeclaredMethod("unstarted", Runnable.class)
                      .invoke(builder, operation);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
              throw new RuntimeException(e);
            }
          };
    } else {
      EXECUTOR_SERVICE_FACTORY = prefix -> Executors.newCachedThreadPool(threadFactory(prefix));
      THREAD_FACTORY =
          (name, operation) -> {
            Thread thread = Executors.defaultThreadFactory().newThread(operation);
            thread.setName(name);
            return thread;
          };
    }
  }

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

  static ExecutorService executorService() {
    return EXECUTOR_SERVICE_FACTORY.apply(null);
  }

  static Thread newThread(String name, Runnable operation) {
    return THREAD_FACTORY.apply(name, operation);
  }

  private static boolean isJava21OrMore() {
    return Runtime.version().compareTo(Runtime.Version.parse("21")) >= 0;
  }

  private static class NamedThreadFactory implements ThreadFactory {

    private final ThreadFactory backingThreaFactory;

    private final String prefix;

    private final AtomicLong count = new AtomicLong(0);

    private NamedThreadFactory(String prefix) {
      this(Executors.defaultThreadFactory(), prefix);
    }

    private NamedThreadFactory(ThreadFactory backingThreadFactory, String prefix) {
      this.backingThreaFactory = backingThreadFactory;
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = this.backingThreaFactory.newThread(r);
      thread.setName(prefix + count.getAndIncrement());
      return thread;
    }
  }

  static ThreadFactory threadFactory(String prefix) {
    if (prefix == null) {
      return Executors.defaultThreadFactory();
    } else {
      return new NamedThreadFactory(prefix);
    }
  }
}
