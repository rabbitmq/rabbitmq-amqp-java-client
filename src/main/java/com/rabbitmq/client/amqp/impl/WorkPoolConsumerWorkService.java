// Copyright (c) 2025 Broadcom. All Rights Reserved.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

final class WorkPoolConsumerWorkService implements ConsumerWorkService {

  private static final int MAX_RUNNABLE_BLOCK_SIZE = 256;

  private final ExecutorService executor;
  private final WorkPool<AmqpConsumer, Runnable> workPool;

  WorkPoolConsumerWorkService(ExecutorService executorService, Duration queueingTimeout) {
    this.executor = executorService;
    this.workPool = new WorkPool<>(queueingTimeout);
  }

  @Override
  public void dispatch(AmqpConsumer consumer, Runnable runnable) {
    if (this.workPool.addWorkItem(consumer, runnable)) {
      this.executor.execute(new WorkPoolRunnable());
    }
  }

  @Override
  public void dispatch(Runnable runnable) {
    this.executor.execute(runnable);
  }

  @Override
  public void register(AmqpConsumer consumer) {
    this.workPool.registerKey(consumer);
  }

  @Override
  public void unregister(AmqpConsumer consumer) {
    this.workPool.unregisterKey(consumer);
  }

  @Override
  public void close() {
    this.workPool.unregisterAllKeys();
  }

  private final class WorkPoolRunnable implements Runnable {

    @Override
    public void run() {
      int size = MAX_RUNNABLE_BLOCK_SIZE;
      List<Runnable> block = new ArrayList<Runnable>(size);
      try {
        AmqpConsumer key = WorkPoolConsumerWorkService.this.workPool.nextWorkBlock(block, size);
        if (key == null) {
          return; // nothing ready to run
        }
        try {
          for (Runnable runnable : block) {
            runnable.run();
          }
        } finally {
          if (WorkPoolConsumerWorkService.this.workPool.finishWorkBlock(key)) {
            WorkPoolConsumerWorkService.this.executor.execute(new WorkPoolRunnable());
          }
        }
      } catch (RuntimeException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
