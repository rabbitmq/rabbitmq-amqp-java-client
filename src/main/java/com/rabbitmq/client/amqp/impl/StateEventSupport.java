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

import com.rabbitmq.client.amqp.Resource;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StateEventSupport {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateEventSupport.class);

  private final List<Resource.StateListener> listeners;
  private final Executor executor;
  private final Queue<Resource.Context> queue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean draining = new AtomicBoolean(false);

  StateEventSupport(List<Resource.StateListener> listeners, Executor executor) {
    this.listeners = List.copyOf(listeners);
    this.executor = executor;
  }

  void dispatch(
      Resource resource,
      Throwable failureCause,
      Resource.State previousState,
      Resource.State currentState) {
    if (this.listeners.isEmpty()) {
      return;
    }

    this.queue.add(new DefaultContext(resource, failureCause, previousState, currentState));
    scheduleDrain();
  }

  private void scheduleDrain() {
    // The atomic lock ensures only one thread drains the queue at a time
    if (this.draining.compareAndSet(false, true)) {
      try {
        this.executor.execute(this::drain);
      } catch (Exception e) {
        // If the executor rejects the task (e.g., during JVM shutdown), release the lock
        this.draining.set(false);
        LOGGER.debug("Could not schedule state event dispatching", e);
      }
    }
  }

  private void drain() {
    try {
      Resource.Context context;
      // Drain strictly chronologically
      while ((context = this.queue.poll()) != null) {
        for (Resource.StateListener listener : this.listeners) {
          try {
            listener.handle(context);
          } catch (Exception e) {
            LOGGER.warn("Error in resource listener", e);
          }
        }
      }
    } finally {
      // Release the lock
      this.draining.set(false);
      // Double-check to prevent race conditions if an event was added right as we finished
      if (!this.queue.isEmpty()) {
        scheduleDrain();
      }
    }
  }

  private static class DefaultContext implements Resource.Context {

    private final Resource resource;
    private final Throwable failureCause;
    private final Resource.State previousState;
    private final Resource.State currentState;

    private DefaultContext(
        Resource resource,
        Throwable failureCause,
        Resource.State previousState,
        Resource.State currentState) {
      this.resource = resource;
      this.failureCause = failureCause;
      this.previousState = previousState;
      this.currentState = currentState;
    }

    @Override
    public Resource resource() {
      return this.resource;
    }

    @Override
    public Throwable failureCause() {
      return this.failureCause;
    }

    @Override
    public Resource.State previousState() {
      return this.previousState;
    }

    @Override
    public Resource.State currentState() {
      return this.currentState;
    }
  }
}
