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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * This is based on the <code>WorkPool</code> class from RabbitMQ AMQP 091 Java client library. The
 * source code is available <a
 * href="https://github.com/rabbitmq/rabbitmq-java-client/blob/main/src/main/java/com/rabbitmq/client/impl/WorkPool.java">here</a>.
 *
 * <p>This is a generic implementation of the channels specification in <i>Channeling Work</i>, Nov
 * 2010 (<tt>channels.pdf</tt>). Objects of type <b>K</b> must be registered, with <code>
 * <b>registerKey(K)</b></code>, and then they become <i>clients</i> and a queue of items (type
 * <b>W</b>) is stored for each client. Each client has a <i>state</i> which is exactly one of
 * <i>dormant</i>, <i>in progress</i> or <i>ready</i>. Immediately after registration a client is
 * <i>dormant</i>. Items may be (singly) added to (the end of) a client's queue with {@link
 * WorkPool#addWorkItem(Object, Object)}. If the client is <i>dormant</i> it becomes <i>ready</i>
 * thereby. All other states remain unchanged. The next <i>ready</i> client, together with a
 * collection of its items, may be retrieved with <code><b>nextWorkBlock(collection,max)</b></code>
 * (making that client <i>in progress</i>). An <i>in progress</i> client can finish (processing a
 * batch of items) with <code><b>finishWorkBlock(K)</b></code>. It then becomes either
 * <i>dormant</i> or <i>ready</i>, depending if its queue of work items is empty or no. If a client
 * has items queued, it is either <i>in progress</i> or <i>ready</i> but cannot be both. When work
 * is finished it may be marked <i>ready</i> if there is further work, or <i>dormant</i> if there is
 * not. There is never any work for a <i>dormant</i> client. A client may be unregistered, with
 * <code><b>unregisterKey(K)</b></code>, which removes the client from all parts of the state, and
 * any queue of items stored with it. All clients may be unregistered with <code>
 * <b>unregisterAllKeys()</b></code>.
 *
 * <h2>Concurrent Semantics</h2>
 *
 * This implementation is thread-safe.
 *
 * @param <K> Key -- type of client
 * @param <W> Work -- type of work item
 */
final class WorkPool<K, W> {
  private static final int MAX_QUEUE_LENGTH = 1000;

  /** An injective queue of <i>ready</i> clients. */
  private final SetQueue<K> ready = new SetQueue<>();

  /** The set of clients which have work <i>in progress</i>. */
  private final Set<K> inProgress = new HashSet<>();

  /** The pool of registered clients, with their work queues. */
  private final Map<K, LinkedBlockingQueue<W>> pool = new HashMap<>();

  private final BiConsumer<LinkedBlockingQueue<W>, W> enqueueingCallback;
  private final Lock lock = new ReentrantLock();

  public WorkPool(Duration queueingTimeout) {
    if (queueingTimeout.toNanos() > 0) {
      long timeout = queueingTimeout.toMillis();
      this.enqueueingCallback =
          (queue, item) -> {
            try {
              boolean offered = queue.offer(item, timeout, TimeUnit.MILLISECONDS);
              if (!offered) {
                throw new WorkPoolFullException(
                    "Could not enqueue in work pool after " + queueingTimeout + " ms.");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          };
    } else {
      this.enqueueingCallback =
          (queue, item) -> {
            try {
              queue.put(item);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          };
    }
  }

  /**
   * Add client <code><b>key</b></code> to pool of item queues, with an empty queue. A client is
   * initially <i>dormant</i>. No-op if <code><b>key</b></code> already present.
   *
   * @param key client to add to pool
   */
  public void registerKey(K key) {
    this.lock.lock();
    try {
      if (!this.pool.containsKey(key)) {
        this.pool.put(key, new LinkedBlockingQueue<>(MAX_QUEUE_LENGTH));
      }
    } finally {
      this.lock.unlock();
    }
  }

  /**
   * Remove client from pool and from any other state. Has no effect if client already absent.
   *
   * @param key of client to unregister
   */
  public void unregisterKey(K key) {
    this.lock.lock();
    try {
      this.pool.remove(key);
      this.ready.remove(key);
      this.inProgress.remove(key);
    } finally {
      this.lock.unlock();
    }
  }

  /** Remove all clients from pool and from any other state. */
  public void unregisterAllKeys() {
    this.lock.lock();
    try {
      this.pool.clear();
      this.ready.clear();
      this.inProgress.clear();
    } finally {
      this.lock.unlock();
    }
  }

  /**
   * Return the next <i>ready</i> client, and transfer a collection of that client's items to
   * process. Mark client <i>in progress</i>. If there is no <i>ready</i> client, return <code>
   * <b>null</b></code>.
   *
   * @param to collection object in which to transfer items
   * @param size max number of items to transfer
   * @return key of client to whom items belong, or <code><b>null</b></code> if there is none.
   */
  public K nextWorkBlock(Collection<W> to, int size) {
    this.lock.lock();
    try {
      K nextKey = readyToInProgress();
      if (nextKey != null) {
        LinkedBlockingQueue<W> queue = this.pool.get(nextKey);
        drainTo(queue, to, size);
      }
      return nextKey;
    } finally {
      this.lock.unlock();
    }
  }

  /**
   * Private implementation of <code><b>drainTo</b></code> (not implemented for <code>
   * <b>LinkedList&lt;W&gt;</b></code>s).
   *
   * @param deList to take (poll) elements from
   * @param c to add elements to
   * @param maxElements to take from deList
   * @return number of elements actually taken
   */
  private int drainTo(LinkedBlockingQueue<W> deList, Collection<W> c, int maxElements) {
    int n = 0;
    while (n < maxElements) {
      W first = deList.poll();
      if (first == null) break;
      c.add(first);
      ++n;
    }
    return n;
  }

  /**
   * Add (enqueue) an item for a specific client. No change and returns <code><b>false</b></code> if
   * client not registered. If <i>dormant</i>, the client will be marked <i>ready</i>.
   *
   * @param key the client to add to the work item to
   * @param item the work item to add to the client queue
   * @return <code><b>true</b></code> if and only if the client is marked <i>ready</i> &mdash; <i>as
   *     a result of this work item</i>
   */
  public boolean addWorkItem(K key, W item) {
    LinkedBlockingQueue<W> queue;
    this.lock.lock();
    try {
      queue = this.pool.get(key);
    } finally {
      this.lock.unlock();
    }
    // The put operation may block. We need to make sure we are not holding the lock while that
    // happens.
    if (queue != null) {
      enqueueingCallback.accept(queue, item);

      this.lock.lock();
      try {
        if (isDormant(key)) {
          dormantToReady(key);
          return true;
        }
      } finally {
        this.lock.unlock();
      }
    }
    return false;
  }

  /**
   * Set client no longer <i>in progress</i>. Ignore unknown clients (and return <code><b>false</b>
   * </code>).
   *
   * @param key client that has finished work
   * @return <code><b>true</b></code> if and only if client becomes <i>ready</i>
   * @throws IllegalStateException if registered client not <i>in progress</i>
   */
  public boolean finishWorkBlock(K key) {
    this.lock.lock();
    try {
      if (!this.isRegistered(key)) return false;
      if (!this.inProgress.contains(key)) {
        throw new IllegalStateException("Client " + key + " not in progress");
      }

      if (moreWorkItems(key)) {
        inProgressToReady(key);
        return true;
      } else {
        inProgressToDormant(key);
        return false;
      }
    } finally {
      this.lock.unlock();
    }
  }

  private boolean moreWorkItems(K key) {
    LinkedBlockingQueue<W> leList = this.pool.get(key);
    return leList != null && !leList.isEmpty();
  }

  /* State identification functions */
  private boolean isInProgress(K key) {
    return this.inProgress.contains(key);
  }

  private boolean isReady(K key) {
    return this.ready.contains(key);
  }

  private boolean isRegistered(K key) {
    return this.pool.containsKey(key);
  }

  private boolean isDormant(K key) {
    return !isInProgress(key) && !isReady(key) && isRegistered(key);
  }

  /* State transition methods - all assume key registered */
  private void inProgressToReady(K key) {
    this.inProgress.remove(key);
    this.ready.addIfNotPresent(key);
  }

  private void inProgressToDormant(K key) {
    this.inProgress.remove(key);
  }

  private void dormantToReady(K key) {
    this.ready.addIfNotPresent(key);
  }

  /* Basic work selector and state transition step */
  private K readyToInProgress() {
    K key = this.ready.poll();
    if (key != null) {
      this.inProgress.add(key);
    }
    return key;
  }

  /**
   * A generic queue-like implementation (supporting operations <code>addIfNotPresent</code>, <code>
   * poll</code>, <code>contains</code>, and <code>isEmpty</code>) which restricts a queue element
   * to appear at most once. If the element is already present {@link #addIfNotPresent} returns
   * <code><b>false</b></code>. Elements must not be <code><b>null</b></code>.
   *
   * <h2>Concurrent Semantics</h2>
   *
   * This implementation is <i>not</i> thread-safe.
   *
   * @param <T> type of elements in the queue
   */
  private static final class SetQueue<T> {
    private final Set<T> members = new HashSet<T>();
    private final Queue<T> queue = new LinkedList<T>();

    /**
     * Add an element to the back of the queue and return <code><b>true</b></code>, or else return
     * <code><b>false</b></code>.
     *
     * @param item to add
     * @return <b><code>true</code></b> if the element was added, <b><code>false</code></b> if it is
     *     already present.
     */
    public boolean addIfNotPresent(T item) {
      if (this.members.contains(item)) {
        return false;
      }
      this.members.add(item);
      this.queue.offer(item);
      return true;
    }

    /**
     * Remove the head of the queue and return it.
     *
     * @return head element of the queue, or <b><code>null</code></b> if the queue is empty.
     */
    public T poll() {
      T item = this.queue.poll();
      if (item != null) {
        this.members.remove(item);
      }
      return item;
    }

    /**
     * @param item to look for in queue
     * @return <code><b>true</b></code> if and only if <b>item</b> is in the queue.
     */
    public boolean contains(T item) {
      return this.members.contains(item);
    }

    /**
     * @return <code><b>true</b></code> if and only if the queue is empty.
     */
    public boolean isEmpty() {
      return this.members.isEmpty();
    }

    /**
     * Remove item from queue, if present.
     *
     * @param item to remove
     * @return <code><b>true</b></code> if and only if item was initially present and was removed.
     */
    public boolean remove(T item) {
      this.queue.remove(item); // there can only be one such item in the queue
      return this.members.remove(item);
    }

    /** Remove all items from the queue. */
    public void clear() {
      this.queue.clear();
      this.members.clear();
    }
  }

  /** Exception thrown when {@link WorkPool} enqueueing times out. */
  static class WorkPoolFullException extends RuntimeException {

    public WorkPoolFullException(String msg) {
      super(msg);
    }
  }
}
