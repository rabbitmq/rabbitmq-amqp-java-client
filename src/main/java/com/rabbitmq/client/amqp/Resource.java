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
package com.rabbitmq.client.amqp;

/**
 * Marker interface for {@link Resource}-like classes.
 *
 * <p>Instances of these classes have different states during their lifecycle: open, recovering,
 * closed, etc. Application can be interested in taking some actions for a given state (e.g.
 * stopping publishing when a {@link Publisher} is recovering after a connection problem and
 * resuming publishing when it is open again).
 *
 * @see Connection
 * @see Publisher
 * @see Consumer
 */
public interface Resource {

  /**
   * Application listener for a {@link Resource}.
   *
   * <p>They are usually registered at creation time.
   *
   * @see ConnectionBuilder#listeners(StateListener...)
   * @see PublisherBuilder#listeners(StateListener...)
   * @see ConsumerBuilder#listeners(StateListener...)
   */
  @FunctionalInterface
  interface StateListener {

    /**
     * Handle state change.
     *
     * @param context state change context
     */
    void handle(Context context);
  }

  /** Context of a resource state change. */
  interface Context {

    /**
     * The resource instance.
     *
     * @return resource instance
     */
    Resource resource();

    /**
     * The failure cause, can be null.
     *
     * @return failure cause, null if no cause for failure
     */
    Throwable failureCause();

    /**
     * The previous state of the resource.
     *
     * @return previous state
     */
    State previousState();

    /**
     * The current (new) state of the resource.
     *
     * @return current state
     */
    State currentState();
  }

  /** Resource state. */
  enum State {
    /** The resource is currently opening. */
    OPENING,
    /** The resource is open and functional. */
    OPEN,
    /** The resource is recovering. */
    RECOVERING,
    /** The resource is closing. */
    CLOSING,
    /** The resource is closed. */
    CLOSED
  }
}
