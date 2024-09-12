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

/** Builder for {@link Connection} instances. */
public interface ConnectionBuilder extends ConnectionSettings<ConnectionBuilder> {

  /**
   * Configuration for recovery.
   *
   * @return recovery configuration
   */
  RecoveryConfiguration recovery();

  /**
   * Add {@link com.rabbitmq.client.amqp.Resource.StateListener}s to the connection.
   *
   * @param listeners
   * @return this builder instance
   */
  ConnectionBuilder listeners(Resource.StateListener... listeners);

  /**
   * Create the connection instance.
   *
   * @return the configured connection
   */
  Connection build();

  /** Configuration for recovery. */
  interface RecoveryConfiguration {

    /**
     * Whether to activate recovery or not.
     *
     * <p>Activated by default.
     *
     * @param activated activation flag
     * @return the configuration instance
     */
    RecoveryConfiguration activated(boolean activated);

    /**
     * Delay policy for connection attempts.
     *
     * @param backOffDelayPolicy back-off delay policy
     * @return the configuration instance
     */
    RecoveryConfiguration backOffDelayPolicy(BackOffDelayPolicy backOffDelayPolicy);

    /**
     * Whether to activate topology recovery or not.
     *
     * <p>Topology recovery includes recovery of exchanges, queues, bindings, publishers, and
     * consumers.
     *
     * <p>Activated by default.
     *
     * @param activated activation flag
     * @return the configuration instance
     */
    RecoveryConfiguration topology(boolean activated);

    /**
     * The connection builder.
     *
     * @return connection builder
     */
    ConnectionBuilder connectionBuilder();
  }
}
