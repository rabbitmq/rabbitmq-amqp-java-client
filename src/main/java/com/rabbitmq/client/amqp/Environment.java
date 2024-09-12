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
 * The {@link Environment} is the main entry point to a node or a cluster of nodes.
 *
 * <p>The {@link #connectionBuilder()} allows creating {@link Connection} instances. An application
 * is expected to maintain a single {@link Environment} instance and to close with it exits.
 *
 * <p>{@link Environment} instances are expected to be thread-safe.
 *
 * @see com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder
 */
public interface Environment extends AutoCloseable {

  /**
   * Create a builder to configure and create a {@link Connection}.
   *
   * @return
   */
  ConnectionBuilder connectionBuilder();

  /** Close the environment and its resources. */
  @Override
  void close();
}
