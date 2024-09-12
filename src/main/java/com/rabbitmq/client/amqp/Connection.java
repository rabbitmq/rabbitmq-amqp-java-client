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

import java.io.Closeable;

/** A connection to the broker. */
public interface Connection extends Closeable, Resource {

  /**
   * The {@link Management} instance of this connection.
   *
   * @return management instance
   */
  Management management();

  /**
   * Create a builder to configure and create a {@link Publisher}.
   *
   * @return publisher builder
   */
  PublisherBuilder publisherBuilder();

  /**
   * Create a builder to configure and create a {@link Consumer}.
   *
   * @return consumer builder
   */
  ConsumerBuilder consumerBuilder();

  /**
   * Create a builder to configure and create a {@link RpcClientBuilder}.
   *
   * @return RPC client builder
   */
  RpcClientBuilder rpcClientBuilder();

  /**
   * Create a builder to configure and create a {@link RpcServerBuilder}.
   *
   * @return RPC server builder
   */
  RpcServerBuilder rpcServerBuilder();

  /** Close the connection and its resources */
  @Override
  void close();
}
