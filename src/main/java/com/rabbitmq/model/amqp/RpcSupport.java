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

import com.rabbitmq.model.*;

abstract class RpcSupport {

  private RpcSupport() {}

  static class AmqpRpcClientBuilder implements RpcClientBuilder {

    private final AmqpConnection connection;

    private final DefaultRpcClientAddressBuilder requestAddressBuilder =
        new DefaultRpcClientAddressBuilder(this);
    private String replyToQueue;

    AmqpRpcClientBuilder(AmqpConnection connection) {
      this.connection = connection;
    }

    @Override
    public RpcClientAddressBuilder requestAddress() {
      return this.requestAddressBuilder;
    }

    @Override
    public RpcClientBuilder replyToQueue(String replyToQueue) {
      this.replyToQueue = replyToQueue;
      return this;
    }

    @Override
    public RpcClient build() {
      return new AmqpRpcClient(this);
    }

    AmqpConnection connection() {
      return this.connection;
    }

    String replyToQueue() {
      return this.replyToQueue;
    }
  }

  private static class DefaultRpcClientAddressBuilder
      extends DefaultAddressBuilder<RpcClientBuilder.RpcClientAddressBuilder>
      implements RpcClientBuilder.RpcClientAddressBuilder {

    private final AmqpRpcClientBuilder builder;

    private DefaultRpcClientAddressBuilder(AmqpRpcClientBuilder builder) {
      super(null);
      this.builder = builder;
    }

    @Override
    RpcClientBuilder.RpcClientAddressBuilder result() {
      return this;
    }

    @Override
    public RpcClientBuilder rpcClient() {
      return this.builder;
    }
  }

  static class AmqpRpcServerBuilder implements RpcServerBuilder {

    private final AmqpConnection connection;

    private String requestQueue;
    private RpcServer.Handler handler;
    private final DefaultRpcServerAddressBuilder replyToAddressBuilder =
        new DefaultRpcServerAddressBuilder(this);

    AmqpRpcServerBuilder(AmqpConnection connection) {
      this.connection = connection;
    }

    @Override
    public RpcServerBuilder requestQueue(String requestQueue) {
      this.requestQueue = requestQueue;
      return this;
    }

    @Override
    public RpcServerBuilder handler(RpcServer.Handler handler) {
      this.handler = handler;
      return this;
    }

    @Override
    public RpcServerAddressBuilder replyToAddress() {
      return this.replyToAddressBuilder;
    }

    @Override
    public RpcServer build() {
      return new AmqpRpcServer(this);
    }

    AmqpConnection connection() {
      return this.connection;
    }

    String requestQueue() {
      return this.requestQueue;
    }

    RpcServer.Handler handler() {
      return this.handler;
    }
  }

  private static class DefaultRpcServerAddressBuilder
      extends DefaultAddressBuilder<RpcServerBuilder.RpcServerAddressBuilder>
      implements RpcServerBuilder.RpcServerAddressBuilder {

    private final AmqpRpcServerBuilder builder;

    private DefaultRpcServerAddressBuilder(AmqpRpcServerBuilder builder) {
      super(null);
      this.builder = builder;
    }

    @Override
    public RpcServerBuilder rpcServer() {
      return this.builder;
    }
  }
}
