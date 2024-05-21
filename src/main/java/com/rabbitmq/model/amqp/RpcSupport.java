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
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

abstract class RpcSupport {

  private RpcSupport() {}

  static class AmqpRpcClientBuilder implements RpcClientBuilder {

    private static final Duration REQUEST_TIMEOUT_MIN = Duration.ofSeconds(1);

    private final AmqpConnection connection;

    private final DefaultRpcClientAddressBuilder requestAddressBuilder =
        new DefaultRpcClientAddressBuilder(this);
    private String replyToQueue;
    private Supplier<Object> correlationIdSupplier;
    private BiFunction<Message, Object, Message> requestPostProcessor;
    private Function<Message, Object> correlationIdExtractor;
    private Duration requestTimeout = Duration.ofSeconds(30);

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
    public RpcClientBuilder correlationIdSupplier(Supplier<Object> correlationIdSupplier) {
      this.correlationIdSupplier = correlationIdSupplier;
      return this;
    }

    @Override
    public RpcClientBuilder requestPostProcessor(
        BiFunction<Message, Object, Message> requestPostProcessor) {
      this.requestPostProcessor = requestPostProcessor;
      return this;
    }

    @Override
    public RpcClientBuilder correlationIdExtractor(
        Function<Message, Object> correlationIdExtractor) {
      this.correlationIdExtractor = correlationIdExtractor;
      return this;
    }

    @Override
    public RpcClientBuilder requestTimeout(Duration timeout) {
      if (timeout == null) {
        throw new IllegalArgumentException("Request timeout cannot be null");
      }
      if (timeout.compareTo(REQUEST_TIMEOUT_MIN) < 0) {
        throw new IllegalArgumentException(
            "Request timeout cannot be less than " + REQUEST_TIMEOUT_MIN);
      }
      this.requestTimeout = timeout;
      return this;
    }

    Function<Message, Object> correlationIdExtractor() {
      return correlationIdExtractor;
    }

    @Override
    public RpcClient build() {
      return this.connection.createRpcClient(this);
    }

    AmqpConnection connection() {
      return this.connection;
    }

    String replyToQueue() {
      return this.replyToQueue;
    }

    Supplier<Object> correlationIdSupplier() {
      return this.correlationIdSupplier;
    }

    BiFunction<Message, Object, Message> requestPostProcessor() {
      return this.requestPostProcessor;
    }

    Duration requestTimeout() {
      return this.requestTimeout;
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
    private Function<Message, Object> correlationIdExtractor;
    private BiFunction<Message, Object, Message> replyPostProcessor;

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
    public RpcServerBuilder correlationIdExtractor(
        Function<Message, Object> correlationIdExtractor) {
      this.correlationIdExtractor = correlationIdExtractor;
      return this;
    }

    @Override
    public RpcServerBuilder replyPostProcessor(
        BiFunction<Message, Object, Message> replyPostProcessor) {
      this.replyPostProcessor = replyPostProcessor;
      return this;
    }

    @Override
    public RpcServer build() {
      return this.connection.createRpcServer(this);
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

    Function<Message, Object> correlationIdExtractor() {
      return this.correlationIdExtractor;
    }

    BiFunction<Message, Object, Message> replyPostProcessor() {
      return this.replyPostProcessor;
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
