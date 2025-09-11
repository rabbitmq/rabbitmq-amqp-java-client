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

import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.Requester;
import com.rabbitmq.client.amqp.RequesterBuilder;
import com.rabbitmq.client.amqp.Responder;
import com.rabbitmq.client.amqp.ResponderBuilder;
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

final class RequestResponseSupport {

  private RequestResponseSupport() {}

  static class AmqpRequesterBuilder implements RequesterBuilder {

    private static final Duration REQUEST_TIMEOUT_MIN = Duration.ofSeconds(1);

    private final AmqpConnection connection;

    private final DefaultRequesterAddressBuilder requestAddressBuilder =
        new DefaultRequesterAddressBuilder(this);
    private String replyToQueue;
    private Supplier<Object> correlationIdSupplier;
    private BiFunction<Message, Object, Message> requestPostProcessor;
    private Function<Message, Object> correlationIdExtractor;
    private Duration requestTimeout = Duration.ofSeconds(30);

    AmqpRequesterBuilder(AmqpConnection connection) {
      this.connection = connection;
    }

    @Override
    public RequesterAddressBuilder requestAddress() {
      return this.requestAddressBuilder;
    }

    @Override
    public RequesterBuilder replyToQueue(String replyToQueue) {
      this.replyToQueue = replyToQueue;
      return this;
    }

    @Override
    public RequesterBuilder correlationIdSupplier(Supplier<Object> correlationIdSupplier) {
      this.correlationIdSupplier = correlationIdSupplier;
      return this;
    }

    @Override
    public RequesterBuilder requestPostProcessor(
        BiFunction<Message, Object, Message> requestPostProcessor) {
      this.requestPostProcessor = requestPostProcessor;
      return this;
    }

    @Override
    public RequesterBuilder correlationIdExtractor(
        Function<Message, Object> correlationIdExtractor) {
      this.correlationIdExtractor = correlationIdExtractor;
      return this;
    }

    @Override
    public RequesterBuilder requestTimeout(Duration timeout) {
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
    public Requester build() {
      return this.connection.createRequester(this);
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

  private static class DefaultRequesterAddressBuilder
      extends DefaultAddressBuilder<RequesterBuilder.RequesterAddressBuilder>
      implements RequesterBuilder.RequesterAddressBuilder {

    private final AmqpRequesterBuilder builder;

    private DefaultRequesterAddressBuilder(AmqpRequesterBuilder builder) {
      super(null);
      this.builder = builder;
    }

    @Override
    RequesterBuilder.RequesterAddressBuilder result() {
      return this;
    }

    @Override
    public RequesterBuilder requester() {
      return this.builder;
    }
  }

  static class AmqpResponderBuilder implements ResponderBuilder {

    private final AmqpConnection connection;

    private String requestQueue;
    private Responder.Handler handler;
    private Function<Message, Object> correlationIdExtractor;
    private BiFunction<Message, Object, Message> replyPostProcessor;
    private Duration closeTimeout = Duration.ofSeconds(60);

    AmqpResponderBuilder(AmqpConnection connection) {
      this.connection = connection;
    }

    @Override
    public ResponderBuilder requestQueue(String requestQueue) {
      this.requestQueue = requestQueue;
      return this;
    }

    @Override
    public ResponderBuilder handler(Responder.Handler handler) {
      this.handler = handler;
      return this;
    }

    @Override
    public ResponderBuilder correlationIdExtractor(
        Function<Message, Object> correlationIdExtractor) {
      this.correlationIdExtractor = correlationIdExtractor;
      return this;
    }

    @Override
    public ResponderBuilder replyPostProcessor(
        BiFunction<Message, Object, Message> replyPostProcessor) {
      this.replyPostProcessor = replyPostProcessor;
      return this;
    }

    @Override
    public ResponderBuilder closeTimeout(Duration closeTimeout) {
      this.closeTimeout = closeTimeout;
      return this;
    }

    @Override
    public Responder build() {
      return this.connection.createResponder(this);
    }

    AmqpConnection connection() {
      return this.connection;
    }

    String requestQueue() {
      return this.requestQueue;
    }

    Responder.Handler handler() {
      return this.handler;
    }

    Function<Message, Object> correlationIdExtractor() {
      return this.correlationIdExtractor;
    }

    BiFunction<Message, Object, Message> replyPostProcessor() {
      return this.replyPostProcessor;
    }

    Duration closeTimeout() {
      return this.closeTimeout;
    }
  }
}
