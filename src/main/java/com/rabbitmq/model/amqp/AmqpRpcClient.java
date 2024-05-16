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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

class AmqpRpcClient implements RpcClient {

  private static final Publisher.Callback NO_OP_CALLBACK = ctx -> {};

  private final Publisher publisher;
  private final Consumer consumer;
  private final Map<Object, CompletableFuture<Message>> outstandingRequests =
      new ConcurrentHashMap<>();
  private final Supplier<Object> correlationIdSupplier;
  private final BiFunction<Message, Object, Message> requestPostProcessor;
  private final Function<Message, Object> correlationIdExtractor;

  AmqpRpcClient(RpcSupport.AmqpRpcClientBuilder builder) {
    AmqpConnection connection = builder.connection();

    AmqpPublisherBuilder publisherBuilder = (AmqpPublisherBuilder) connection.publisherBuilder();
    ((DefaultAddressBuilder<?>) builder.requestAddress()).copyTo(publisherBuilder.addressBuilder());
    this.publisher = publisherBuilder.build();

    String replyTo = builder.replyToQueue();
    if (replyTo == null) {
      Management.QueueInfo queueInfo =
          connection.management().queue().exclusive(true).autoDelete(true).declare();
      replyTo = queueInfo.name();
    }
    if (builder.correlationIdExtractor() == null) {
      this.correlationIdExtractor = Message::correlationId;
    } else {
      this.correlationIdExtractor = builder.correlationIdExtractor();
    }
    this.consumer =
        connection
            .consumerBuilder()
            .queue(replyTo)
            .messageHandler(
                (ctx, msg) -> {
                  ctx.accept();
                  CompletableFuture<Message> result =
                      this.outstandingRequests.remove(this.correlationIdExtractor.apply(msg));
                  if (result != null) {
                    result.complete(msg);
                  }
                })
            .build();

    if (builder.correlationIdSupplier() == null) {
      String correlationIdPrefix = UUID.randomUUID().toString();
      AtomicLong correlationIdSequence = new AtomicLong();
      this.correlationIdSupplier =
          () -> correlationIdPrefix + "-" + correlationIdSequence.getAndIncrement();
    } else {
      this.correlationIdSupplier = builder.correlationIdSupplier();
    }

    if (builder.requestPostProcessor() == null) {
      DefaultAddressBuilder<?> addressBuilder = new DefaultAddressBuilder<>(null) {};
      addressBuilder.queue(replyTo);
      String replyToAddress = addressBuilder.address();
      this.requestPostProcessor =
          (request, correlationId) -> request.replyTo(replyToAddress).messageId(correlationId);
    } else {
      this.requestPostProcessor = builder.requestPostProcessor();
    }
  }

  @Override
  public Message message() {
    return this.publisher.message();
  }

  @Override
  public Message message(byte[] body) {
    return this.publisher.message(body);
  }

  @Override
  public CompletableFuture<Message> publish(Message message) {
    Object correlationId = this.correlationIdSupplier.get();
    message = this.requestPostProcessor.apply(message, correlationId);
    CompletableFuture<Message> result = new CompletableFuture<>();
    this.outstandingRequests.put(correlationId, result);
    this.publisher.publish(message, NO_OP_CALLBACK);
    return result;
  }

  @Override
  public void close() {
    this.publisher.close();
    this.consumer.close();
  }
}
