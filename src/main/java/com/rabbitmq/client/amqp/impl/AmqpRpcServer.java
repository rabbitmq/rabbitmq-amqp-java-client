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

import com.rabbitmq.client.amqp.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpRpcServer implements RpcServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpRpcServer.class);

  private static final Publisher.Callback NO_OP_CALLBACK = ctx -> {};

  private static final Predicate<Exception> RESPONSE_SENDING_EXCEPTION_PREDICATE =
      ex ->
          ex instanceof AmqpException.AmqpResourceInvalidStateException
              && !(ex instanceof AmqpException.AmqpResourceClosedException);

  private static final List<Duration> RESPONSE_SENDING_RETRY_WAIT_TIMES =
      List.of(
          Duration.ofSeconds(1),
          Duration.ofSeconds(3),
          Duration.ofSeconds(5),
          Duration.ofSeconds(10));

  private final AmqpConnection connection;
  private final Publisher publisher;
  private final Consumer consumer;
  private final Function<Message, Object> correlationIdExtractor;
  private final BiFunction<Message, Object, Message> replyPostProcessor;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  AmqpRpcServer(RpcSupport.AmqpRpcServerBuilder builder) {
    this.connection = builder.connection();
    Handler handler = builder.handler();

    this.publisher = this.connection.publisherBuilder().build();

    Context context =
        new Context() {
          @Override
          public Message message() {
            return publisher.message();
          }

          @Override
          public Message message(byte[] body) {
            return publisher.message(body);
          }
        };
    if (builder.correlationIdExtractor() == null) {
      // HTTP over AMQP 1.0 extension specification, 5.1:
      // To associate a response with a request, the correlation-id value of the response properties
      // MUST be set to the message-id value of the request properties.
      this.correlationIdExtractor = Message::messageId;
    } else {
      this.correlationIdExtractor = builder.correlationIdExtractor();
    }
    if (builder.replyPostProcessor() == null) {
      this.replyPostProcessor = Message::correlationId;
    } else {
      this.replyPostProcessor = builder.replyPostProcessor();
    }
    this.consumer =
        this.connection
            .consumerBuilder()
            .queue(builder.requestQueue())
            .messageHandler(
                (ctx, msg) -> {
                  ctx.accept();
                  Message reply = handler.handle(context, msg);
                  if (reply != null && msg.replyTo() != null) {
                    reply.to(msg.replyTo());
                  }
                  Object correlationId = correlationIdExtractor.apply(msg);
                  reply = replyPostProcessor.apply(reply, correlationId);
                  if (reply != null && reply.to() != null) {
                    sendReply(reply);
                  }
                })
            .build();
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.connection.removeRpcServer(this);
      try {
        this.consumer.close();
      } catch (Exception e) {
        LOGGER.warn("Error while closing RPC server consumer: {}", e.getMessage());
      }
      try {
        this.publisher.close();
      } catch (Exception e) {
        LOGGER.warn("Error while closing RPC server publisher: {}", e.getMessage());
      }
    }
  }

  private void sendReply(Message reply) {
    try {
      RetryUtils.callAndMaybeRetry(
          () -> {
            this.publisher.publish(reply, NO_OP_CALLBACK);
            return null;
          },
          RESPONSE_SENDING_EXCEPTION_PREDICATE,
          RESPONSE_SENDING_RETRY_WAIT_TIMES,
          "RPC Server Response");
    } catch (Exception e) {
      LOGGER.info("Error while processing RPC request: {}", e.getMessage());
    }
  }
}
