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

import static com.rabbitmq.client.amqp.Management.ExchangeType.FANOUT;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@AmqpTestInfrastructure
public class RpcTest {

  private static final RpcServer.Handler HANDLER =
      (ctx, request) -> {
        String in = new String(request.body(), UTF_8);
        return ctx.message(process(in).getBytes(UTF_8));
      };

  static Environment environment;
  static ExecutorService executorService;

  @BeforeAll
  static void initAll() {
    executorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
  }

  @AfterAll
  static void tearDownAll() {
    executorService.shutdownNow();
  }

  @Test
  void rpcWithDefaults() {
    try (Connection clientConnection = environment.connectionBuilder().build();
        Connection serverConnection = environment.connectionBuilder().build()) {

      String requestQueue = serverConnection.management().queue().exclusive(true).declare().name();

      RpcClient rpcClient =
          clientConnection
              .rpcClientBuilder()
              .requestAddress()
              .queue(requestQueue)
              .rpcClient()
              .build();

      serverConnection.rpcServerBuilder().requestQueue(requestQueue).handler(HANDLER).build();

      int requestCount = 100;
      Sync sync = sync(requestCount);
      IntStream.range(0, requestCount)
          .forEach(
              ignored ->
                  executorService.submit(
                      () -> {
                        String request = UUID.randomUUID().toString();
                        CompletableFuture<Message> responseFuture =
                            rpcClient.publish(rpcClient.message(request.getBytes(UTF_8)));
                        Message response = responseFuture.get(10, TimeUnit.SECONDS);
                        assertThat(response.body()).asString(UTF_8).isEqualTo(process(request));
                        sync.down();
                        return null;
                      }));
      assertThat(sync).completes();
    }
  }

  @Test
  void rpcWithCustomSettings() {
    try (Connection clientConnection = environment.connectionBuilder().build();
        Connection serverConnection = environment.connectionBuilder().build()) {

      String requestQueue = serverConnection.management().queue().exclusive(true).declare().name();

      String replyToQueue = clientConnection.management().queue().exclusive(true).declare().name();
      AtomicLong correlationIdSequence = new AtomicLong(0);

      // we are using application properties for the correlation ID and the reply-to queue
      // (instead of the standard properties)
      RpcClient rpcClient =
          clientConnection
              .rpcClientBuilder()
              .correlationIdSupplier(correlationIdSequence::getAndIncrement)
              .requestPostProcessor(
                  (msg, corrId) ->
                      msg.property("reply-to-queue", replyToQueue)
                          .property("message-id", (Long) corrId))
              .correlationIdExtractor(msg -> msg.property("correlation-id"))
              .replyToQueue(replyToQueue)
              .requestAddress()
              .queue(requestQueue)
              .rpcClient()
              .build();

      serverConnection
          .rpcServerBuilder()
          .correlationIdExtractor(msg -> msg.property("message-id"))
          .replyPostProcessor((msg, corrId) -> msg.property("correlation-id", (Long) corrId))
          .requestQueue(requestQueue)
          .handler(
              (ctx, request) -> {
                Message reply = HANDLER.handle(ctx, request);
                return reply
                    .toAddress()
                    .queue(request.property("reply-to-queue").toString())
                    .message();
              })
          .build();

      int requestCount = 100;
      Sync sync = sync(requestCount);
      IntStream.range(0, requestCount)
          .forEach(
              ignored ->
                  executorService.submit(
                      () -> {
                        String request = UUID.randomUUID().toString();
                        CompletableFuture<Message> responseFuture =
                            rpcClient.publish(rpcClient.message(request.getBytes(UTF_8)));
                        Message response = responseFuture.get(10, TimeUnit.SECONDS);
                        org.assertj.core.api.Assertions.assertThat(response.body())
                            .asString(UTF_8)
                            .isEqualTo(process(request));
                        sync.down();
                        return null;
                      }));
      assertThat(sync).completes();
    }
  }

  @Test
  void rpcUseCorrelationIdRequestProperty() {
    try (Connection clientConnection = environment.connectionBuilder().build();
        Connection serverConnection = environment.connectionBuilder().build()) {

      String requestQueue = serverConnection.management().queue().exclusive(true).declare().name();

      String replyToQueue =
          clientConnection.management().queue().autoDelete(true).exclusive(true).declare().name();
      RpcClient rpcClient =
          clientConnection
              .rpcClientBuilder()
              .correlationIdSupplier(UUID::randomUUID)
              .requestPostProcessor(
                  (msg, corrId) ->
                      msg.correlationId(corrId).replyToAddress().queue(replyToQueue).message())
              .replyToQueue(replyToQueue)
              .requestAddress()
              .queue(requestQueue)
              .rpcClient()
              .build();

      serverConnection
          .rpcServerBuilder()
          .correlationIdExtractor(Message::correlationId)
          .requestQueue(requestQueue)
          .handler(HANDLER)
          .build();

      int requestCount = 100;
      Sync sync = sync(requestCount);
      IntStream.range(0, requestCount)
          .forEach(
              ignored ->
                  executorService.submit(
                      () -> {
                        String request = UUID.randomUUID().toString();
                        CompletableFuture<Message> responseFuture =
                            rpcClient.publish(rpcClient.message(request.getBytes(UTF_8)));
                        Message response = responseFuture.get(10, TimeUnit.SECONDS);
                        assertThat(response.body()).asString(UTF_8).isEqualTo(process(request));
                        sync.down();
                        return null;
                      }));
      assertThat(sync).completes();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rpcShouldRecoverAfterConnectionIsClosed(boolean isolateResources)
      throws ExecutionException, InterruptedException, TimeoutException {
    String clientConnectionName = UUID.randomUUID().toString();
    Sync clientConnectionSync = sync();
    String serverConnectionName = UUID.randomUUID().toString();
    Sync serverConnectionSync = sync();

    BackOffDelayPolicy backOffDelayPolicy = BackOffDelayPolicy.fixed(ofMillis(100));
    Connection serverConnection =
        connectionBuilder()
            .name(serverConnectionName)
            .isolateResources(isolateResources)
            .listeners(recoveredListener(serverConnectionSync))
            .recovery()
            .backOffDelayPolicy(backOffDelayPolicy)
            .connectionBuilder()
            .build();
    String requestQueue = serverConnection.management().queue().declare().name();
    try (Connection clientConnection =
        connectionBuilder()
            .name(clientConnectionName)
            .isolateResources(isolateResources)
            .listeners(recoveredListener(clientConnectionSync))
            .recovery()
            .backOffDelayPolicy(backOffDelayPolicy)
            .connectionBuilder()
            .build()) {
      RpcClient rpcClient =
          clientConnection
              .rpcClientBuilder()
              .requestAddress()
              .queue(requestQueue)
              .rpcClient()
              .build();

      serverConnection.rpcServerBuilder().requestQueue(requestQueue).handler(HANDLER).build();

      byte[] requestBody = request(UUID.randomUUID().toString());
      CompletableFuture<Message> response =
          rpcClient.publish(rpcClient.message(requestBody).messageId(UUID.randomUUID()));
      assertThat(response.get(10, TimeUnit.SECONDS).body()).isEqualTo(process(requestBody));

      Cli.closeConnection(clientConnectionName);
      requestBody = request(UUID.randomUUID().toString());
      try {
        rpcClient.publish(rpcClient.message(requestBody).messageId(UUID.randomUUID()));
        fail("Client connection is recovering, the call should have failed");
      } catch (AmqpException e) {
        // OK
      }
      assertThat(clientConnectionSync).completes();
      requestBody = request(UUID.randomUUID().toString());
      response = rpcClient.publish(rpcClient.message(requestBody).messageId(UUID.randomUUID()));
      assertThat(response.get(10, TimeUnit.SECONDS).body()).isEqualTo(process(requestBody));

      Cli.closeConnection(serverConnectionName);
      requestBody = request(UUID.randomUUID().toString());
      response = rpcClient.publish(rpcClient.message(requestBody).messageId(UUID.randomUUID()));
      assertThat(response.get(10, TimeUnit.SECONDS).body()).isEqualTo(process(requestBody));
      assertThat(serverConnectionSync).completes();
      requestBody = request(UUID.randomUUID().toString());
      response = rpcClient.publish(rpcClient.message(requestBody).messageId(UUID.randomUUID()));
      assertThat(response.get(10, TimeUnit.SECONDS).body()).isEqualTo(process(requestBody));
    } finally {
      serverConnection.management().queueDelete(requestQueue);
      serverConnection.close();
    }
  }

  @Test
  void poisonRequestsShouldTimeout() {
    try (Connection clientConnection = environment.connectionBuilder().build();
        Connection serverConnection = environment.connectionBuilder().build()) {

      String requestQueue = serverConnection.management().queue().exclusive(true).declare().name();

      serverConnection
          .rpcServerBuilder()
          .requestQueue(requestQueue)
          .handler(
              (ctx, msg) -> {
                String request = new String(msg.body(), UTF_8);
                if (request.contains("poison")) {
                  return null;
                } else {
                  return HANDLER.handle(ctx, msg);
                }
              })
          .replyPostProcessor((r, corrId) -> r == null ? null : r.correlationId(corrId))
          .build();

      Duration requestTimeout = Duration.ofSeconds(1);
      RpcClient rpcClient =
          clientConnection
              .rpcClientBuilder()
              .requestTimeout(requestTimeout)
              .requestAddress()
              .queue(requestQueue)
              .rpcClient()
              .build();

      int requestCount = 100;
      AtomicInteger expectedPoisonCount = new AtomicInteger();
      AtomicInteger timedOutRequestCount = new AtomicInteger();
      Sync sync = sync(requestCount);
      Random random = new Random();
      IntStream.range(0, requestCount)
          .forEach(
              ignored -> {
                boolean poison = random.nextBoolean();
                String request;
                if (poison) {
                  request = "poison";
                  expectedPoisonCount.incrementAndGet();
                } else {
                  request = UUID.randomUUID().toString();
                }
                executorService.submit(
                    () -> {
                      CompletableFuture<Message> responseFuture =
                          rpcClient.publish(rpcClient.message(request.getBytes(UTF_8)));
                      responseFuture.handle(
                          (msg, ex) -> {
                            if (ex != null) {
                              timedOutRequestCount.incrementAndGet();
                            }
                            sync.down();
                            return null;
                          });
                    });
              });
      assertThat(sync).completes();
      assertThat(timedOutRequestCount).hasPositiveValue().hasValue(expectedPoisonCount.get());
    }
  }

  @Test
  void outstandingRequestsShouldCompleteExceptionallyOnRpcClientClosing() {
    try (Connection clientConnection = environment.connectionBuilder().build();
        Connection serverConnection = environment.connectionBuilder().build()) {

      String requestQueue = serverConnection.management().queue().exclusive(true).declare().name();

      serverConnection
          .rpcServerBuilder()
          .requestQueue(requestQueue)
          .handler(
              (ctx, msg) -> {
                String request = new String(msg.body(), UTF_8);
                if (request.contains("poison")) {
                  return null;
                } else {
                  return HANDLER.handle(ctx, msg);
                }
              })
          .replyPostProcessor((r, corrId) -> r == null ? null : r.correlationId(corrId))
          .build();

      RpcClient rpcClient =
          clientConnection
              .rpcClientBuilder()
              .requestAddress()
              .queue(requestQueue)
              .rpcClient()
              .build();

      int requestCount = 100;
      AtomicInteger expectedPoisonCount = new AtomicInteger();
      AtomicInteger timedOutRequestCount = new AtomicInteger();
      AtomicInteger completedRequestCount = new AtomicInteger();
      Random random = new Random();
      Sync allRequestSubmitted = sync(requestCount);
      IntStream.range(0, requestCount)
          .forEach(
              ignored -> {
                boolean poison = random.nextBoolean();
                String request;
                if (poison) {
                  request = "poison";
                  expectedPoisonCount.incrementAndGet();
                } else {
                  request = UUID.randomUUID().toString();
                }
                executorService.submit(
                    () -> {
                      CompletableFuture<Message> responseFuture =
                          rpcClient.publish(rpcClient.message(request.getBytes(UTF_8)));
                      responseFuture.handle(
                          (msg, ex) -> {
                            if (ex == null) {
                              completedRequestCount.incrementAndGet();
                            } else {
                              timedOutRequestCount.incrementAndGet();
                            }
                            return null;
                          });
                    });
                allRequestSubmitted.down();
              });
      assertThat(allRequestSubmitted).completes();
      waitAtMost(() -> completedRequestCount.get() == requestCount - expectedPoisonCount.get());
      assertThat(timedOutRequestCount).hasValue(0);
      rpcClient.close();
      assertThat(timedOutRequestCount).hasPositiveValue().hasValue(expectedPoisonCount.get());
    }
  }

  @Test
  void errorDuringProcessingShouldDiscardMessageAndDeadLetterIfSet(TestInfo info)
      throws ExecutionException, InterruptedException, TimeoutException {
    try (Connection clientConnection = environment.connectionBuilder().build();
        Connection serverConnection = environment.connectionBuilder().build()) {

      String dlx = name(info);
      String dlq = name(info);
      Management management = serverConnection.management();
      management.exchange(dlx).type(FANOUT).autoDelete(true).declare();
      management.queue(dlq).exclusive(true).declare();
      management.binding().sourceExchange(dlx).destinationQueue(dlq).bind();

      String requestQueue =
          management.queue().exclusive(true).deadLetterExchange(dlx).declare().name();

      Duration requestTimeout = Duration.ofSeconds(1);
      RpcClient rpcClient =
          clientConnection
              .rpcClientBuilder()
              .requestTimeout(requestTimeout)
              .requestAddress()
              .queue(requestQueue)
              .rpcClient()
              .build();

      serverConnection
          .rpcServerBuilder()
          .requestQueue(requestQueue)
          .handler(
              (ctx, request) -> {
                String body = new String(request.body(), UTF_8);
                if (body.contains("poison")) {
                  throw new RuntimeException("Poison message");
                }
                return HANDLER.handle(ctx, request);
              })
          .build();

      String request = UUID.randomUUID().toString();
      CompletableFuture<Message> responseFuture =
          rpcClient.publish(rpcClient.message(request.getBytes(UTF_8)));
      Message response = responseFuture.get(10, TimeUnit.SECONDS);
      assertThat(response.body()).asString(UTF_8).isEqualTo(process(request));

      assertThat(management.queueInfo(dlq)).isEmpty();

      request = "poison";
      CompletableFuture<Message> poisonFuture =
          rpcClient.publish(rpcClient.message(request.getBytes(UTF_8)));
      waitAtMost(() -> management.queueInfo(dlq).messageCount() == 1);
      assertThatThrownBy(
              () -> poisonFuture.get(requestTimeout.multipliedBy(3).toMillis(), MILLISECONDS))
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(AmqpException.class);
    }
  }

  private static AmqpConnectionBuilder connectionBuilder() {
    return (AmqpConnectionBuilder) environment.connectionBuilder();
  }

  private static String process(String in) {
    return "*** " + in + " ***";
  }

  private static byte[] request(String request) {
    return request.getBytes(UTF_8);
  }

  private static byte[] process(byte[] in) {
    return process(new String(in, UTF_8)).getBytes(UTF_8);
  }

  private static Resource.StateListener recoveredListener(Sync sync) {
    return context -> {
      if (context.previousState() == Resource.State.RECOVERING
          && context.currentState() == Resource.State.OPEN) {
        sync.down();
      }
    };
  }
}
