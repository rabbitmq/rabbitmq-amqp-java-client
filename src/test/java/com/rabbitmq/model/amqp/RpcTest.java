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

import static com.rabbitmq.model.BackOffDelayPolicy.fixed;
import static com.rabbitmq.model.amqp.Cli.closeConnection;
import static com.rabbitmq.model.amqp.TestUtils.environmentBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.rabbitmq.model.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.*;

public class RpcTest {

  private static final RpcServer.Handler HANDLER =
      (ctx, request) -> {
        String in = new String(request.body(), UTF_8);
        return ctx.message(process(in).getBytes(UTF_8));
      };

  static Environment environment;
  static ExecutorService executorService;
  Connection connection;

  @BeforeAll
  static void initAll() {
    environment = environmentBuilder().build();
    executorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
  }

  @AfterAll
  static void tearDownAll() {
    executorService.shutdownNow();
    environment.close();
  }

  @BeforeEach
  void init() {
    this.connection = environment.connectionBuilder().build();
  }

  @AfterEach
  void tearDown() {
    this.connection.close();
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
      CountDownLatch latch = new CountDownLatch(requestCount);
      IntStream.range(0, requestCount)
          .forEach(
              ignored ->
                  executorService.submit(
                      () -> {
                        String request = UUID.randomUUID().toString();
                        CompletableFuture<Message> responseFuture =
                            rpcClient.publish(
                                rpcClient
                                    .message(request.getBytes(UTF_8))
                                    .messageId(UUID.randomUUID()));
                        Message response = responseFuture.get(10, TimeUnit.SECONDS);
                        assertThat(response.body()).asString(UTF_8).isEqualTo(process(request));
                        latch.countDown();
                        return null;
                      }));
      TestUtils.assertThat(latch).completes();
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
                    .address()
                    .queue(request.property("reply-to-queue").toString())
                    .message();
              })
          .build();

      int requestCount = 100;
      CountDownLatch latch = new CountDownLatch(requestCount);
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
                        latch.countDown();
                        return null;
                      }));
      TestUtils.assertThat(latch).completes();
    }
  }

  @Test
  void rpcShouldRecoverAfterConnectionIsClosed()
      throws ExecutionException, InterruptedException, TimeoutException {
    String clientConnectionName = UUID.randomUUID().toString();
    CountDownLatch clientConnectionLatch = new CountDownLatch(1);
    String serverConnectionName = UUID.randomUUID().toString();
    CountDownLatch serverConnectionLatch = new CountDownLatch(1);

    BackOffDelayPolicy backOffDelayPolicy = fixed(ofMillis(100));
    Connection serverConnection =
        connectionBuilder()
            .name(serverConnectionName)
            .listeners(recoveredListener(serverConnectionLatch))
            .recovery()
            .backOffDelayPolicy(backOffDelayPolicy)
            .connectionBuilder()
            .build();
    String requestQueue = serverConnection.management().queue().declare().name();
    try (Connection clientConnection =
        connectionBuilder()
            .name(clientConnectionName)
            .listeners(recoveredListener(clientConnectionLatch))
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

      closeConnection(clientConnectionName);
      requestBody = request(UUID.randomUUID().toString());
      try {
        rpcClient.publish(rpcClient.message(requestBody).messageId(UUID.randomUUID()));
        fail("Client connection is recovering, the call should have failed");
      } catch (ModelException e) {
        // OK
      }
      TestUtils.assertThat(clientConnectionLatch).completes();
      requestBody = request(UUID.randomUUID().toString());
      response = rpcClient.publish(rpcClient.message(requestBody).messageId(UUID.randomUUID()));
      assertThat(response.get(10, TimeUnit.SECONDS).body()).isEqualTo(process(requestBody));

      closeConnection(serverConnectionName);
      requestBody = request(UUID.randomUUID().toString());
      response = rpcClient.publish(rpcClient.message(requestBody).messageId(UUID.randomUUID()));
      assertThat(response.get(10, TimeUnit.SECONDS).body()).isEqualTo(process(requestBody));
      TestUtils.assertThat(serverConnectionLatch).completes();
      requestBody = request(UUID.randomUUID().toString());
      response = rpcClient.publish(rpcClient.message(requestBody).messageId(UUID.randomUUID()));
      assertThat(response.get(10, TimeUnit.SECONDS).body()).isEqualTo(process(requestBody));
    } finally {
      serverConnection.management().queueDeletion().delete(requestQueue);
      serverConnection.close();
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

  private static Resource.StateListener recoveredListener(CountDownLatch latch) {
    return context -> {
      if (context.previousState() == Resource.State.RECOVERING
          && context.currentState() == Resource.State.OPEN) {
        latch.countDown();
      }
    };
  }
}
