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

import static com.rabbitmq.model.amqp.TestUtils.environmentBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.model.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;

public class RpcTest {

  static Environment environment;
  Connection connection;

  @BeforeAll
  static void initAll() {
    environment = environmentBuilder().build();
  }

  @AfterAll
  static void tearDownAll() {
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
  void rpc() throws ExecutionException, InterruptedException, TimeoutException {
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

      serverConnection
          .rpcServerBuilder()
          .requestQueue(requestQueue)
          .handler(
              (ctx, msg) -> {
                String request = new String(msg.body(), UTF_8);
                return ctx.message(("*** " + request + " ***").getBytes(UTF_8));
              })
          .build();

      CompletableFuture<Message> responseFuture =
          rpcClient.publish(
              rpcClient.message("hello".getBytes(UTF_8)).messageId(UUID.randomUUID()));
      Message response = responseFuture.get(10, TimeUnit.SECONDS);
      assertThat(response.body()).asString(UTF_8).isEqualTo("*** hello ***");
    }
  }

  private static class OutstandingRequest {

    private final String request;
    private final CountDownLatch latch = new CountDownLatch(1);
    private AtomicReference<String> response = new AtomicReference<>();

    private OutstandingRequest(String request) {
      this.request = request;
    }
  }
}
