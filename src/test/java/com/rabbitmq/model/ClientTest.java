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
package com.rabbitmq.model;

import static java.nio.charset.StandardCharsets.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.model.amqp.AmqpEnvironmentBuilder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.qpid.protonj2.client.*;
import org.junit.jupiter.api.*;

public class ClientTest {

  static Environment environment;
  static Management management;
  String q;

  @BeforeAll
  static void initAll() {
    environment = new AmqpEnvironmentBuilder().build();
    management = environment.management();
  }

  @BeforeEach
  void init(TestInfo info) {
    q = TestUtils.name(info);
    management.queue().name(q).declare();
  }

  @AfterEach
  void tearDown() {
    management.queueDeletion().delete(q);
  }

  @AfterAll
  static void tearDownAll() {
    environment.close();
  }

  @Test
  void deliveryCount() throws Exception {
    ClientOptions clientOptions = new ClientOptions();
    try (Client client = Client.create(clientOptions);
        Publisher publisher = environment.publisherBuilder().address(q).build()) {
      int messageCount = 10;
      CountDownLatch publishLatch = new CountDownLatch(5);
      IntStream.range(0, messageCount)
          .forEach(
              ignored ->
                  publisher.publish(
                      publisher.message().addData("".getBytes(UTF_8)),
                      context -> publishLatch.countDown()));

      ConnectionOptions connectionOptions = new ConnectionOptions();
      connectionOptions.user("guest");
      connectionOptions.password("guest");
      connectionOptions.virtualHost("vhost:/");
      // only the mechanisms supported in RabbitMQ
      connectionOptions.saslOptions().addAllowedMechanism("PLAIN").addAllowedMechanism("EXTERNAL");

      Connection connection = client.connect("localhost", 5672, connectionOptions);
      Receiver receiver = connection.openReceiver(q, new ReceiverOptions());
      int receivedMessages = 0;
      while (receiver.receive(100, TimeUnit.MILLISECONDS) != null) {
        receivedMessages++;
      }

      assertThat(receivedMessages).isEqualTo(messageCount);
    }
  }
}
