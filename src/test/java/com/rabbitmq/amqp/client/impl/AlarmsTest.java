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
package com.rabbitmq.amqp.client.impl;

import static com.rabbitmq.amqp.client.impl.TestUtils.*;
import static java.util.stream.IntStream.range;

import com.rabbitmq.amqp.client.Connection;
import com.rabbitmq.amqp.client.Environment;
import com.rabbitmq.amqp.client.ModelException;
import com.rabbitmq.amqp.client.Publisher;
import java.time.Duration;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AlarmsTest {

  static Environment environment;
  Connection connection;

  @BeforeAll
  static void initAll() {
    environment = environmentBuilder().build();
  }

  @BeforeEach
  void init() {
    this.connection = environment.connectionBuilder().build();
  }

  @AfterEach
  void tearDown() {
    this.connection.close();
  }

  @AfterAll
  static void tearDownAll() {
    environment.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {"disk", "memory"})
  void alarmShouldBlockPublisher(String alarmType) throws Exception {
    String q = connection.management().queue().exclusive(true).declare().name();
    Publisher publisher =
        connection.publisherBuilder().queue(q).publishTimeout(Duration.ofMillis(100)).build();
    int messageCount = 100;
    Sync publishSync = sync(messageCount);
    range(0, messageCount)
        .forEach(ignored -> publisher.publish(publisher.message(), ctx -> publishSync.down()));
    assertThat(publishSync).completes();
    Sync consumeSync = sync(messageCount);
    try (AutoCloseable ignored = alarm(alarmType)) {
      Sync publishTimeoutSync = sync();
      new Thread(
              () -> {
                try {
                  publisher.publish(publisher.message(), ctx -> {});
                } catch (ModelException e) {
                  if (e.getCause() instanceof ClientSendTimedOutException)
                    publishTimeoutSync.down();
                }
              })
          .start();
      assertThat(publishTimeoutSync).completes();
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, msg) -> {
                ctx.accept();
                consumeSync.down();
              })
          .build();
      assertThat(consumeSync).completes();
      publishSync.reset(messageCount);
      consumeSync.reset(messageCount);
    }

    range(0, messageCount)
        .forEach(ignored -> publisher.publish(publisher.message(), ctx -> publishSync.down()));
    assertThat(publishSync).completes();
    assertThat(consumeSync).completes();
  }

  private static AutoCloseable alarm(String type) throws Exception {
    if ("disk".equals(type)) {
      return Cli.diskAlarm();
    } else if ("memory".equals(type)) {
      return Cli.memoryAlarm();
    } else {
      throw new IllegalArgumentException();
    }
  }
}