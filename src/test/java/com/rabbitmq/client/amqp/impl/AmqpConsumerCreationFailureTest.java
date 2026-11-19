// Copyright (c) 2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.client.amqp.Resource.State.CLOSED;
import static com.rabbitmq.client.amqp.Resource.State.CLOSING;
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.Resource.State.OPENING;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfAddressV1Permitted;
import com.rabbitmq.client.amqp.metrics.MicrometerMetricsCollector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@DisabledIfAddressV1Permitted
public class AmqpConsumerCreationFailureTest {

  SimpleMeterRegistry registry;
  Environment environment;
  Connection connection;
  List<Resource.State> states;
  List<AmqpConsumer> consumers;

  @BeforeEach
  void init() {
    registry = new SimpleMeterRegistry();
    environment =
        TestUtils.environmentBuilder()
            .metricsCollector(new MicrometerMetricsCollector(registry))
            .build();
    connection = environment.connectionBuilder().build();
    states = new CopyOnWriteArrayList<>();
    consumers = new CopyOnWriteArrayList<>();
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void listenersShouldOnlyBeNotifiedOfOpeningAndGaugeShouldNotChange(boolean queueExists) {
    String q = UUID.randomUUID().toString();
    connection.management().queue(q).exclusive(true).declare();
    connection.consumerBuilder().queue(q).messageHandler((ctx, msg) -> {}).build();
    assertThat(openConsumers()).isEqualTo(1);

    AmqpConsumer c;
    if (queueExists) {
      // exclusive queue of another connection
      try (Connection otherConnection = environment.connectionBuilder().build()) {
        c = failedConsumer(otherConnection, q, AmqpException.class);
      }
    } else {
      c = failedConsumer();
    }
    // no state change, so no other notification
    assertThat(c.state()).isEqualTo(OPENING);
    assertThat(states).containsExactly(OPENING);
    assertThat(openConsumers()).isEqualTo(1);
  }

  @Test
  void closingConsumerWhoseCreationFailedShouldBeANoOp() {
    AmqpConsumer c = failedConsumer();
    c.close();
    assertThat(c.state()).isEqualTo(OPENING);
    assertThat(states).containsExactly(OPENING);
    assertThat(openConsumers()).isZero();
  }

  @Test
  void closingBuiltConsumerShouldNotifyListenersAndUpdateGauge() {
    String q = UUID.randomUUID().toString();
    connection.management().queue(q).exclusive(true).declare();
    Consumer consumer =
        connection
            .consumerBuilder()
            .queue(q)
            .messageHandler((ctx, msg) -> {})
            .listeners(ctx -> states.add(ctx.currentState()))
            .build();
    assertThat(openConsumers()).isEqualTo(1);
    consumer.close();
    waitAtMost(() -> states.contains(CLOSED));
    assertThat(states).containsExactly(OPENING, OPEN, CLOSING, CLOSED);
    assertThat(openConsumers()).isZero();
  }

  private AmqpConsumer failedConsumer() {
    return failedConsumer(
        connection,
        UUID.randomUUID().toString(),
        AmqpException.AmqpEntityDoesNotExistException.class);
  }

  private AmqpConsumer failedConsumer(
      Connection connection, String queue, Class<? extends AmqpException> expectedException) {
    assertThatThrownBy(() ->
            connection
                .consumerBuilder()
                .queue(queue)
                .messageHandler((ctx, msg) -> {})
                .listeners(ctx -> {
                  consumers.add((AmqpConsumer) ctx.resource());
                  states.add(ctx.currentState());
                })
                .build())
        .isInstanceOf(expectedException);
    waitAtMost(() -> states.contains(OPENING));
    return consumers.get(0);
  }

  private double openConsumers() {
    return registry.get("rabbitmq.amqp.consumers").gauge().value();
  }
}
