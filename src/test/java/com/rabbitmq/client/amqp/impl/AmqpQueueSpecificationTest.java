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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.rabbitmq.client.amqp.Management;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class AmqpQueueSpecificationTest {

  @Test
  void quorumQueueDelayedRetryType() {
    AmqpManagement management = mock(AmqpManagement.class);
    AmqpQueueSpecification spec = new AmqpQueueSpecification(management);
    spec.quorum().delayedRetryType(Management.DelayedRetryType.ALL);

    Map<String, Object> arguments = new HashMap<>();
    spec.arguments(arguments::put);

    assertThat(arguments).containsEntry("x-delayed-retry-type", "all");

    assertThatThrownBy(() -> spec.quorum().delayedRetryType(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'x-delayed-retry-type' cannot be null");
  }

  @Test
  void quorumQueueDelayedRetryMin() {
    AmqpManagement management = mock(AmqpManagement.class);
    AmqpQueueSpecification spec = new AmqpQueueSpecification(management);
    spec.quorum().delayedRetryMin(Duration.ofSeconds(1));

    Map<String, Object> arguments = new HashMap<>();
    spec.arguments(arguments::put);

    assertThat(arguments).containsEntry("x-delayed-retry-min", 1000L);

    assertThatThrownBy(() -> spec.quorum().delayedRetryMin(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'x-delayed-retry-min' cannot be null");

    assertThatThrownBy(() -> spec.quorum().delayedRetryMin(Duration.ofSeconds(0)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'x-delayed-retry-min' must be positive");
  }

  @Test
  void quorumQueueDelayedRetryMax() {
    AmqpManagement management = mock(AmqpManagement.class);
    AmqpQueueSpecification spec = new AmqpQueueSpecification(management);
    spec.quorum().delayedRetryMax(Duration.ofSeconds(10));

    Map<String, Object> arguments = new HashMap<>();
    spec.arguments(arguments::put);

    assertThat(arguments).containsEntry("x-delayed-retry-max", 10000L);

    assertThatThrownBy(() -> spec.quorum().delayedRetryMax(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'x-delayed-retry-max' cannot be null");

    assertThatThrownBy(() -> spec.quorum().delayedRetryMax(Duration.ofSeconds(0)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'x-delayed-retry-max' must be positive");
  }

  @Test
  void streamInitialOffset() {
    AmqpManagement management = mock(AmqpManagement.class);
    AmqpQueueSpecification spec = new AmqpQueueSpecification(management);

    spec.stream().initialOffset(100L);

    Map<String, Object> arguments = new HashMap<>();
    spec.arguments(arguments::put);

    assertThat(arguments).containsEntry("x-stream-initial-offset", 100L);

    assertThatThrownBy(() -> spec.stream().initialOffset(-1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Initial offset must be between 0 and "
                + AmqpQueueSpecification.MAX_STREAM_INITIAL_OFFSET);

    assertThatThrownBy(
            () -> spec.stream().initialOffset(AmqpQueueSpecification.MAX_STREAM_INITIAL_OFFSET + 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Initial offset must be between 0 and "
                + AmqpQueueSpecification.MAX_STREAM_INITIAL_OFFSET);
  }

  @ParameterizedTest
  @EnumSource(Management.QueueType.class)
  void leaderLocatorShouldSetLeaderLocatorArgumentForAllTypes(Management.QueueType type) {
    for (Management.QueueLeaderLocator locator : Management.QueueLeaderLocator.values()) {
      AmqpQueueSpecification spec = new AmqpQueueSpecification(mock(AmqpManagement.class));
      spec.type(type).leaderLocator(locator);

      assertThat(arguments(spec))
          .containsEntry("x-queue-leader-locator", locator.locator())
          .doesNotContainKey("x-queue-master-locator");
    }
  }

  @Test
  void leaderLocatorShouldBeSetIfTypeIsNotSet() {
    AmqpQueueSpecification spec = new AmqpQueueSpecification(mock(AmqpManagement.class));
    spec.leaderLocator(Management.QueueLeaderLocator.BALANCED);

    assertThat(arguments(spec))
        .containsEntry("x-queue-leader-locator", "balanced")
        .doesNotContainKeys("x-queue-type", "x-queue-master-locator");
  }

  @ParameterizedTest
  @EnumSource(Management.QueueType.class)
  void leaderLocatorShouldBeSetIfTypeIsSetAfter(Management.QueueType type) {
    AmqpQueueSpecification spec = new AmqpQueueSpecification(mock(AmqpManagement.class));
    spec.leaderLocator(Management.QueueLeaderLocator.BALANCED).type(type);

    assertThat(arguments(spec)).containsEntry("x-queue-leader-locator", "balanced");
  }

  @Test
  void leaderLocatorNullShouldRemoveOnlyLeaderLocatorArgument() {
    AmqpQueueSpecification spec = new AmqpQueueSpecification(mock(AmqpManagement.class));
    spec.classic()
        .queue()
        .argument("x-queue-master-locator", "client-local")
        .leaderLocator(Management.QueueLeaderLocator.BALANCED)
        .leaderLocator(null);

    assertThat(arguments(spec))
        .doesNotContainKey("x-queue-leader-locator")
        .containsEntry("x-queue-master-locator", "client-local");
  }

  private static Map<String, Object> arguments(AmqpQueueSpecification spec) {
    Map<String, Object> arguments = new HashMap<>();
    spec.arguments(arguments::put);
    return arguments;
  }
}
