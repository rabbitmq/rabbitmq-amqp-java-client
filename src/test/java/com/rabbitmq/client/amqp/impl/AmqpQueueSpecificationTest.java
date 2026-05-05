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
}
