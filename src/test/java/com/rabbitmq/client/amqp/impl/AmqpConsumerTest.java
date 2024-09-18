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

import static com.rabbitmq.client.amqp.impl.AmqpConsumer.checkAnnotations;
import static java.util.Map.of;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class AmqpConsumerTest {

  @Test
  void checkAnnotationsOK() {
    checkAnnotations(of());
    checkAnnotations(of("x-opt-foo", "bar"));
    checkAnnotations(of("x-opt-foo-1", "bar1", "x-opt-foo-2", "bar2"));
  }

  @Test
  void checkAnnotationsKO() {
    assertThatThrownBy(() -> checkAnnotations(of("foo", "bar")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> checkAnnotations(of("x-opt-foo", "bar1", "foo", "bar2")))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
