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

import static com.rabbitmq.client.amqp.impl.Utils.checkMessageAnnotations;
import static java.util.Map.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UtilsTest {

  @Test
  void nameSupplierShouldGenerateRandomNames() {
    assertThat(Utils.NAME_SUPPLIER.get())
        .hasSizeGreaterThan(20)
        .isNotEqualTo(Utils.NAME_SUPPLIER.get())
        .isNotEqualTo(Utils.NAME_SUPPLIER.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {"1Y", "7D", "20m"})
  void validateMaxAgeOK(String input) {
    assertThat(Utils.validateMaxAge(input)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"foo", "1", "10", "", "7DD", "1y", "6g"})
  void validateMaxAgeKO(String input) {
    assertThat(Utils.validateMaxAge(input)).isFalse();
  }

  @Test
  void checkAnnotationsOK() {
    checkMessageAnnotations(of());
    checkMessageAnnotations(of("x-foo", "bar"));
    checkMessageAnnotations(of("x-foo-1", "bar1", "x-foo-2", "bar2"));
  }

  @Test
  void checkAnnotationsKO() {
    assertThatThrownBy(() -> checkMessageAnnotations(of("foo", "bar")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> checkMessageAnnotations(of("x-foo", "bar1", "foo", "bar2")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {
  "3.13.6",
  "3.13.6.2",
  "3.13.6-alpha.0",
  "3.13.6~beta-1",
  "3.13.6+funky-metadata-1",
  "4.0.6",
  "4.0.6.9",
  "4.0.6-alpha.0",
  "4.0.6~beta-1",
  "4.0.6+funky-metadata-1"
  })
  void validateBrokerVersionParsing4AndEarlier(String brokerVersion) {
      assertThat(Utils.is4_1_OrMore(brokerVersion)).isFalse();
      assertThat(Utils.supportFilterExpressions(brokerVersion)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {
  "4.1.6",
  "4.1.6.9",
  "4.1.6-alpha.0",
  "4.1.6~beta-1",
  "4.1.6+funky-metadata-1",
  "4.2.6",
  "4.2.6.9",
  "4.2.6-alpha.0",
  "4.2.6~beta-1",
  "4.2.6+funky-metadata-1"
  })
  void validateBrokerVersionParsing41AndLater(String brokerVersion) {
      assertThat(Utils.is4_1_OrMore(brokerVersion)).isTrue();
      assertThat(Utils.supportFilterExpressions(brokerVersion)).isTrue();
  }
}
