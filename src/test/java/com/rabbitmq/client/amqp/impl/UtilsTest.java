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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class UtilsTest {

  @ParameterizedTest
  @CsvSource({"/exchange/foo/bar,", "/topic/foo,", "/queue/foo,foo", "foo,"})
  void extractQueueFromSourceAddress(String address, String expectedQueue) {
    assertThat(Utils.extractQueueFromSourceAddress(address)).isEqualTo(expectedQueue);
  }

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
}
