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
package com.rabbitmq.client.amqp.observation.micrometer;

import static com.rabbitmq.client.amqp.observation.micrometer.Utils.exchangeRoutingKeyFromTo;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class UtilsTest {

  @ParameterizedTest
  @CsvSource(
      value = {
        "/exchanges/foo/bar,foo,bar",
        "/exchanges/foo,foo,NULL",
        "/exchanges/foo%20bar,foo bar,NULL",
        "/exchanges/foo%20bar/bar%25foo,foo bar,bar%foo",
        "/queues/bar,'',bar",
        "/queues/foo%20bar,'',foo bar",
        "not a address,NULL,NULL"
      },
      nullValues = "NULL")
  void testExchangeRoutingKeyFromTo(String to, String ex, String rk) {
    assertThat(exchangeRoutingKeyFromTo(to)).containsExactly(ex, rk);
  }
}
