// Copyright (c) 2020-2024 Broadcom. All Rights Reserved.
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
package com.rabbitmq.amqp.client;

import static java.util.stream.Stream.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ByteCapacityTest {

  static Stream<Arguments> fromOkArguments() {
    return of(
            arguments("100tb", 100_000_000_000_000L),
            arguments("100gb", 100_000_000_000L),
            arguments("100mb", 100_000_000L),
            arguments("100kb", 100_000L),
            arguments("100", 100L))
        .flatMap(
            arguments ->
                Stream.of(
                    arguments,
                    arguments(arguments.get()[0].toString().toUpperCase(), arguments.get()[1])));
  }

  @ParameterizedTest
  @MethodSource("fromOkArguments")
  void fromOk(String value, long expectedValueInBytes) {
    ByteCapacity capacity = ByteCapacity.from(value);
    assertThat(capacity.toBytes()).isEqualTo(expectedValueInBytes);
    assertThat(capacity.toString()).isEqualTo(value);
  }

  @ValueSource(strings = {"0tb", "0gb", "0mb", "0kb", "0"})
  @ParameterizedTest
  void zero(String value) {
    assertThat(ByteCapacity.from(value).toBytes()).isEqualTo(0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"100.0gb", "abc", "100.0", "-10gb", "10b"})
  void fromKo(String value) {
    assertThatThrownBy(() -> ByteCapacity.from(value)).isInstanceOf(IllegalArgumentException.class);
  }
}
