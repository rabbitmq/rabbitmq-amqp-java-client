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

public class DefaultAddressBuilderTest {

  @Test
  void pathSegmentsShouldBePercentEncoded() {
    assertThat(b().exchange("foo").address()).isEqualTo("/exchanges/foo");
    assertThat(b().exchange("foo").key("bar").address()).isEqualTo("/exchanges/foo/bar");
    assertThat(b().exchange("foo bar").address()).isEqualTo("/exchanges/foo%20bar");
    assertThat(b().exchange("foo").key("b ar").address()).isEqualTo("/exchanges/foo/b%20ar");
    assertThat(b().queue("foo").address()).isEqualTo("/queues/foo");
    assertThat(b().queue("foo bar").address()).isEqualTo("/queues/foo%20bar");
  }

  TestAddressBuilder b() {
    return new TestAddressBuilder();
  }

  private static class TestAddressBuilder extends DefaultAddressBuilder<TestAddressBuilder> {

    TestAddressBuilder() {
      super(null);
    }

    @Override
    TestAddressBuilder result() {
      return this;
    }
  }
}
