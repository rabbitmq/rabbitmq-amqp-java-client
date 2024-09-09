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

import static com.rabbitmq.client.amqp.ConnectionSettings.SASL_MECHANISM_PLAIN;
import static com.rabbitmq.client.amqp.impl.DefaultConnectionSettings.DEFAULT_PASSWORD;
import static com.rabbitmq.client.amqp.impl.DefaultConnectionSettings.DEFAULT_USERNAME;

import com.rabbitmq.client.amqp.DefaultUsernamePasswordCredentialsProvider;
import com.rabbitmq.client.amqp.Environment;
import java.util.concurrent.CountDownLatch;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectionSettingsTest {

  @Test
  void environmentCredentialsProviderShouldBeUsedIfNoneSetForConnection() {
    CountDownLatch usernameReturnedLatch = new CountDownLatch(1);
    try (Environment environment =
        TestUtils.environmentBuilder()
            .connectionSettings()
            .saslMechanism(SASL_MECHANISM_PLAIN)
            .credentialsProvider(new LatchCredentialsProvider(usernameReturnedLatch))
            .environmentBuilder()
            .build()) {
      environment.connectionBuilder().build();
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(usernameReturnedLatch).completes();
    }
  }

  @Test
  void environmentCredentialsProviderShouldNotBeUsedIfOneSetForConnection() {
    CountDownLatch environmentUsernameReturnedLatch = new CountDownLatch(1);
    try (Environment environment =
        TestUtils.environmentBuilder()
            .connectionSettings()
            .credentialsProvider(new LatchCredentialsProvider(environmentUsernameReturnedLatch))
            .environmentBuilder()
            .build()) {
      CountDownLatch connectionUsernameReturnedLatch = new CountDownLatch(1);
      environment
          .connectionBuilder()
          .credentialsProvider(new LatchCredentialsProvider(connectionUsernameReturnedLatch))
          .build();
      com.rabbitmq.client.amqp.impl.Assertions.assertThat(connectionUsernameReturnedLatch)
          .completes();
      Assertions.assertThat(environmentUsernameReturnedLatch.getCount()).isEqualTo(1);
    }
  }

  private static class LatchCredentialsProvider extends DefaultUsernamePasswordCredentialsProvider {

    private final CountDownLatch latch;

    public LatchCredentialsProvider(CountDownLatch latch) {
      super(DEFAULT_USERNAME, DEFAULT_PASSWORD);
      this.latch = latch;
    }

    @Override
    public String getUsername() {
      latch.countDown();
      return super.getUsername();
    }
  }
}
