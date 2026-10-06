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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.DefaultUsernamePasswordCredentialsProvider;
import com.rabbitmq.client.amqp.Environment;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
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
            .saslMechanism(SASL_MECHANISM_PLAIN)
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

  @Test
  void connectionTimeoutShouldApplyToTcpConnection() {
    String unreachableHost = "10.255.255.1";
    Assumptions.assumeTrue(
        tcpConnectionHangs(unreachableHost),
        "TCP connection to " + unreachableHost + " fails fast");
    try (Environment environment =
        TestUtils.environmentBuilder()
            .connectionSettings()
            .host(unreachableHost)
            .connectionTimeout(Duration.ofMillis(500))
            .environmentBuilder()
            .build()) {
      long start = System.nanoTime();
      assertThatThrownBy(() -> environment.connectionBuilder().build())
          .isInstanceOf(AmqpException.class);
      Assertions.assertThat(Duration.ofNanos(System.nanoTime() - start))
          .isLessThan(Duration.ofSeconds(10));
    }
  }

  @Test
  void invalidConnectionTimeoutShouldBeRejected() {
    try (Environment environment = TestUtils.environmentBuilder().build()) {
      for (Duration timeout :
          new Duration[] {
            null, Duration.ZERO, Duration.ofMillis(-1), Duration.ofMillis(Integer.MAX_VALUE + 1L)
          }) {
        assertThatThrownBy(() -> environment.connectionBuilder().connectionTimeout(timeout))
            .isInstanceOf(IllegalArgumentException.class);
      }
    }
  }

  private static boolean tcpConnectionHangs(String host) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, 5672), 200);
      return false;
    } catch (SocketTimeoutException e) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private static class LatchCredentialsProvider extends DefaultUsernamePasswordCredentialsProvider {

    private final CountDownLatch latch;

    public LatchCredentialsProvider(CountDownLatch latch) {
      super("guest", "guest");
      this.latch = latch;
    }

    @Override
    public String getUsername() {
      latch.countDown();
      return super.getUsername();
    }
  }
}
