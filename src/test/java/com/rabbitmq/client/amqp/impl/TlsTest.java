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

import static com.rabbitmq.client.amqp.impl.TestUtils.environmentBuilder;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfAuthMechanismSslNotEnabled;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfTlsNotEnabled;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import javax.net.ssl.*;
import org.junit.jupiter.api.*;

@DisabledIfTlsNotEnabled
@AmqpTestInfrastructure
public class TlsTest {

  static Environment environment;

  @Test
  void publishWithVerifiedConnectionConsumeWithUnverifiedConnection(TestInfo info)
      throws Exception {
    try (Connection publishingConnection =
            environment
                .connectionBuilder()
                .tls()
                .sslContext(
                    TlsTestUtils.sslContext(
                        TlsTestUtils.trustManagerFactory(TlsTestUtils.caCertificate())))
                .connection()
                .build();
        Connection consumingConnection =
            environment
                .connectionBuilder()
                .tls()
                .sslContext(TlsTestUtils.alwaysTrustSslContext())
                .connection()
                .build()) {

      int messageCount = 1000;
      String q = TestUtils.name(info);
      Management management = publishingConnection.management();
      management.queue(q).autoDelete(true).declare();
      Publisher publisher = publishingConnection.publisherBuilder().queue(q).build();
      CountDownLatch publishLatch = new CountDownLatch(messageCount);
      IntStream.range(0, messageCount)
          .forEach(
              ignored ->
                  publisher.publish(
                      publisher.message("hello".getBytes(UTF_8)), ctx -> publishLatch.countDown()));
      Assertions.assertThat(publishLatch).completes();
      Assertions.assertThat(management.queueInfo(q)).hasMessageCount(messageCount);

      CountDownLatch consumeLatch = new CountDownLatch(messageCount);
      consumingConnection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, msg) -> {
                consumeLatch.countDown();
                ctx.accept();
              })
          .build();
      Assertions.assertThat(consumeLatch).completes();
      TestUtils.waitAtMost(() -> management.queueInfo(q).messageCount() == 0);
    }
  }

  @Test
  void connectionConfigurationShouldOverrideEnvironmentConfiguration() throws Exception {
    try (Environment env =
        environmentBuilder()
            .connectionSettings()
            .tls()
            .sslContext(TlsTestUtils.alwaysTrustSslContext())
            .connection()
            .environmentBuilder()
            .build()) {

      // default environment settings, should work
      env.connectionBuilder().build();

      // using the client certificate in the trust manager, should fail
      assertThatThrownBy(
              () ->
                  env.connectionBuilder()
                      .tls()
                      .sslContext(
                          TlsTestUtils.sslContext(
                              TlsTestUtils.trustManagerFactory(TlsTestUtils.clientCertificate())))
                      .connection()
                      .build())
          .isInstanceOf(AmqpException.AmqpSecurityException.class)
          .hasCauseInstanceOf(SSLHandshakeException.class);
    }
  }

  @Test
  void trustEverythingMode() {
    try (Connection ignored =
        environment.connectionBuilder().tls().trustEverything().connection().build()) {}
  }

  @Test
  void trustEverythingSslContext() throws Exception {
    try (Connection ignored =
        environment
            .connectionBuilder()
            .tls()
            .sslContext(TlsTestUtils.alwaysTrustSslContext())
            .connection()
            .build()) {}
  }

  @Test
  void verifiedConnectionWithCorrectServerCertificate() throws Exception {
    SSLContext sslContext =
        TlsTestUtils.sslContext(TlsTestUtils.trustManagerFactory(TlsTestUtils.caCertificate()));
    try (Connection ignored =
        environment.connectionBuilder().tls().sslContext(sslContext).connection().build()) {}
  }

  @Test
  void verifiedConnectionWithWrongServerCertificate() throws Exception {
    SSLContext sslContext =
        TlsTestUtils.sslContext(TlsTestUtils.trustManagerFactory(TlsTestUtils.clientCertificate()));
    assertThatThrownBy(
            () -> environment.connectionBuilder().tls().sslContext(sslContext).connection().build())
        .isInstanceOf(AmqpException.AmqpSecurityException.class)
        .hasCauseInstanceOf(SSLHandshakeException.class);
  }

  @Test
  @DisabledIfAuthMechanismSslNotEnabled
  void saslExternalShouldSucceedWithUserForClientCertificate() throws Exception {
    X509Certificate clientCertificate = TlsTestUtils.clientCertificate();
    SSLContext sslContext =
        TlsTestUtils.sslContext(
            TlsTestUtils.keyManagerFactory(TlsTestUtils.clientKey(), clientCertificate),
            TlsTestUtils.trustManagerFactory(TlsTestUtils.caCertificate()));
    String username = clientCertificate.getSubjectX500Principal().getName();
    Cli.rabbitmqctlIgnoreError(format("delete_user %s", username));
    Cli.rabbitmqctl(format("add_user %s foo", username));
    try {
      Cli.rabbitmqctl(format("set_permissions %s '.*' '.*' '.*'", username));
      try (Connection ignored =
          environment
              .connectionBuilder()
              .username(UUID.randomUUID().toString())
              .saslMechanism(ConnectionSettings.SASL_MECHANISM_EXTERNAL)
              .tls()
              .sslContext(sslContext)
              .connection()
              .build()) {}
    } finally {
      Cli.rabbitmqctl(format("delete_user %s", username));
    }
  }
}
