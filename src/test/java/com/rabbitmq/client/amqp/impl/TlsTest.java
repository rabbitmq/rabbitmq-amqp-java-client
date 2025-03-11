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

import static com.rabbitmq.client.amqp.impl.Cli.*;
import static com.rabbitmq.client.amqp.impl.TestUtils.environmentBuilder;
import static com.rabbitmq.client.amqp.impl.TlsTestUtils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.AmqpException.AmqpSecurityException;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfAuthMechanismSslNotEnabled;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfTlsNotEnabled;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfWebSocket;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import javax.net.ssl.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@DisabledIfTlsNotEnabled
@AmqpTestInfrastructure
public class TlsTest {

  private static final String VH = "test_tls";
  private static final String USERNAME = "tls";
  private static final String PASSWORD = "tls";

  static Environment environment;

  @Test
  void publishWithVerifiedConnectionConsumeWithUnverifiedConnection(TestInfo info)
      throws Exception {
    try (Connection publishingConnection =
            environment
                .connectionBuilder()
                .tls()
                .sslContext(sslContext(trustManagerFactory(caCertificate())))
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
                      .sslContext(sslContext(trustManagerFactory(clientCertificate())))
                      .connection()
                      .build())
          .isInstanceOf(AmqpSecurityException.class)
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
    SSLContext sslContext = sslContext(trustManagerFactory(caCertificate()));
    try (Connection ignored =
        environment.connectionBuilder().tls().sslContext(sslContext).connection().build()) {}
  }

  @Test
  void verifiedConnectionWithWrongServerCertificate() throws Exception {
    SSLContext sslContext = sslContext(trustManagerFactory(clientCertificate()));
    assertThatThrownBy(
            () -> environment.connectionBuilder().tls().sslContext(sslContext).connection().build())
        .isInstanceOf(AmqpSecurityException.class)
        .hasCauseInstanceOf(SSLHandshakeException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {DefaultConnectionSettings.DEFAULT_VIRTUAL_HOST, VH})
  @DisabledIfAuthMechanismSslNotEnabled
  void saslExternalShouldSucceedWithUserForClientCertificate(String vh) throws Exception {
    X509Certificate clientCertificate = clientCertificate();
    SSLContext sslContext =
        sslContext(
            keyManagerFactory(clientKey(), clientCertificate),
            trustManagerFactory(caCertificate()));
    String username = clientCertificate.getSubjectX500Principal().getName();
    Runnable connect =
        () -> {
          try (Connection ignored =
              environment
                  .connectionBuilder()
                  .username(UUID.randomUUID().toString())
                  .virtualHost(vh)
                  .saslMechanism(ConnectionSettings.SASL_MECHANISM_EXTERNAL)
                  .tls()
                  .sslContext(sslContext)
                  .connection()
                  .build()) {}
        };
    // there is no user with the client certificate's subject DN
    assertThatThrownBy(connect::run).isInstanceOf(AmqpSecurityException.class);
    try {
      setUpVirtualHost(vh, username, username);
      try (Connection ignored =
          environment
              .connectionBuilder()
              .username(UUID.randomUUID().toString())
              .virtualHost(vh)
              .saslMechanism(ConnectionSettings.SASL_MECHANISM_EXTERNAL)
              .tls()
              .sslContext(sslContext)
              .connection()
              .build()) {}
    } finally {
      tearDownVirtualHost(vh, username);
    }
  }

  @Test
  void hostnameVerificationShouldFailWhenSettingHostToLoopbackInterface() throws Exception {
    SSLContext sslContext = sslContext(trustManagerFactory(caCertificate()));
    assertThatThrownBy(
            () ->
                environment
                    .connectionBuilder()
                    .host("127.0.0.1")
                    .tls()
                    .sslContext(sslContext)
                    .connection()
                    .build())
        .isInstanceOf(AmqpSecurityException.class)
        .cause()
        .isInstanceOf(SSLHandshakeException.class)
        .hasMessageContaining("subject alternative names");
  }

  @Test
  @DisabledIfWebSocket
  void connectToLoopbackInterfaceShouldWorkIfNoHostnameVerification() throws Exception {
    SSLContext sslContext = sslContext(trustManagerFactory(caCertificate()));
    try (Connection ignored =
        environment
            .connectionBuilder()
            .host("127.0.0.1")
            .tls()
            .sslContext(sslContext)
            .hostnameVerification(false)
            .connection()
            .build()) {}
  }

  @Test
  void connectToNonDefaultVirtualHostShouldSucceed() throws Exception {
    try {
      setUpVirtualHost(VH, USERNAME, PASSWORD);

      SSLContext sslContext = sslContext(trustManagerFactory(caCertificate()));
      try (Connection ignored =
          environment
              .connectionBuilder()
              .username(USERNAME)
              .password(PASSWORD)
              .virtualHost(VH)
              .tls()
              .sslContext(sslContext)
              .connection()
              .build()) {}
    } finally {
      tearDownVirtualHost(VH, USERNAME);
    }
  }

  private static void setUpVirtualHost(String vh, String username, String password) {
    if (!isDefaultVirtualHost(vh)) {
      addVhost(vh);
    }
    addUser(username, password);
    setPermissions(username, vh, ".*");
  }

  private static void tearDownVirtualHost(String vh, String username) {
    deleteUser(username);
    if (!isDefaultVirtualHost(vh)) {
      deleteVhost(vh);
    }
  }

  private static boolean isDefaultVirtualHost(String vh) {
    return DefaultConnectionSettings.DEFAULT_VIRTUAL_HOST.equals(vh);
  }
}
