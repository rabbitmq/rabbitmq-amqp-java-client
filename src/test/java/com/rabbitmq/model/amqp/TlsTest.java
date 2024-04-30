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
package com.rabbitmq.model.amqp;

import static com.rabbitmq.model.amqp.TestUtils.environmentBuilder;
import static com.rabbitmq.model.amqp.TlsTestUtils.*;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.model.Connection;
import com.rabbitmq.model.ConnectionSettings;
import com.rabbitmq.model.Environment;
import com.rabbitmq.model.ModelException;
import com.rabbitmq.model.amqp.TestUtils.DisabledIfAuthMechanismSslNotEnabled;
import com.rabbitmq.model.amqp.TestUtils.DisabledIfTlsNotEnabled;
import java.security.cert.X509Certificate;
import java.util.UUID;
import javax.net.ssl.*;
import org.junit.jupiter.api.*;

@DisabledIfTlsNotEnabled
public class TlsTest {

  static Environment environment;

  @BeforeAll
  static void initAll() {
    environment = environmentBuilder().build();
  }

  @AfterAll
  static void tearDownAll() {
    environment.close();
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
            .sslContext(alwaysTrustSslContext())
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
        .isInstanceOf(ModelException.class)
        .hasCauseInstanceOf(SSLHandshakeException.class);
  }

  @Test
  @DisabledIfAuthMechanismSslNotEnabled
  void saslExternalShouldSucceedWithUserForClientCertificate() throws Exception {
    X509Certificate clientCertificate = clientCertificate();
    SSLContext sslContext =
        sslContext(
            keyManagerFactory(clientKey(), clientCertificate),
            trustManagerFactory(caCertificate()));
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
