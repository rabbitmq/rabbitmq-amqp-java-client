// Copyright (c) 2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.client.amqp.impl.TlsTestUtils.caCertificate;
import static com.rabbitmq.client.amqp.impl.TlsTestUtils.sslContext;
import static com.rabbitmq.client.amqp.impl.TlsTestUtils.trustManagerFactory;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfTlsNotEnabled;
import com.rabbitmq.client.amqp.impl.TestUtils.ErlangVersionAtLeast;
import java.security.Security;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;

@DisabledIfTlsNotEnabled
@AmqpTestInfrastructure
class AdvancedTlsTest {

  String protocol = "TLSv1.3";
  String group = "X25519MLKEM768";
  String cipher = "TLS_AES_256_GCM_SHA384";

  static Environment environment;

  @Test
  @EnabledForJreRange(minVersion = 27)
  @ErlangVersionAtLeast(28)
  void jssePqcGroupNegotiation() throws Exception {
    SSLContext sslContext = sslContext(trustManagerFactory(caCertificate()));
    AtomicReference<SSLEngine> sslEngine = new AtomicReference<>();
    try (Connection ignored =
        environment
            .connectionBuilder()
            .tls()
            .sslContext(sslContext)
            .namedGroups(group)
            .ciphers(cipher)
            .sslEngineCustomizer(sslEngine::set)
            .connection()
            .build()) {
      assertThat(sslEngine).doesNotHaveNullValue();
      SSLSession session = sslEngine.get().getSession();
      assertThat(session.getCipherSuite()).isEqualTo(cipher);
      assertThat(session.getProtocol()).isEqualTo(protocol);
    }
  }

  @Test
  @ErlangVersionAtLeast(28)
  void bouncyCastlePqcGroupNegotiation() throws Exception {
    if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      Security.addProvider(new BouncyCastleProvider());
    }
    if (Security.getProvider(BouncyCastleJsseProvider.PROVIDER_NAME) == null) {
      Security.addProvider(new BouncyCastleJsseProvider());
    }
    SSLContext sslContext = SSLContext.getInstance("TLS", BouncyCastleJsseProvider.PROVIDER_NAME);
    sslContext.init(null, trustManagerFactory(caCertificate()).getTrustManagers(), null);
    AtomicReference<SSLEngine> sslEngine = new AtomicReference<>();
    try (Connection ignored =
        environment
            .connectionBuilder()
            .tls()
            .sslContext(sslContext)
            .namedGroups(group)
            .ciphers(cipher)
            .sslEngineCustomizer(sslEngine::set)
            .connection()
            .build()) {
      assertThat(sslEngine).doesNotHaveNullValue();
      SSLSession session = sslEngine.get().getSession();
      assertThat(session.getCipherSuite()).isEqualTo(cipher);
      assertThat(session.getProtocol()).isEqualTo(protocol);
    }
  }
}
