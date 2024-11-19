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

import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersion.RABBITMQ_4_1_0;
import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static java.lang.System.currentTimeMillis;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersionAtLeast;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfOauth2AuthBackendNotEnabled;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.time.Duration;
import java.util.List;
import org.jose4j.base64url.Base64;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.NumericDate;
import org.jose4j.keys.HmacKey;
import org.jose4j.lang.JoseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@AmqpTestInfrastructure
@DisabledIfOauth2AuthBackendNotEnabled
public class Oauth2Test {

  private static final String BASE64_KEY = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH";

  Environment environment;

  @Test
  void expiredTokenShouldFail() throws JoseException {
    String expiredToken = token(currentTimeMillis() - 1000);
    assertThatThrownBy(
            () -> environment.connectionBuilder().username("").password(expiredToken).build())
        .isInstanceOf(AmqpException.AmqpSecurityException.class);
  }

  @Test
  void validTokenShouldSucceed() throws JoseException {
    String validToken = token(currentTimeMillis() + Duration.ofMinutes(10).toMillis());
    try (Connection ignored =
        environment.connectionBuilder().username("").password(validToken).build()) {}
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void connectionShouldBeClosedWhenTokenExpires(TestInfo info) throws JoseException {
    String q = TestUtils.name(info);
    long expiry = currentTimeMillis() + ofSeconds(2).toMillis();
    String token = token(expiry);
    Sync connectionClosedSync = sync();
    Connection c =
        environment
            .connectionBuilder()
            .username("")
            .password(token)
            .listeners(closedOnSecurityExceptionListener(connectionClosedSync))
            .build();
    c.management().queue(q).exclusive(true).declare();
    Sync publisherClosedSync = sync();
    Publisher p =
        c.publisherBuilder()
            .queue(q)
            .listeners(closedOnSecurityExceptionListener(publisherClosedSync))
            .build();
    Sync consumeSync = sync();
    Sync consumerClosedSync = sync();
    c.consumerBuilder()
        .queue(q)
        .messageHandler(
            (ctx, msg) -> {
              ctx.accept();
              consumeSync.down();
            })
        .listeners(closedOnSecurityExceptionListener(consumerClosedSync))
        .build();
    p.publish(p.message(), ctx -> {});
    assertThat(consumeSync).completes();
    long newExpiry = currentTimeMillis() + ofSeconds(3).toMillis();
    token = token(newExpiry);
    ((AmqpManagement) c.management()).setToken(token);
    // wait for the first token to expire
    waitAtMost(() -> currentTimeMillis() > expiry + ofMillis(500).toMillis());
    // the connection should still work thanks to the new token
    consumeSync.reset();
    p.publish(p.message(), ctx -> {});
    assertThat(consumeSync).completes();
    waitAtMost(() -> currentTimeMillis() > newExpiry);
    assertThat(connectionClosedSync).completes();
    assertThat(publisherClosedSync).completes();
    assertThat(consumerClosedSync).completes();
  }

  private static String token(long expirationTime) throws JoseException {
    JwtClaims claims = new JwtClaims();
    claims.setIssuer("unit_test");
    claims.setAudience("rabbitmq");
    claims.setExpirationTime(NumericDate.fromMilliseconds(expirationTime));
    claims.setStringListClaim(
        "scope", List.of("rabbitmq.configure:*/*", "rabbitmq.write:*/*", "rabbitmq.read:*/*"));

    JsonWebSignature signature = new JsonWebSignature();

    byte[] key = Base64.decode(BASE64_KEY);
    HmacKey hmacKey = new HmacKey(key);
    signature.setKeyIdHeaderValue("token-key");
    signature.setAlgorithmHeaderValue(AlgorithmIdentifiers.HMAC_SHA256);
    signature.setKey(hmacKey);
    signature.setPayload(claims.toJson());

    return signature.getCompactSerialization();
  }
}
