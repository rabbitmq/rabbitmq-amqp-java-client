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
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersion.RABBITMQ_4_1_0;
import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersionAtLeast;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfOauth2AuthBackendNotEnabled;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import com.rabbitmq.client.amqp.oauth.HttpTokenRequester;
import com.rabbitmq.client.amqp.oauth.Token;
import com.rabbitmq.client.amqp.oauth.TokenParser;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.HmacKey;
import org.jose4j.lang.JoseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@AmqpTestInfrastructure
@DisabledIfOauth2AuthBackendNotEnabled
public class Oauth2Test {

  private static final String BASE64_KEY = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH";
  private static final HmacKey KEY = new HmacKey(Base64.getDecoder().decode(BASE64_KEY));
  Environment environment;
  HttpServer server;

  private static String token(long expirationTime) {
    JwtClaims claims = new JwtClaims();
    claims.setIssuer("unit_test");
    claims.setAudience("rabbitmq");
    claims.setExpirationTime(NumericDate.fromMilliseconds(expirationTime));
    claims.setStringListClaim(
        "scope", List.of("rabbitmq.configure:*/*", "rabbitmq.write:*/*", "rabbitmq.read:*/*"));

    JsonWebSignature signature = new JsonWebSignature();

    signature.setKeyIdHeaderValue("token-key");
    signature.setAlgorithmHeaderValue(AlgorithmIdentifiers.HMAC_SHA256);
    signature.setKey(KEY);
    signature.setPayload(claims.toJson());
    try {
      return signature.getCompactSerialization();
    } catch (JoseException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  void tearDown() {
    if (this.server != null) {
      server.stop(0);
    }
  }

  @Test
  void expiredTokenShouldFail() {
    String expiredToken = token(currentTimeMillis() - 1000);
    assertThatThrownBy(
            () -> environment.connectionBuilder().username("").password(expiredToken).build())
        .isInstanceOf(AmqpException.AmqpSecurityException.class);
  }

  @Test
  void validTokenShouldSucceed() {
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

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void httpClientProviderShouldGetToken(TestInfo info) throws Exception {
    String q = TestUtils.name(info);

    int port = randomNetworkPort();
    String contextPath = "/uaa/oauth/token";

    this.server =
        HttpTestUtils.startHttpServer(
            port,
            contextPath,
            exchange -> {
              long expirationTime = currentTimeMillis() + 60_000;
              byte[] data = token(expirationTime).getBytes(UTF_8);
              Headers responseHeaders = exchange.getResponseHeaders();
              responseHeaders.set("content-type", "application/json");
              exchange.sendResponseHeaders(200, data.length);
              OutputStream responseBody = exchange.getResponseBody();
              responseBody.write(data);
              responseBody.close();
            });

    HttpTokenRequester tokenRequester =
        new HttpTokenRequester(
            "http://localhost:" + port + contextPath,
            "",
            "",
            "",
            Collections.emptyMap(),
            null,
            null,
            null,
            null);
    TokenParser tokenParser =
        tokenAsString -> {
          long expirationTime;
          try {
            JwtConsumer consumer =
                new JwtConsumerBuilder()
                    .setExpectedAudience("rabbitmq")
                    .setVerificationKey(KEY)
                    .build();
            JwtClaims claims = consumer.processToClaims(tokenAsString);
            expirationTime = claims.getExpirationTime().getValueInMillis();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return new Token() {
            @Override
            public String value() {
              return tokenAsString;
            }

            @Override
            public long expirationTime() {
              return expirationTime;
            }
          };
        };

    CredentialsProvider credentialsProvider =
        new OAuthCredentialsProvider(tokenRequester, tokenParser);
    Connection c =
        environment
            .connectionBuilder()
            .saslMechanism(SASL_MECHANISM_PLAIN)
            .credentialsProvider(credentialsProvider)
            .build();
    c.management().queue(q).exclusive(true).declare();
  }
}
