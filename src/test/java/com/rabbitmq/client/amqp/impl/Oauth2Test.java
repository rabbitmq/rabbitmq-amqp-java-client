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
import static com.rabbitmq.client.amqp.impl.HttpTestUtils.startHttpServer;
import static com.rabbitmq.client.amqp.impl.JwtTestUtils.*;
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
import com.rabbitmq.client.amqp.oauth.TokenParser;
import com.rabbitmq.client.amqp.oauth.TokenRequester;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.function.LongSupplier;
import org.jose4j.lang.JoseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@AmqpTestInfrastructure
@DisabledIfOauth2AuthBackendNotEnabled
public class Oauth2Test {

  Environment environment;
  HttpServer server;

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
    String q = name(info);
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
  void openingConnectionWithHttpValidTokenShouldWork(TestInfo info) throws Exception {
    String q = name(info);
    int port = randomNetworkPort();
    String contextPath = "/uaa/oauth/token";

    this.server =
        startHttpServer(port, contextPath, tokenHttpHandler(() -> currentTimeMillis() + 60_000));

    TokenRequester tokenRequester = httpTokenRequester("http://localhost:" + port + contextPath);
    TokenParser tokenParser =
        json -> {
          String compact = parse(json).get("value").toString();
          return parseToken(compact);
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
    Publisher publisher = c.publisherBuilder().queue(q).build();
    Sync consumeSync = TestUtils.sync();
    c.consumerBuilder()
        .queue(q)
        .messageHandler(
            (ctx, msg) -> {
              ctx.accept();
              consumeSync.down();
            })
        .build();
    publisher.publish(publisher.message(), ctx -> {});
    assertThat(consumeSync).completes();
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void openingConnectionWithHttpExpiredTokenShouldFail() throws Exception {
    int port = randomNetworkPort();
    String contextPath = "/uaa/oauth/token";

    this.server =
        startHttpServer(port, contextPath, tokenHttpHandler(() -> currentTimeMillis() - 60_000));

    TokenRequester tokenRequester = httpTokenRequester("http://localhost:" + port + contextPath);
    TokenParser tokenParser =
        json -> {
          String compact = parse(json).get("value").toString();
          return parseToken(compact);
        };

    CredentialsProvider credentialsProvider =
        new OAuthCredentialsProvider(tokenRequester, tokenParser);
    assertThatThrownBy(
            () ->
                environment
                    .connectionBuilder()
                    .saslMechanism(SASL_MECHANISM_PLAIN)
                    .credentialsProvider(credentialsProvider)
                    .build())
        .isInstanceOf(AmqpException.AmqpSecurityException.class);
  }

  private static HttpHandler tokenHttpHandler(LongSupplier expirationTimeSupplier) {
    return exchange -> {
      long expirationTime = expirationTimeSupplier.getAsLong();
      String token = token(expirationTime);
      byte[] data = String.format("{ 'value': '%s' }", token).replace('\'', '"').getBytes(UTF_8);
      Headers responseHeaders = exchange.getResponseHeaders();
      responseHeaders.set("content-type", "application/json");
      exchange.sendResponseHeaders(200, data.length);
      OutputStream responseBody = exchange.getResponseBody();
      responseBody.write(data);
      responseBody.close();
    };
  }

  private static TokenRequester httpTokenRequester(String uri) {
    return new HttpTokenRequester(uri, "", "", "", Collections.emptyMap(), null, null, null, null);
  }
}
