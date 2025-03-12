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
import static com.rabbitmq.client.amqp.impl.HttpTestUtils.generateKeyPair;
import static com.rabbitmq.client.amqp.impl.HttpTestUtils.startServer;
import static com.rabbitmq.client.amqp.impl.JwtTestUtils.*;
import static com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersion.RABBITMQ_4_1_0;
import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static com.rabbitmq.client.amqp.oauth2.OAuth2TestUtils.sampleJsonToken;
import static com.rabbitmq.client.amqp.oauth2.TokenCredentialsManager.ratioRefreshDelayStrategy;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.*;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder.EnvironmentConnectionSettings;
import com.rabbitmq.client.amqp.impl.DefaultConnectionSettings.DefaultOAuth2Settings;
import com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersionAtLeast;
import com.rabbitmq.client.amqp.impl.TestUtils.DisabledIfOauth2AuthBackendNotEnabled;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@DisabledIfOauth2AuthBackendNotEnabled
public class Oauth2Test {

  Environment environment;
  HttpServer server;

  @BeforeEach
  void init() {
    environment = TestUtils.environmentBuilder().build();
  }

  @AfterEach
  void tearDown() {
    environment.close();
    if (this.server != null) {
      server.stop(0);
    }
  }

  @Test
  void validTokenShouldSucceed() {
    String validToken = token(currentTimeMillis() + ofMinutes(10).toMillis());
    try (Connection ignored =
        environment.connectionBuilder().username("").password(validToken).build()) {}
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void connectionShouldBeClosedWhenTokenExpires(TestInfo info) {
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
  void openingConnectionWithExpiredTokenShouldFail() throws Exception {
    int port = randomNetworkPort();
    String contextPath = "/uaa/oauth/token";
    String uri = "http://localhost:" + port + contextPath;

    this.server =
        startServer(
            port,
            contextPath,
            oAuth2TokenHttpHandler(() -> currentTimeMillis() - ofMinutes(60).toMillis()));

    assertThatThrownBy(
            () ->
                environment
                    .connectionBuilder()
                    .oauth2()
                    .tokenEndpointUri(uri)
                    .clientId("rabbitmq")
                    .clientSecret("rabbitmq")
                    .connection()
                    .build())
        .isInstanceOf(AmqpException.AmqpSecurityException.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void tokenShouldBeRefreshedAutomatically(boolean shared, TestInfo info) throws Exception {
    int port = randomNetworkPort();
    String contextPath = "/uaa/oauth/token";
    int connectionCount = 10;
    int refreshRounds = 3;
    int expectedRefreshCount = shared ? refreshRounds : refreshRounds * connectionCount;
    Sync tokenRequestSync = sync(expectedRefreshCount);
    AtomicInteger refreshCount = new AtomicInteger();
    HttpHandler httpHandler =
        oAuth2TokenHttpHandler(
            () -> currentTimeMillis() + 3_000,
            () -> {
              refreshCount.incrementAndGet();
              tokenRequestSync.down();
            });
    this.server = startServer(port, contextPath, httpHandler);

    String uri = "http://localhost:" + port + contextPath;
    AmqpEnvironmentBuilder envBuilder = TestUtils.environmentBuilder();
    DefaultOAuth2Settings<? extends EnvironmentConnectionSettings> oauth2 =
        (DefaultOAuth2Settings<? extends EnvironmentConnectionSettings>)
            envBuilder.connectionSettings().oauth2();
    // the broker works at the second level for expiration
    // we have to make sure to renew fast enough for short-lived tokens
    oauth2.refreshDelayStrategy(ratioRefreshDelayStrategy(0.4f));
    oauth2
        .tokenEndpointUri(uri)
        .clientId("rabbitmq")
        .clientSecret("rabbitmq")
        .shared(shared)
        .connection()
        .environmentBuilder();
    try (Environment env = envBuilder.build()) {
      List<Tuples.Pair<Publisher, Sync>> states =
          IntStream.range(0, connectionCount)
              .mapToObj(
                  ignored -> {
                    Connection c = env.connectionBuilder().build();
                    String q = name(info);
                    c.management().queue(q).exclusive(true).declare();
                    Publisher publisher = c.publisherBuilder().queue(q).build();
                    Sync consumeSync = sync();
                    c.consumerBuilder()
                        .queue(q)
                        .messageHandler(
                            (ctx, msg) -> {
                              ctx.accept();
                              consumeSync.down();
                            })
                        .build();
                    return Tuples.pair(publisher, consumeSync);
                  })
              .collect(toList());

      Runnable publish = () -> states.forEach(s -> s.v1().publish(s.v1().message(), ctx -> {}));
      Runnable expectMessages = () -> states.forEach(s -> assertThat(s.v2()).completes());

      publish.run();
      expectMessages.run();

      assertThat(tokenRequestSync).completes();

      publish.run();
      expectMessages.run();
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void tokenOnHttpsShouldBeRefreshed(TestInfo info) throws Exception {
    KeyStore keyStore = generateKeyPair();

    Sync tokenRefreshedSync = sync(3);
    int port = randomNetworkPort();
    String contextPath = "/uaa/oauth/token";
    HttpHandler httpHandler =
        oAuth2TokenHttpHandler(() -> currentTimeMillis() + 3_000, tokenRefreshedSync::down);
    this.server = startServer(port, contextPath, keyStore, httpHandler);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
    tmf.init(keyStore);
    sslContext.init(null, tmf.getTrustManagers(), null);

    String uri = "https://localhost:" + port + contextPath;
    OAuth2Settings<? extends ConnectionBuilder> oauth2 =
        environment
            .connectionBuilder()
            .oauth2()
            .tokenEndpointUri(uri)
            .clientId("rabbitmq")
            .clientSecret("rabbitmq")
            .tls()
            .sslContext(sslContext)
            .oauth2();
    // the broker works at the second level for expiration
    // we have to make sure to renew fast enough for short-lived tokens
    ((DefaultOAuth2Settings<?>) oauth2).refreshDelayStrategy(ratioRefreshDelayStrategy(0.4f));
    Connection c = oauth2.connection().build();

    String q = name(info);
    c.management().queue(q).exclusive(true).declare();
    Publisher publisher = c.publisherBuilder().queue(q).build();
    Sync consumeSync = sync();
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

    assertThat(tokenRefreshedSync).completes();

    consumeSync.reset();
    publisher.publish(publisher.message(), ctx -> {});
    assertThat(consumeSync).completes();
  }

  @Test
  void oauthConfigurationShouldUsePlainSaslMechanism() throws Exception {
    int port = randomNetworkPort();
    String contextPath = "/uaa/oauth/token";
    HttpHandler httpHandler = oAuth2TokenHttpHandler(() -> currentTimeMillis() + 3_000);
    this.server = startServer(port, contextPath, httpHandler);

    String uri = "http://localhost:" + port + contextPath;
    try (Environment env =
        TestUtils.environmentBuilder()
            .connectionSettings()
            .oauth2()
            .tokenEndpointUri(uri)
            .clientId("rabbitmq")
            .clientSecret("rabbitmq")
            .connection()
            .environmentBuilder()
            .build()) {

      Connection c1 = env.connectionBuilder().build();
      Connection c2 =
          env.connectionBuilder()
              .oauth2()
              .tokenEndpointUri(uri)
              .clientId("rabbitmq")
              .clientSecret("rabbitmq")
              .shared(false)
              .connection()
              .build();

      List<String> connectionNames = List.of(c1.toString(), c2.toString());
      List<Cli.ConnectionInfo> connections =
          Cli.listConnections().stream()
              .filter(c -> connectionNames.contains(c.clientProvidedName()))
              .collect(toList());
      assertThat(connections)
          .hasSameSizeAs(connectionNames)
          .as("Authentication mechanism should be PLAIN")
          .allMatch(c -> SASL_MECHANISM_PLAIN.equals(c.authMechanism()));
    }
  }

  private static HttpHandler oAuth2TokenHttpHandler(LongSupplier expirationTimeSupplier) {
    return oAuth2TokenHttpHandler(expirationTimeSupplier, () -> {});
  }

  private static HttpHandler oAuth2TokenHttpHandler(
      LongSupplier expirationTimeSupplier, Runnable requestCallback) {
    return exchange -> {
      long expirationTime = expirationTimeSupplier.getAsLong();
      String jwtToken = token(expirationTime);
      Duration expiresIn = Duration.ofMillis(expirationTime - System.currentTimeMillis());
      String oauthToken = sampleJsonToken(jwtToken, expiresIn);
      byte[] data = oauthToken.getBytes(UTF_8);
      Headers responseHeaders = exchange.getResponseHeaders();
      responseHeaders.set("content-type", "application/json");
      exchange.sendResponseHeaders(200, data.length);
      OutputStream responseBody = exchange.getResponseBody();
      responseBody.write(data);
      responseBody.close();
      requestCallback.run();
    };
  }
}
