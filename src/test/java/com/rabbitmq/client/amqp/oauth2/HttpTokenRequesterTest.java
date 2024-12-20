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
package com.rabbitmq.client.amqp.oauth2;

import static com.rabbitmq.client.amqp.impl.TestUtils.randomNetworkPort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.amqp.impl.HttpTestUtils;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.http.HttpClient;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class HttpTokenRequesterTest {

  HttpServer server;
  int port;
  String contextPath = "/uaa/oauth/token";

  @BeforeEach
  void init() throws IOException {
    this.port = randomNetworkPort();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void requestToken(boolean tls) throws Exception {
    String protocol;
    KeyStore keyStore;
    Consumer<HttpClient.Builder> clientBuilderConsumer;
    if (tls) {
      protocol = "https";
      keyStore = HttpTestUtils.generateKeyPair();
      SSLContext sslContext = SSLContext.getInstance("TLS");
      TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
      tmf.init(keyStore);
      sslContext.init(null, tmf.getTrustManagers(), null);
      clientBuilderConsumer = b -> b.sslContext(sslContext);
    } else {
      protocol = "http";
      keyStore = null;
      clientBuilderConsumer = b -> {};
    }
    String uri = String.format("%s://localhost:%d%s", protocol, port, contextPath);
    AtomicReference<String> httpMethod = new AtomicReference<>();
    AtomicReference<String> contentType = new AtomicReference<>();
    AtomicReference<String> authorization = new AtomicReference<>();
    AtomicReference<String> accept = new AtomicReference<>();
    AtomicReference<Map<String, String>> httpParameters = new AtomicReference<>();

    String accessToken = UUID.randomUUID().toString();

    Duration expiresIn = Duration.ofSeconds(60);
    server =
        HttpTestUtils.startServer(
            port,
            contextPath,
            keyStore,
            exchange -> {
              Headers headers = exchange.getRequestHeaders();
              httpMethod.set(exchange.getRequestMethod());
              contentType.set(headers.getFirst("content-type"));
              authorization.set(headers.getFirst("authorization"));
              accept.set(headers.getFirst("accept"));

              String requestBody = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
              Map<String, String> parameters =
                  Arrays.stream(requestBody.split("&"))
                      .map(p -> p.split("="))
                      .collect(Collectors.toMap(p -> p[0], p -> p[1]));
              httpParameters.set(parameters);

              byte[] data = OAuth2TestUtils.sampleJsonToken(accessToken, expiresIn).getBytes(UTF_8);

              Headers responseHeaders = exchange.getResponseHeaders();
              responseHeaders.set("content-type", "application/json");
              exchange.sendResponseHeaders(200, data.length);
              OutputStream responseBody = exchange.getResponseBody();
              responseBody.write(data);
              responseBody.close();
            });

    TokenRequester requester =
        new HttpTokenRequester(
            uri,
            "rabbit_client",
            "rabbit_secret",
            "password",
            Map.of("username", "rabbit_username", "password", "rabbit_password"),
            clientBuilderConsumer,
            null,
            StringToken::new);

    String token = requester.request().value();
    assertThat(token).contains(accessToken);
    Gson gson = new Gson();
    TypeToken<Map<String, Object>> mapType = new TypeToken<>() {};
    Map<String, Object> tokenMap = gson.fromJson(token, mapType);
    assertThat(tokenMap)
        .containsEntry("access_token", accessToken)
        .containsEntry("expires_in", (double) expiresIn.toSeconds());

    assertThat(httpMethod).hasValue("POST");
    assertThat(contentType).hasValue("application/x-www-form-urlencoded");
    assertThat(authorization).hasValue("Basic cmFiYml0X2NsaWVudDpyYWJiaXRfc2VjcmV0");
    assertThat(accept).hasValue("application/json");
    Map<String, String> parameters = httpParameters.get();
    assertThat(parameters)
        .isNotNull()
        .hasSize(3)
        .containsEntry("grant_type", "password")
        .containsEntry("username", "rabbit_username")
        .containsEntry("password", "rabbit_password");
  }

  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  private static class StringToken implements Token {

    private final String value;

    private StringToken(String value) {
      this.value = value;
    }

    @Override
    public String value() {
      return this.value;
    }

    @Override
    public Instant expirationTime() {
      return Instant.EPOCH;
    }
  }
}
