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
import static com.rabbitmq.client.amqp.impl.TestUtils.sync;
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import com.rabbitmq.client.amqp.oauth.Token;
import com.rabbitmq.client.amqp.oauth.TokenRequester;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TokenCredentialsTest {

  ScheduledExecutorService scheduledExecutorService;
  AutoCloseable mocks;
  @Mock TokenRequester requester;

  @BeforeEach
  void init() {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    this.scheduledExecutorService.shutdownNow();
    this.mocks.close();
  }

  @Test
  void refreshShouldStopOnceUnregistered() throws InterruptedException {
    Duration tokenExpiry = ofMillis(50);
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestCount.incrementAndGet();
              return token("ok", Instant.now().plus(tokenExpiry));
            });
    TokenCredentials credentials =
        new TokenCredentials(this.requester, this.scheduledExecutorService);
    int expectedRefreshCount = 3;
    AtomicInteger refreshCount = new AtomicInteger();
    Sync refreshSync = sync(expectedRefreshCount);
    Credentials.Registration registration =
        credentials.register(
            "",
            (u, p) -> {
              refreshCount.incrementAndGet();
              refreshSync.down();
            });
    registration.connect(connectionCallback(() -> {}));
    assertThat(requestCount).hasValue(1);
    assertThat(refreshSync).completes();
    assertThat(requestCount).hasValue(expectedRefreshCount + 1);
    registration.unregister();
    assertThat(refreshCount).hasValue(expectedRefreshCount);
    assertThat(requestCount).hasValue(expectedRefreshCount + 1);
    Thread.sleep(tokenExpiry.multipliedBy(2).toMillis());
    assertThat(refreshCount).hasValue(expectedRefreshCount);
    assertThat(requestCount).hasValue(expectedRefreshCount + 1);
  }

  @Test
  void severalRegistrationsShouldBeRefreshed() throws InterruptedException {
    Duration tokenExpiry = ofMillis(50);
    Duration waitTime = tokenExpiry.dividedBy(4);
    Duration timeout = tokenExpiry.multipliedBy(20);
    when(this.requester.request())
        .thenAnswer(ignored -> token("ok", Instant.now().plus(tokenExpiry)));
    TokenCredentials credentials =
        new TokenCredentials(this.requester, this.scheduledExecutorService);
    int expectedRefreshCountPerConnection = 3;
    int connectionCount = 10;
    AtomicInteger totalRefreshCount = new AtomicInteger();
    List<Tuples.Pair<Credentials.Registration, Sync>> registrations =
        range(0, connectionCount)
            .mapToObj(
                ignored -> {
                  Sync sync = sync(expectedRefreshCountPerConnection);
                  Credentials.Registration r =
                      credentials.register(
                          "",
                          (username, password) -> {
                            totalRefreshCount.incrementAndGet();
                            sync.down();
                          });
                  return Tuples.pair(r, sync);
                })
            .collect(toList());

    registrations.forEach(r -> r.v1().connect(connectionCallback(() -> {})));
    registrations.forEach(r -> assertThat(r.v2()).completes());
    // all connections have been refreshed once
    int refreshCountSnapshot = totalRefreshCount.get();
    assertThat(refreshCountSnapshot).isEqualTo(connectionCount * expectedRefreshCountPerConnection);

    // unregister half of the connections
    int splitCount = connectionCount / 2;
    registrations.subList(0, splitCount).forEach(r -> r.v1().unregister());
    // only the remaining connections should get refreshed again
    waitAtMost(
        timeout, waitTime, () -> totalRefreshCount.get() == refreshCountSnapshot + splitCount);
    // waiting another round of refresh
    waitAtMost(
        timeout, waitTime, () -> totalRefreshCount.get() == refreshCountSnapshot + splitCount * 2);
    // unregister all connections
    registrations.forEach(r -> r.v1().unregister());
    // wait 2 expiry times
    Thread.sleep(tokenExpiry.multipliedBy(2).toMillis());
    // no new refresh
    assertThat(totalRefreshCount).hasValue(refreshCountSnapshot + splitCount * 2);
  }

  @Test
  void refreshDelayStrategy() {
    Duration diff = ofMillis(100);
    Function<Instant, Duration> strategy = TokenCredentials.ratioRefreshDelayStrategy(0.8f);
    assertThat(strategy.apply(Instant.now().plusSeconds(10))).isCloseTo(ofSeconds(8), diff);
    assertThat(strategy.apply(Instant.now().minusSeconds(10))).isEqualTo(ofSeconds(1));
  }

  private static Token token(String value, Instant expirationTime) {
    return new Token() {
      @Override
      public String value() {
        return value;
      }

      @Override
      public Instant expirationTime() {
        return expirationTime;
      }
    };
  }

  private static Credentials.AuthenticationCallback connectionCallback(Runnable passwordCallback) {
    return (username, password) -> passwordCallback.run();
  }
}
