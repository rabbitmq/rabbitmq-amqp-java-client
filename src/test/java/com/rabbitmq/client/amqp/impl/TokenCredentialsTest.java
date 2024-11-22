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
import static org.mockito.Mockito.when;

import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import com.rabbitmq.client.amqp.oauth.Token;
import com.rabbitmq.client.amqp.oauth.TokenRequester;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
  void refresh() {
    when(this.requester.request())
        .thenAnswer(ignored -> token("ok", System.currentTimeMillis() + 100));
    TokenCredentials credentials =
        new TokenCredentials(this.requester, this.scheduledExecutorService);
    Sync refreshSync = TestUtils.sync(3);
    Credentials.Registration registration =
        credentials.register(
            (u, p) -> {
              refreshSync.down();
            });
    registration.connect(
        new Credentials.ConnectionCallback() {
          @Override
          public Credentials.ConnectionCallback username(String username) {
            return this;
          }

          @Override
          public Credentials.ConnectionCallback password(String password) {
            return this;
          }
        });
    assertThat(refreshSync).completes();
    registration.unregister();
  }

  private static Token token(String value, long expirationTime) {
    return new Token() {
      @Override
      public String value() {
        return value;
      }

      @Override
      public long expirationTime() {
        return expirationTime;
      }
    };
  }
}
