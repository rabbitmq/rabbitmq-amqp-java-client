// Copyright (c) 2024-2025 Broadcom. All Rights Reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.oauth2.CredentialsManager;
import org.junit.jupiter.api.Test;

public class CredentialsManagerFactoryTest {

  @Test
  void closeShouldCloseGlobalManager() {
    Environment environment = TestUtils.environmentBuilder().build();
    try {
      AmqpEnvironment env = (AmqpEnvironment) environment;
      CredentialsManagerFactory factory = env.credentialsManagerFactory();
      DefaultConnectionSettings<?> settings = oauth2Settings(true);
      CredentialsManager manager = factory.credentials(settings);
      factory.close();
      assertThatThrownBy(() -> manager.register("r", (u, p) -> {}))
          .isInstanceOf(IllegalStateException.class);
    } finally {
      environment.close();
    }
  }

  @Test
  void credentialsShouldFailAfterClose() {
    Environment environment = TestUtils.environmentBuilder().build();
    try {
      AmqpEnvironment env = (AmqpEnvironment) environment;
      CredentialsManagerFactory factory = env.credentialsManagerFactory();
      factory.close();
      assertThatThrownBy(() -> factory.credentials(oauth2Settings(true)))
          .isInstanceOf(IllegalStateException.class);
    } finally {
      environment.close();
    }
  }

  @Test
  void nonSharedManagersAreNotTrackedByFactory() {
    Environment environment = TestUtils.environmentBuilder().build();
    try {
      AmqpEnvironment env = (AmqpEnvironment) environment;
      CredentialsManagerFactory factory = env.credentialsManagerFactory();
      CredentialsManager m1 = factory.credentials(oauth2Settings(false));
      CredentialsManager m2 = factory.credentials(oauth2Settings(false));
      assertThat(m1).isNotSameAs(m2);
      // closing the factory must not fail even though non-shared managers were never registered
      factory.close();
    } finally {
      environment.close();
    }
  }

  private static DefaultConnectionSettings<?> oauth2Settings(boolean shared) {
    DefaultConnectionSettings<?> settings = DefaultConnectionSettings.instance();
    settings
        .oauth2()
        .tokenEndpointUri("http://localhost:1/token")
        .clientId("client")
        .clientSecret("secret")
        .shared(shared);
    settings.consolidate();
    return settings;
  }
}
