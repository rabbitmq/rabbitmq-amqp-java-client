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

import com.rabbitmq.client.amqp.CredentialsProvider;
import com.rabbitmq.client.amqp.UsernamePasswordCredentialsProvider;
import com.rabbitmq.client.amqp.oauth2.GsonTokenParser;
import com.rabbitmq.client.amqp.oauth2.HttpTokenRequester;
import java.net.http.HttpClient;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

final class CredentialsFactory {

  private volatile Credentials globalOAuth2Credentials;
  private final Lock oauth2CredentialsLock = new ReentrantLock();
  private final AmqpEnvironment environment;

  CredentialsFactory(AmqpEnvironment environment) {
    this.environment = environment;
  }

  Credentials credentials(DefaultConnectionSettings<?> settings) {
    CredentialsProvider provider = settings.credentialsProvider();
    Credentials credentials;
    if (settings.oauth2().enabled()) {
      if (settings.oauth2().shared()) {
        credentials = globalOAuth2Credentials(settings);
      } else {
        credentials = createOAuth2Credentials(settings);
      }
    } else {
      if (provider instanceof UsernamePasswordCredentialsProvider) {
        UsernamePasswordCredentialsProvider credentialsProvider =
            (UsernamePasswordCredentialsProvider) provider;
        credentials = new UsernamePasswordCredentials(credentialsProvider);
      } else {
        credentials = Credentials.NO_OP;
      }
    }
    return credentials;
  }

  private Credentials globalOAuth2Credentials(DefaultConnectionSettings<?> connectionSettings) {
    Credentials result = this.globalOAuth2Credentials;
    if (result != null) {
      return result;
    }

    this.oauth2CredentialsLock.lock();
    try {
      if (this.globalOAuth2Credentials == null) {
        this.globalOAuth2Credentials = createOAuth2Credentials(connectionSettings);
      }
      return this.globalOAuth2Credentials;
    } finally {
      this.oauth2CredentialsLock.unlock();
    }
  }

  private Credentials createOAuth2Credentials(DefaultConnectionSettings<?> connectionSettings) {
    DefaultConnectionSettings.DefaultOAuth2Settings<?> settings = connectionSettings.oauth2();
    Consumer<HttpClient.Builder> clientBuilderConsumer;
    if (settings.tlsEnabled()) {
      clientBuilderConsumer = b -> b.sslContext(settings.tls().sslContext());
    } else {
      clientBuilderConsumer = ignored -> {};
    }
    HttpTokenRequester tokenRequester =
        new HttpTokenRequester(
            settings.tokenEndpointUri(),
            settings.clientId(),
            settings.clientSecret(),
            settings.grantType(),
            settings.parameters(),
            clientBuilderConsumer,
            null,
            new GsonTokenParser());
    return new TokenCredentials(
        tokenRequester, environment.scheduledExecutorService(), settings.refreshDelayStrategy());
  }
}
