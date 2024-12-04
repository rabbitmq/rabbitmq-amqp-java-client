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
import com.rabbitmq.client.amqp.oauth.GsonTokenParser;
import com.rabbitmq.client.amqp.oauth.HttpTokenRequester;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class CredentialsFactory {

  private volatile Credentials globalOAuthCredentials;
  private final Lock oauthCredentialsLock = new ReentrantLock();
  private final AmqpEnvironment environment;

  CredentialsFactory(AmqpEnvironment environment) {
    this.environment = environment;
  }

  Credentials credentials(DefaultConnectionSettings<?> settings) {
    CredentialsProvider provider = settings.credentialsProvider();
    Credentials credentials;
    if (settings.oauth().enabled()) {
      if (settings.oauth().shared()) {
        credentials = globalOAuthCredentials(settings);
      } else {
        credentials = createOAuthCredentials(settings);
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

  private Credentials globalOAuthCredentials(DefaultConnectionSettings<?> connectionSettings) {
    Credentials result = this.globalOAuthCredentials;
    if (result != null) {
      return result;
    }

    this.oauthCredentialsLock.lock();
    try {
      if (this.globalOAuthCredentials == null) {
        this.globalOAuthCredentials = createOAuthCredentials(connectionSettings);
      }
      return this.globalOAuthCredentials;
    } finally {
      this.oauthCredentialsLock.unlock();
    }
  }

  private Credentials createOAuthCredentials(DefaultConnectionSettings<?> connectionSettings) {
    DefaultConnectionSettings.DefaultOAuthSettings<?> settings = connectionSettings.oauth();
    // TODO set TLS configuration on TLS requester
    // TODO use pre-configured token requester if any
    HttpTokenRequester tokenRequester =
        new HttpTokenRequester(
            settings.tokenEndpointUri(),
            settings.clientId(),
            settings.clientSecret(),
            settings.grantType(),
            settings.parameters(),
            null,
            null,
            null,
            null,
            new GsonTokenParser());
    return new TokenCredentials(tokenRequester, environment.scheduledExecutorService());
  }
}
