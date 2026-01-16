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
import com.rabbitmq.client.amqp.oauth2.CredentialsManager;
import com.rabbitmq.client.amqp.oauth2.GsonTokenParser;
import com.rabbitmq.client.amqp.oauth2.HttpTokenRequester;
import com.rabbitmq.client.amqp.oauth2.TokenCredentialsManager;
import java.net.HttpURLConnection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

final class CredentialsManagerFactory {

  private volatile CredentialsManager globalOAuth2CredentialsManager;
  private final Lock oauth2CredentialsLock = new ReentrantLock();
  private final AmqpEnvironment environment;

  CredentialsManagerFactory(AmqpEnvironment environment) {
    this.environment = environment;
  }

  CredentialsManager credentials(DefaultConnectionSettings<?> settings) {
    CredentialsProvider provider = settings.credentialsProvider();
    CredentialsManager credentialsManager;
    if (settings.oauth2().enabled()) {
      if (settings.oauth2().shared()) {
        credentialsManager = globalOAuth2Credentials(settings);
      } else {
        credentialsManager = createOAuth2Credentials(settings);
      }
    } else {
      if (provider instanceof UsernamePasswordCredentialsProvider) {
        UsernamePasswordCredentialsProvider credentialsProvider =
            (UsernamePasswordCredentialsProvider) provider;
        credentialsManager = new UsernamePasswordCredentialsManager(credentialsProvider);
      } else {
        credentialsManager = CredentialsManager.NO_OP;
      }
    }
    return credentialsManager;
  }

  private CredentialsManager globalOAuth2Credentials(
      DefaultConnectionSettings<?> connectionSettings) {
    CredentialsManager result = this.globalOAuth2CredentialsManager;
    if (result != null) {
      return result;
    }

    this.oauth2CredentialsLock.lock();
    try {
      if (this.globalOAuth2CredentialsManager == null) {
        this.globalOAuth2CredentialsManager = createOAuth2Credentials(connectionSettings);
      }
      return this.globalOAuth2CredentialsManager;
    } finally {
      this.oauth2CredentialsLock.unlock();
    }
  }

  private CredentialsManager createOAuth2Credentials(
      DefaultConnectionSettings<?> connectionSettings) {
    DefaultConnectionSettings.DefaultOAuth2Settings<?> settings = connectionSettings.oauth2();
    Consumer<HttpURLConnection> connectionConfigurator;
    if (settings.tlsEnabled()) {
      SSLContext sslContext = settings.tls().sslContext();
      connectionConfigurator =
          c -> {
            if (c instanceof HttpsURLConnection) {
              ((HttpsURLConnection) c).setSSLSocketFactory(sslContext.getSocketFactory());
            }
          };
    } else {
      connectionConfigurator = c -> {};
    }
    HttpTokenRequester tokenRequester =
        new HttpTokenRequester(
            settings.tokenEndpointUri(),
            settings.clientId(),
            settings.clientSecret(),
            settings.grantType(),
            settings.parameters(),
            connectionConfigurator,
            null,
            new GsonTokenParser());
    return new TokenCredentialsManager(
        tokenRequester, environment.scheduledExecutorService(), settings.refreshDelayStrategy());
  }
}
