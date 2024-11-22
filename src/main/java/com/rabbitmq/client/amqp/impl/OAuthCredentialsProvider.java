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

import com.rabbitmq.client.amqp.UsernamePasswordCredentialsProvider;
import com.rabbitmq.client.amqp.oauth.Token;
import com.rabbitmq.client.amqp.oauth.TokenRequester;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class OAuthCredentialsProvider implements UsernamePasswordCredentialsProvider {

  private final TokenRequester requester;
  private volatile Token token;
  private final Lock lock = new ReentrantLock();

  OAuthCredentialsProvider(TokenRequester requester) {
    this.requester = requester;
  }

  @Override
  public String getUsername() {
    return "";
  }

  @Override
  public String getPassword() {
    lock.lock();
    try {
      if (token == null || token.expirationTime() < System.currentTimeMillis()) {
        this.token = this.requester.request();
      }
    } finally {
      lock.unlock();
    }
    return token.value();
  }
}
