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
package com.rabbitmq.client.amqp;

import java.time.Duration;
import javax.net.ssl.SSLContext;

public interface ConnectionSettings<T> {

  String SASL_MECHANISM_PLAIN = "PLAIN";
  String SASL_MECHANISM_EXTERNAL = "EXTERNAL";

  T uri(String uri);

  T uris(String... uris);

  T username(String username);

  T password(String password);

  T host(String host);

  T port(int port);

  T virtualHost(String virtualHost);

  T credentialsProvider(CredentialsProvider credentialsProvider);

  T idleTimeout(Duration idleTimeout);

  T addressSelector(AddressSelector selector);

  T saslMechanism(String mechanism);

  TlsSettings<? extends T> tls();

  Affinity<? extends T> affinity();

  interface TlsSettings<T> {

    TlsSettings<T> hostnameVerification();

    TlsSettings<T> hostnameVerification(boolean hostnameVerification);

    TlsSettings<T> sslContext(SSLContext sslContext);

    TlsSettings<T> trustEverything();

    T connection();
  }

  interface Affinity<T> {

    Affinity<T> queue(String queue);

    Affinity<T> operation(Operation operation);

    Affinity<T> reuse(boolean reuse);

    T connection();

    enum Operation {
      PUBLISH,
      CONSUME
    }
  }
}
