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
package com.rabbitmq.model.amqp;

import com.rabbitmq.model.*;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.qpid.protonj2.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpEnvironment implements Environment {

  private final Logger LOGGER = LoggerFactory.getLogger(AmqpEnvironment.class);

  private final Utils.ConnectionParameters connectionParameters;

  private final Client client;
  private final ExecutorService executorService;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final boolean internalExecutor;
  private final List<AmqpConnection> connections = Collections.synchronizedList(new ArrayList<>());

  AmqpEnvironment(String uri, ExecutorService executorService) {
    this.connectionParameters = connectionParameters(uri);
    ClientOptions clientOptions = new ClientOptions();
    this.client = Client.create(clientOptions);

    if (executorService == null) {
      this.executorService = Utils.executorService();
      this.internalExecutor = true;
    } else {
      this.executorService = executorService;
      this.internalExecutor = false;
    }
  }

  Utils.ConnectionParameters connectionParameters() {
    return this.connectionParameters;
  }

  Client client() {
    return this.client;
  }

  private Utils.ConnectionParameters connectionParameters(String uriString) {
    String username = "guest";
    String password = "guest";
    String host = "localhost";
    String virtualHost = "/";
    int port = 5672;
    URI uri = URI.create(uriString);
    if (uri.getHost() != null) {
      host = uri.getHost();
    }

    if (uri.getPort() != -1) {
      port = uri.getPort();
    }

    String userInfo = uri.getRawUserInfo();
    if (userInfo != null) {
      String[] userPassword = userInfo.split(":");
      if (userPassword.length > 2) {
        throw new IllegalArgumentException("Bad user info in URI " + userInfo);
      }

      username = uriDecode(userPassword[0]);
      if (userPassword.length == 2) {
        password = uriDecode(userPassword[1]);
      }
    }

    String path = uri.getRawPath();
    if (path != null && !path.isEmpty()) {
      if (path.indexOf('/', 1) != -1) {
        throw new IllegalArgumentException("Multiple segments in path of URI: " + path);
      }
      virtualHost = uriDecode(uri.getPath().substring(1));
    }

    return new Utils.ConnectionParameters(username, password, host, virtualHost, port);
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      for (AmqpConnection connection : this.connections) {
        try {
          connection.close();
        } catch (Exception e) {
          LOGGER.warn("Error while closing connection", e);
        }
      }
      this.client.close();
      if (this.internalExecutor) {
        this.executorService.shutdownNow();
      }
    }
  }

  @Override
  public ConnectionBuilder connectionBuilder() {
    return new AmqpConnectionBuilder(this);
  }

  private static String uriDecode(String s) {
    try {
      // URLDecode decodes '+' to a space, as for
      // form encoding. So protect plus signs.
      return URLDecoder.decode(s.replace("+", "%2B"), "US-ASCII");
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  ExecutorService executorService() {
    return this.executorService;
  }

  void addConnection(AmqpConnection connection) {
    this.connections.add(connection);
  }
}
