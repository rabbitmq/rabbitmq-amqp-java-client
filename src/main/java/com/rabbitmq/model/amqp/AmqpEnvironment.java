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

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.model.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.ClientOptions;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

class AmqpEnvironment implements Environment {

  private final ConnectionParameters connectionParameters;

  private final Client client;
  private final Connection connection;
  private final com.rabbitmq.client.Connection amqplConnection;
  private final Lock managementLock = new ReentrantLock();
  private volatile AmqpManagement management;
  private final ExecutorService executorService;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  AmqpEnvironment(String uri, ExecutorService executorService) {
    this.connectionParameters = connectionParameters(uri);
    ClientOptions clientOptions = new ClientOptions();
    this.client = Client.create(clientOptions);
    this.executorService = executorService;

    ConnectionOptions connectionOptions = new ConnectionOptions();
    connectionOptions.user(this.connectionParameters.username);
    connectionOptions.password(this.connectionParameters.password);
    connectionOptions.virtualHost("vhost:" + this.connectionParameters.virtualHost);
    // only the mechanisms supported in RabbitMQ
    connectionOptions.saslOptions().addAllowedMechanism("PLAIN").addAllowedMechanism("EXTERNAL");

    try {
      this.connection =
          client.connect(
              this.connectionParameters.host, this.connectionParameters.port, connectionOptions);
      this.connection.openFuture().get();

      ConnectionFactory cf = new ConnectionFactory();
      cf.setUri(uri);
      this.amqplConnection = cf.newConnection();

    } catch (ClientException
        | ExecutionException
        | URISyntaxException
        | NoSuchAlgorithmException
        | KeyManagementException
        | IOException
        | TimeoutException e) {
      throw new ModelException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ModelException(e);
    }
  }

  private ConnectionParameters connectionParameters(String uriString) {
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

    return new ConnectionParameters(username, password, host, virtualHost, port);
  }

  @Override
  public Management management() {
    try {
      this.managementLock.lock();
      if (this.management == null || !this.management.isOpen()) {
        this.management = new AmqpManagement(this);
      }
    } finally {
      this.managementLock.unlock();
    }
    return this.management;
  }

  @Override
  public PublisherBuilder publisherBuilder() {
    return new AmqpPublisherBuilder(this);
  }

  @Override
  public ConsumerBuilder consumerBuilder() {
    return new AmqpConsumerBuilder(this);
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      try {
        this.managementLock.lock();
        if (this.management != null) {
          this.management.close();
        }
      } finally {
        this.managementLock.unlock();
      }
      try {
        this.amqplConnection.close();
      } catch (IOException e) {
        throw new ModelException(e);
      }
      this.client.close();
    }
  }

  Connection connection() {
    return this.connection;
  }

  com.rabbitmq.client.Connection amqplConnection() {
    return this.amqplConnection;
  }

  private static class ConnectionParameters {

    private final String username, password, host, virtualHost;
    private final int port;

    private ConnectionParameters(
        String username, String password, String host, String virtualHost, int port) {
      this.username = username;
      this.password = password;
      this.host = host;
      this.virtualHost = virtualHost;
      this.port = port;
    }

    @Override
    public String toString() {
      return "ConnectionParameters{"
          + "username='"
          + username
          + '\''
          + ", password='********'"
          + ", host='"
          + host
          + '\''
          + ", virtualHost='"
          + virtualHost
          + '\''
          + ", port="
          + port
          + '}';
    }
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
}
