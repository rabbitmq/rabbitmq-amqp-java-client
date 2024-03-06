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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

class AmqpConnection implements Connection {

  private final AmqpEnvironment environment;
  private volatile AmqpManagement management;
  private final Lock managementLock = new ReentrantLock();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final org.apache.qpid.protonj2.client.Connection nativeConnection;

  AmqpConnection(AmqpEnvironment environment) {
    this.environment = environment;

    Utils.ConnectionParameters connectionParameters = this.environment.connectionParameters();
    ConnectionOptions connectionOptions = new ConnectionOptions();
    connectionOptions.user(connectionParameters.username());
    connectionOptions.password(connectionParameters.password());
    connectionOptions.virtualHost("vhost:" + connectionParameters.virtualHost());
    // only the mechanisms supported in RabbitMQ
    connectionOptions.saslOptions().addAllowedMechanism("PLAIN").addAllowedMechanism("EXTERNAL");

    try {
      this.nativeConnection =
          this.environment
              .client()
              .connect(connectionParameters.host(), connectionParameters.port(), connectionOptions);
      this.nativeConnection.openFuture().get();
    } catch (ClientException | ExecutionException e) {
      throw new ModelException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ModelException(e);
    }
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
    }
  }

  org.apache.qpid.protonj2.client.Connection nativeConnection() {
    return this.nativeConnection;
  }

  ExecutorService executorService() {
    return this.environment.executorService();
  }
}
