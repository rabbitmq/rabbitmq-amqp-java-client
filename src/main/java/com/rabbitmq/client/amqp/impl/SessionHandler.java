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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface SessionHandler extends AutoCloseable {

  Session session();

  Session sessionNoCheck();

  void close();

  class ConnectionNativeSessionSessionHandler implements SessionHandler {

    private final AmqpConnection connection;

    ConnectionNativeSessionSessionHandler(AmqpConnection connection) {
      this.connection = connection;
    }

    @Override
    public Session session() {
      return this.connection.nativeSession();
    }

    @Override
    public Session sessionNoCheck() {
      return this.connection.nativeSession(false);
    }

    @Override
    public void close() {}
  }

  class SingleSessionSessionHandler implements SessionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleSessionSessionHandler.class);

    private final Supplier<Connection> connection;
    private final AtomicReference<Session> session = new AtomicReference<>();

    public SingleSessionSessionHandler(AmqpConnection connection) {
      this.connection = connection::nativeConnection;
    }

    @Override
    public Session session() {
      closeCurrentSession();
      try {
        Session session = this.connection.get().openSession(Utils.sessionOptions());
        this.session.set(ExceptionUtils.wrapGet(session.openFuture()));
        return this.session.get();
      } catch (ClientException e) {
        this.session.set(null);
        throw ExceptionUtils.convert(e);
      }
    }

    @Override
    public Session sessionNoCheck() {
      return this.session();
    }

    @Override
    public void close() {
      closeCurrentSession();
    }

    private void closeCurrentSession() {
      Session previousSession = session.getAndSet(null);
      if (previousSession != null) {
        try {
          previousSession.close();
        } catch (RuntimeException e) {
          LOGGER.debug("Error while closing session: {}", e.getMessage());
        }
      }
    }
  }
}
