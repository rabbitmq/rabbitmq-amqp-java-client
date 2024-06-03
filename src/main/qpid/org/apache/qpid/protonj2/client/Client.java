/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.client;

import java.util.concurrent.Future;

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientInstance;

/**
 * The Container that hosts AMQP Connections. From this container new connections
 * can be created and an API exists to close all open connections hosted in this
 * container instance.
 */
public interface Client extends AutoCloseable {

    /**
     * @return a new {@link Client} instance configured with defaults.
     */
    static Client create() {
        return ClientInstance.create();
    }

    /**
     * Create a new {@link Client} instance using provided configuration options.
     *
     * @param options
     * 		The configuration options to use when creating the client.
     *
     * @return a new {@link Client} instance configured using the provided options.
     */
    static Client create(ClientOptions options) {
        return ClientInstance.create(options);
    }

    /**
     * @return the container id assigned to this {@link Client} instance.
     */
    String containerId();

    /**
     * Closes all currently open {@link Connection} instances created by this client.
     * <p>
     * This method blocks and waits for each connection to close in turn using the configured
     * close timeout of the {@link ConnectionOptions} that the connection was created with.
     */
    @Override
    void close();

    /**
     * Closes all currently open {@link Connection} instances created by this client.
     * <p>
     * This method does not block and wait for each connection to be closed in turn, instead
     * it returns a future which will be completed once the close of all connections has been
     * completed.
     *
     * @return a {@link Future} that will be completed when all open connections have closed.
     */
    Future<Client> closeAsync();

    /**
     * Connect to the specified host and port, without credentials and with all
     * connection options set to their defaults.
     * <p>
     * The connection returned may still fail afterwards as the majority of connection
     * setup is done asynchronously so the application should be prepared for errors to
     * arise of the connection methods if the open future is not waited on.
     *
     * @param host
     *            the host to connect to
     * @param port
     *            the port to connect to
     *
     * @return connection, establishment not yet completed
     *
     * @throws ClientException if the {@link Client} is closed or an error occurs during connect.
     */
    Connection connect(String host, int port) throws ClientException;

    /**
     * Connect to the specified host and port, with given connection options.
     * <p>
     * The connection returned may still fail afterwards as the majority of connection
     * setup is done asynchronously so the application should be prepared for errors to
     * arise of the connection methods if the open future is not waited on.
     *
     * @param host
     *            the host to connect to
     * @param port
     *            the port to connect to
     * @param options
     *            options to use when creating the connection.
     *
     * @return connection, establishment not yet completed
     *
     * @throws ClientException if the {@link Client} is closed or an error occurs during connect.
     */
    Connection connect(String host, int port, ConnectionOptions options) throws ClientException;

    /**
     * Connect to the specified host, using the default port, without credentials and with all
     * connection options set to their defaults.
     * <p>
     * The connection returned may still fail afterwards as the majority of connection
     * setup is done asynchronously so the application should be prepared for errors to
     * arise of the connection methods if the open future is not waited on.
     *
     * @param host
     *            the host to connect to
     *
     * @return connection, establishment not yet completed
     *
     * @throws ClientException if the {@link Client} is closed or an error occurs during connect.
     */
    Connection connect(String host) throws ClientException;

    /**
     * Connect to the specified host, using the default port, without credentials and with all
     * connection options set to their defaults.
     * <p>
     * The connection returned may still fail afterwards as the majority of connection
     * setup is done asynchronously so the application should be prepared for errors to
     * arise of the connection methods if the open future is not waited on.
     *
     * @param host
     *            the host to connect to
     * @param options
     *            options to use when creating the connection.
     *
     * @return connection, establishment not yet completed
     *
     * @throws ClientException if the {@link Client} is closed or an error occurs during connect.
     */
    Connection connect(String host, ConnectionOptions options) throws ClientException;

}
