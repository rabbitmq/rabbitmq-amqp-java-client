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
import java.util.List;
import javax.net.ssl.SSLContext;

/**
 * Settings for a connection.
 *
 * @param <T> the type of object returned by methods, usually the object itself
 */
public interface ConnectionSettings<T> {

  /** ANONYMOUS SASL mechanism (the default). */
  String SASL_MECHANISM_ANONYMOUS = "ANONYMOUS";

  /** PLAIN SASL mechanism (username and password). */
  String SASL_MECHANISM_PLAIN = "PLAIN";

  /** EXTERNAL SASL mechanism (e.g. client certificate). */
  String SASL_MECHANISM_EXTERNAL = "EXTERNAL";

  /**
   * The URI of a node to connect to.
   *
   * <p>URI must be of the form <code>amqp://guest:guest@localhost:5672/%2f</code>.
   *
   * @param uri node URI
   * @return type-parameter object
   * @see <a href="https://www.rabbitmq.com/docs/uri-spec">RabbitMQ URI Specification</a>
   */
  T uri(String uri);

  /**
   * A list of URIs of nodes of the same cluster to use to connect to.
   *
   * <p>URIs must be of the form <code>amqp://guest:guest@localhost:5672/%2f</code>.
   *
   * @param uris list of node URIs
   * @return type-parameter object
   * @see <a href="https://www.rabbitmq.com/docs/uri-spec">RabbitMQ URI Specification</a>
   */
  T uris(String... uris);

  /**
   * The username to use.
   *
   * @param username username
   * @return type-parameter object
   */
  T username(String username);

  /**
   * The password to use.
   *
   * @param password password
   * @return type-parameter object
   */
  T password(String password);

  /**
   * The virtual host to connect to.
   *
   * @param virtualHost
   * @return type-parameter object.
   */
  T virtualHost(String virtualHost);

  /**
   * The host to connect to.
   *
   * @param host
   * @return
   */
  T host(String host);

  /**
   * The port to use to connect.
   *
   * @param port
   * @return type-parameter object
   */
  T port(int port);

  /**
   * The {@link CredentialsProvider} to use.
   *
   * @param credentialsProvider credentials provider
   * @return type-parameter object
   */
  T credentialsProvider(CredentialsProvider credentialsProvider);

  /**
   * Idle timeout (heartbeat) to use.
   *
   * <p>Default is 60 seconds.
   *
   * @param idleTimeout
   * @return type-parameter object
   */
  T idleTimeout(Duration idleTimeout);

  /**
   * The {@link AddressSelector} to use.
   *
   * @param selector address selector
   * @return type-parameter object
   */
  T addressSelector(AddressSelector selector);

  /**
   * SASL mechanism to use.
   *
   * @param mechanism SASL mechanism
   * @return type-parameter object
   * @see <a href="https://www.rabbitmq.com/docs/access-control#mechanisms">RabbitMQ Authentication
   *     Mechanisms</a>
   */
  T saslMechanism(String mechanism);

  /**
   * TLS settings.
   *
   * @return TLS settings
   */
  TlsSettings<? extends T> tls();

  /**
   * Connection affinity settings.
   *
   * <p>This is an experimental API, subject to change.
   *
   * @return affinity settings
   */
  Affinity<? extends T> affinity();

  /**
   * TLS settings.
   *
   * @param <T>
   */
  interface TlsSettings<T> {

    /**
     * Activate hostname verification.
     *
     * <p>Activated by default.
     *
     * @return TLS settings
     */
    TlsSettings<T> hostnameVerification();

    /**
     * Whether to activate hostname verification or not.
     *
     * <p>Activated by default.
     *
     * @param hostnameVerification activation flag
     * @return TLS settings
     */
    TlsSettings<T> hostnameVerification(boolean hostnameVerification);

    /**
     * {@link SSLContext} to use.
     *
     * @param sslContext the SSL context to use
     * @return TLS settings
     */
    TlsSettings<T> sslContext(SSLContext sslContext);

    /**
     * Convenience method to set a {@link SSLContext} that trusts all servers.
     *
     * <p>When this feature is enabled, no peer verification is performed, which <strong>provides no
     * protection against Man-in-the-Middle (MITM) attacks</strong>.
     *
     * <p><strong>Use this only in development and QA environments</strong>.
     *
     * @return TLS settings
     */
    TlsSettings<T> trustEverything();

    /**
     * The connection builder.
     *
     * @return connection builder
     */
    T connection();
  }

  /**
   * Connection affinity settings.
   *
   * <p>This is an experimental API, subject to change.
   *
   * @param <T> type of the owning object
   */
  interface Affinity<T> {

    /**
     * The queue to have affinity with.
     *
     * @param queue queue
     * @return affinity settings
     */
    Affinity<T> queue(String queue);

    /**
     * The type of operation to look affinity with.
     *
     * <p>PUBLISH will favor the node with the queue leader on it.
     *
     * <p>CONSUME will favor a node with a queue member (replica) on it.
     *
     * @param operation type of operation
     * @return affinity settings
     */
    Affinity<T> operation(Operation operation);

    /**
     * Whether an open connection with the same affinity should reused.
     *
     * <p>Default is <code>false</code> (open a new connection, do not reuse).
     *
     * @param reuse true to reuse, false to open a new connection
     * @return affinity settings
     */
    Affinity<T> reuse(boolean reuse);

    /**
     * Set the {@link AffinityStrategy}.
     *
     * @param strategy affinity strategy
     * @return affinity settings
     */
    Affinity<T> strategy(AffinityStrategy strategy);

    /**
     * Return the connection builder.
     *
     * @return connection builder
     */
    T connection();

    /** Affinity operation. */
    enum Operation {
      PUBLISH,
      CONSUME
    }
  }

  /** Context for the {@link AffinityStrategy}. */
  interface AffinityContext {

    /**
     * Queue to have affinity with.
     *
     * @return the queue
     */
    String queue();

    /**
     * Operation for affinity.
     *
     * @return operation
     */
    Affinity.Operation operation();
  }

  /** Strategy to pick candidate nodes with an affinity with a queue. */
  @FunctionalInterface
  interface AffinityStrategy {

    /**
     * Pick affinity.
     *
     * @param context the affinity requirements
     * @param info information about the target queue
     * @return the candidate nodes
     */
    List<String> nodesWithAffinity(AffinityContext context, Management.QueueInfo info);
  }
}
