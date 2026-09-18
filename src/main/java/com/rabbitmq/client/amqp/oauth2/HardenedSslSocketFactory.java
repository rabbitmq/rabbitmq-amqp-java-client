// Copyright (c) 2026 Broadcom. All Rights Reserved.
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
package com.rabbitmq.client.amqp.oauth2;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * Delegating {@link SSLSocketFactory} that pins the endpoint identification algorithm on every
 * socket it hands back, and optionally restricts cipher suites and named groups (key exchange
 * groups).
 *
 * <p>{@link javax.net.ssl.HttpsURLConnection} exposes no way to reach an {@link SSLParameters} on a
 * per-connection basis, which is why this goes through the socket factory instead.
 */
final class HardenedSslSocketFactory extends SSLSocketFactory {

  private final SSLSocketFactory delegate;
  private final String[] ciphers;
  private final String[] namedGroups;

  HardenedSslSocketFactory(SSLSocketFactory delegate, String[] ciphers, String[] namedGroups) {
    this.delegate = delegate;
    this.ciphers = ciphers;
    this.namedGroups = namedGroups;
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return this.delegate.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return this.delegate.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket() throws IOException {
    return configure(this.delegate.createSocket());
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    return configure(this.delegate.createSocket(host, port));
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
      throws IOException {
    return configure(this.delegate.createSocket(host, port, localHost, localPort));
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    return configure(this.delegate.createSocket(host, port));
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
      throws IOException {
    return configure(this.delegate.createSocket(address, port, localAddress, localPort));
  }

  @Override
  public Socket createSocket(Socket s, String host, int port, boolean autoClose)
      throws IOException {
    return configure(this.delegate.createSocket(s, host, port, autoClose));
  }

  private Socket configure(Socket socket) {
    SSLSocket sslSocket = (SSLSocket) socket;
    SSLParameters parameters = sslSocket.getSSLParameters();
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    if (this.ciphers != null) {
      parameters.setCipherSuites(this.ciphers);
    }
    if (this.namedGroups != null) {
      TlsUtils.setNamedGroups(parameters, this.namedGroups);
    }
    sslSocket.setSSLParameters(parameters);
    return sslSocket;
  }
}
