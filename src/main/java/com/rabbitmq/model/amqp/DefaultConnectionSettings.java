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

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.*;

import com.rabbitmq.model.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class DefaultConnectionSettings<T> implements ConnectionSettings<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConnectionSettings.class);

  static final String DEFAULT_USERNAME = "guest";
  static final String DEFAULT_PASSWORD = DEFAULT_USERNAME;
  static final String DEFAULT_HOST = "localhost";
  static final int DEFAULT_PORT = 5672;
  static final int DEFAULT_TLS_PORT = 5671;
  static final String DEFAULT_VIRTUAL_HOST = "/";

  private String host = DEFAULT_HOST;
  private int port = DEFAULT_PORT;
  private CredentialsProvider credentialsProvider =
      new DefaultUsernamePasswordCredentialsProvider(DEFAULT_USERNAME, DEFAULT_PASSWORD);
  private String virtualHost = DEFAULT_VIRTUAL_HOST;
  private List<URI> uris = Collections.emptyList();
  private Duration idleTimeout = Duration.ofMillis(ConnectionOptions.DEFAULT_IDLE_TIMEOUT);
  private static final Random RANDOM = new Random();
  private Function<List<Address>, Address> addressSelector =
      addresses -> {
        if (addresses.isEmpty()) {
          throw new IllegalStateException("There should at least one node to connect to");
        } else if (addresses.size() == 1) {
          return addresses.get(0);
        } else {
          return addresses.get(RANDOM.nextInt(addresses.size()));
        }
      };
  private final List<Address> addresses = new CopyOnWriteArrayList<>();
  private String saslMechanism = ConnectionSettings.SASL_MECHANISM_PLAIN;
  private final DefaultTlsSettings<T> tlsSettings = new DefaultTlsSettings<>(this);

  @Override
  public T uri(String uriString) {
    return this.uris(uriString);
  }

  @Override
  public T uris(String... uris) {
    if (uris == null) {
      throw new IllegalArgumentException("URIs parameter cannot be null");
    }
    this.uris = stream(uris).map(DefaultConnectionSettings::toUri).collect(toUnmodifiableList());
    boolean tls = this.uris.stream().anyMatch(uri -> uri.getScheme().equalsIgnoreCase("amqps"));
    if (tls) {
      this.tlsSettings.enable();
    }
    return toReturn();
  }

  @Override
  public T username(String username) {
    if (this.credentialsProvider instanceof UsernamePasswordCredentialsProvider) {
      this.credentialsProvider =
          new DefaultUsernamePasswordCredentialsProvider(
              username,
              ((UsernamePasswordCredentialsProvider) this.credentialsProvider).getPassword());
    } else {
      this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(username, null);
    }
    return toReturn();
  }

  @Override
  public T password(String password) {
    if (this.credentialsProvider instanceof UsernamePasswordCredentialsProvider) {
      this.credentialsProvider =
          new DefaultUsernamePasswordCredentialsProvider(
              ((UsernamePasswordCredentialsProvider) this.credentialsProvider).getUsername(),
              password);
    } else {
      this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(null, password);
    }
    return toReturn();
  }

  @Override
  public T host(String host) {
    this.host = host;
    return toReturn();
  }

  @Override
  public T port(int port) {
    this.port = port;
    return toReturn();
  }

  @Override
  public T virtualHost(String virtualHost) {
    this.virtualHost = virtualHost;
    return toReturn();
  }

  @Override
  public T credentialsProvider(CredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
    return this.toReturn();
  }

  @Override
  public T idleTimeout(Duration idleTimeout) {
    if (idleTimeout.isNegative()) {
      throw new IllegalArgumentException("Idle timeout cannot be negative");
    }
    this.idleTimeout = idleTimeout;
    return this.toReturn();
  }

  @Override
  public T addressSelector(Function<List<Address>, Address> selector) {
    this.addressSelector = selector;
    return this.toReturn();
  }

  @Override
  public T saslMechanism(String mechanism) {
    if (!SASL_MECHANISM_PLAIN.equals(mechanism) && !SASL_MECHANISM_EXTERNAL.equals(mechanism)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported SASL mechanism: '%s'. " + "Supported mechanisms are '%s' and '%s'.",
              mechanism, SASL_MECHANISM_PLAIN, SASL_MECHANISM_EXTERNAL));
    }
    this.saslMechanism = mechanism;
    return this.toReturn();
  }

  CredentialsProvider credentialsProvider() {
    return credentialsProvider;
  }

  String virtualHost() {
    return virtualHost;
  }

  Duration idleTimeout() {
    return idleTimeout;
  }

  Address selectAddress() {
    return this.addressSelector.apply(this.addresses);
  }

  String saslMechanism() {
    return this.saslMechanism;
  }

  abstract T toReturn();

  boolean tlsEnabled() {
    return this.tlsSettings.enabled();
  }

  DefaultTlsSettings<?> tlsSettings() {
    return this.tlsSettings;
  }

  void copyTo(DefaultConnectionSettings<?> copy) {
    copy.host(this.host);
    copy.port(this.port);
    copy.credentialsProvider(this.credentialsProvider);
    copy.virtualHost(this.virtualHost);
    copy.uris(this.uris.stream().map(URI::toString).toArray(String[]::new));
    copy.addressSelector(this.addressSelector);
    copy.idleTimeout(this.idleTimeout);
    copy.saslMechanism(this.saslMechanism);

    if (this.tlsSettings.enabled()) {
      this.tlsSettings.copyTo((DefaultTlsSettings<?>) copy.tls());
    }
  }

  DefaultConnectionSettings<?> consolidate() {
    if (this.uris.isEmpty()) {
      int p = this.port;
      if (this.tlsEnabled() && this.port == DEFAULT_PORT) {
        p = DEFAULT_TLS_PORT;
      }
      this.addresses.add(new Address(this.host, p));
    } else {
      URI uri = uris.get(0);
      String host = uri.getHost();
      if (host != null) {
        this.host(host);
      }

      int port = uri.getPort();
      if (port != -1) {
        this.port(port);
      }

      String userInfo = uri.getRawUserInfo();
      if (userInfo != null) {
        String[] userPassword = userInfo.split(":");
        if (userPassword.length > 2) {
          throw new IllegalArgumentException("Bad user info in URI " + userInfo);
        }

        this.username(uriDecode(userPassword[0]));
        if (userPassword.length == 2) {
          this.password(uriDecode(userPassword[1]));
        }
      }

      String path = uri.getRawPath();
      if (path != null && !path.isEmpty()) {
        if (path.indexOf('/', 1) != -1) {
          throw new IllegalArgumentException("Multiple segments in path of URI: " + path);
        }
        this.virtualHost(uriDecode(uri.getPath().substring(1)));
      }

      boolean tls =
          this.tlsEnabled()
              || this.uris.stream().anyMatch(u -> u.getScheme().equalsIgnoreCase("amqps"));

      int defaultPort = tls ? DEFAULT_TLS_PORT : DEFAULT_PORT;
      List<Address> addrs =
          this.uris.stream()
              .map(
                  uriItem ->
                      new Address(
                          uriItem.getHost() == null ? DEFAULT_HOST : uriItem.getHost(),
                          uriItem.getPort() == -1 ? defaultPort : uriItem.getPort()))
              .collect(toList());
      this.addresses.clear();
      this.addresses.addAll(addrs);
    }
    return this;
  }

  @Override
  public TlsSettings<T> tls() {
    this.tlsSettings.enable();
    return this.tlsSettings;
  }

  static DefaultConnectionSettings<?> instance() {
    return new DefaultConnectionSettings<>() {
      @Override
      Object toReturn() {
        return null;
      }
    };
  }

  private static URI toUri(String uriString) {
    try {
      URI uri = new URI(uriString);
      if (!"amqp".equalsIgnoreCase(uri.getScheme()) && !"amqps".equalsIgnoreCase(uri.getScheme())) {
        throw new IllegalArgumentException(
            "Wrong scheme in AMQP URI: " + uri.getScheme() + ". Should be amqp or amqps");
      }
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URI: " + uriString, e);
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

  static class DefaultTlsSettings<T> implements TlsSettings<T> {

    private final DefaultConnectionSettings<T> connectionSettings;

    private boolean enabled = false;
    private boolean hostnameVerification = true;
    private SSLContext sslContext;

    private DefaultTlsSettings(DefaultConnectionSettings<T> connectionSettings) {
      this.connectionSettings = connectionSettings;
    }

    @Override
    public TlsSettings<T> hostnameVerification() {
      this.hostnameVerification = true;
      return this;
    }

    @Override
    public TlsSettings<T> hostnameVerification(boolean hostnameVerification) {
      this.hostnameVerification = hostnameVerification;
      return this;
    }

    @Override
    public TlsSettings<T> sslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
    }

    @Override
    public TlsSettings<T> trustEverything() {
      LOGGER.warn(
          "SECURITY ALERT: this feature trusts every server certificate, effectively disabling peer verification. "
              + "This is convenient for local development but offers no protection against man-in-the-middle attacks. "
              + "Please see https://www.rabbitmq.com/ssl.html to learn more about peer certificate verification.");
      SSLContext context = null;
      for (String protocol : TlsUtils.PROTOCOLS) {
        try {
          context = SSLContext.getInstance(protocol);
        } catch (NoSuchAlgorithmException ignored) {
          // OK, trying the next protocol
        }
      }
      if (context == null) {
        throw new IllegalStateException(
            "None of the mandatory TLS protocols supported:"
                + String.join(", ", TlsUtils.PROTOCOLS)
                + ".");
      }
      try {
        context.init(null, new TrustManager[] {TlsUtils.TRUST_EVERYTHING_TRUST_MANAGER}, null);
      } catch (KeyManagementException e) {
        throw new ModelException(e);
      }
      this.sslContext = context;
      return this;
    }

    @Override
    public T connection() {
      return this.connectionSettings.toReturn();
    }

    void copyTo(DefaultTlsSettings<?> copy) {
      copy.enabled = this.enabled;
      copy.sslContext(this.sslContext);
      copy.hostnameVerification(this.hostnameVerification);
    }

    void enable() {
      this.enabled = true;
    }

    boolean enabled() {
      return this.enabled;
    }

    SSLContext sslContext() {
      return this.sslContext;
    }

    boolean isHostnameVerification() {
      return this.hostnameVerification;
    }
  }
}
