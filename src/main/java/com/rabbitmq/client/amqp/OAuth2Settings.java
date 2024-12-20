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

import javax.net.ssl.SSLContext;

/**
 * Configuration to retrieve a token using the <a
 * href="https://tools.ietf.org/html/rfc6749#section-4.4">OAuth 2 Client Credentials flow</a>.
 *
 * <p>OAuth 2 requires RabbitMQ 4.1 or more.
 *
 * @param <T> the type of object returned by methods, usually the object itself
 * @since 0.4.0
 */
public interface OAuth2Settings<T> {

  /**
   * Set the URI to access to get the token.
   *
   * <p>TLS is supported by providing a <code>HTTPS</code> URI and setting a {@link SSLContext}. See
   * {@link #tls()} for more information. <em>Applications in production should always use HTTPS to
   * retrieve tokens.</em>
   *
   * @param uri access URI
   * @return OAuth 2 settings
   * @see #tls()
   * @see TlsSettings#sslContext(SSLContext)
   */
  OAuth2Settings<T> tokenEndpointUri(String uri);

  /**
   * Set the OAuth 2 client ID
   *
   * <p>The client ID usually identifies the application that requests a token.
   *
   * @param clientId client ID
   * @return OAuth 2 settings
   */
  OAuth2Settings<T> clientId(String clientId);

  /**
   * Set the secret (password) to use to get a token.
   *
   * @param clientSecret client secret
   * @return OAuth 2 settings
   */
  OAuth2Settings<T> clientSecret(String clientSecret);

  /**
   * Set the grant type to use when requesting the token.
   *
   * <p>The default is <code>client_credentials</code>, but some OAuth 2 servers can use
   * non-standard grant types to request tokens with extra-information.
   *
   * @param grantType grant type
   * @return OAuth 2 settings
   */
  OAuth2Settings<T> grantType(String grantType);

  /**
   * Set a parameter to pass in the request.
   *
   * <p>The OAuth 2 server may require extra parameters to narrow down the identity of the user.
   *
   * @param name name of the parameter
   * @param value value of the parameter
   * @return OAuth 2 settings
   */
  OAuth2Settings<T> parameter(String name, String value);

  /**
   * Whether to share the same token between connections.
   *
   * <p>Default is <true>true</true> (the token is shared between connections).
   *
   * @param shared flag to share the token between connections
   * @return OAuth 2 settings
   */
  OAuth2Settings<T> shared(boolean shared);

  /**
   * TLS configuration for requesting the token.
   *
   * @return TLS configuration
   */
  TlsSettings<? extends T> tls();

  /**
   * Connection settings.
   *
   * @return connections settings
   */
  T connection();

  /**
   * TLS settings to request the OAuth 2 token.
   *
   * @param <T> the type of object returned by methods, usually the object itself
   */
  interface TlsSettings<T> {

    /**
     * {@link SSLContext} for HTTPS requests.
     *
     * @param sslContext the SSL context
     * @return TLS settings
     */
    TlsSettings<T> sslContext(SSLContext sslContext);

    /**
     * Go back to the general OAuth 2 settings.
     *
     * @return OAuth 2 settings
     */
    OAuth2Settings<T> oauth2();
  }
}
