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

import static com.rabbitmq.client.amqp.impl.TlsUtils.PROTOCOLS;
import static com.rabbitmq.client.amqp.impl.TlsUtils.TRUST_EVERYTHING_TRUST_MANAGER;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

abstract class TlsTestUtils {

  private TlsTestUtils() {}

  static X509Certificate caCertificate() throws Exception {
    return loadCertificate(caCertificateFile());
  }

  static String caCertificateFile() {
    return tlsArtefactPath(
        System.getProperty("ca.certificate", "/tmp/tls-gen/basic/result/ca_certificate.pem"));
  }

  static X509Certificate clientCertificate() throws Exception {
    return loadCertificate(clientCertificateFile());
  }

  static String clientCertificateFile() {
    return tlsArtefactPath(
        System.getProperty(
            "client.certificate",
            "/tmp/tls-gen/basic/result/client_" + hostname() + "_certificate.pem"));
  }

  static PrivateKey clientKey() throws Exception {
    return loadPrivateKey(clientKeyFile());
  }

  static PrivateKey loadPrivateKey(String filename) throws Exception {
    File file = new File(filename);
    String key = Files.readString(file.toPath(), Charset.defaultCharset());

    String privateKeyPEM =
        key.replace("-----BEGIN PRIVATE KEY-----", "")
            .replaceAll(System.lineSeparator(), "")
            .replace("-----END PRIVATE KEY-----", "");

    byte[] decoded = Base64.getDecoder().decode(privateKeyPEM);

    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decoded);
    PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
    return privateKey;
  }

  static String clientKeyFile() {
    return tlsArtefactPath(
        System.getProperty(
            "client.key", "/tmp/tls-gen/basic/result/client_" + hostname() + "_key.pem"));
  }

  static X509Certificate loadCertificate(String file) throws Exception {
    try (FileInputStream inputStream = new FileInputStream(file)) {
      CertificateFactory fact = CertificateFactory.getInstance("X.509");
      X509Certificate certificate = (X509Certificate) fact.generateCertificate(inputStream);
      return certificate;
    }
  }

  private static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return Cli.hostname();
    }
  }

  private static String tlsArtefactPath(String in) {
    return in.replace("$(hostname)", hostname()).replace("$(hostname -s)", hostname());
  }

  static SSLContext alwaysTrustSslContext() throws Exception {
    SSLContext sslContext = SSLContext.getInstance(PROTOCOLS[0]);
    sslContext.init(null, new TrustManager[] {TRUST_EVERYTHING_TRUST_MANAGER}, null);
    return sslContext;
  }

  static KeyManagerFactory keyManagerFactory(Key key, Certificate certificate)
      throws NoSuchAlgorithmException,
          KeyStoreException,
          CertificateException,
          IOException,
          UnrecoverableKeyException {
    KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, null);
    keyStore.setKeyEntry("some-key", key, null, new Certificate[] {certificate});
    keyManagerFactory.init(keyStore, null);
    return keyManagerFactory;
  }

  static TrustManagerFactory trustManagerFactory(Certificate certificate) throws Exception {
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, null);
    keyStore.setCertificateEntry("some-certificate", certificate);
    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(keyStore);
    return trustManagerFactory;
  }

  static SSLContext sslContext(TrustManagerFactory trustManagerFactory)
      throws NoSuchAlgorithmException, KeyManagementException {
    return sslContext(null, trustManagerFactory);
  }

  static SSLContext sslContext(
      KeyManagerFactory keyManagerFactory, TrustManagerFactory trustManagerFactory)
      throws NoSuchAlgorithmException, KeyManagementException {
    SSLContext sslContext = SSLContext.getInstance(PROTOCOLS[0]);
    sslContext.init(
        keyManagerFactory == null ? null : keyManagerFactory.getKeyManagers(),
        trustManagerFactory == null ? null : trustManagerFactory.getTrustManagers(),
        null);
    return sslContext;
  }
}
