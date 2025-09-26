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

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

public final class HttpTestUtils {

  private static final char[] KEY_STORE_PASSWORD = "password".toCharArray();

  private HttpTestUtils() {}

  public static HttpServer startServer(int port, String path, HttpHandler handler) {
    return startServer(port, path, null, handler);
  }

  public static HttpServer startServer(
      int port, String path, KeyStore keyStore, HttpHandler handler) {
    HttpServer server;
    try {
      if (keyStore != null) {
        KeyManagerFactory keyManagerFactory =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEY_STORE_PASSWORD);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        server = HttpsServer.create(new InetSocketAddress(port), 0);
        ((HttpsServer) server).setHttpsConfigurator(new HttpsConfigurator(sslContext));
      } else {
        server = HttpServer.create(new InetSocketAddress(port), 0);
      }
      server.createContext(path, handler);
      server.start();
      return server;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static KeyStore generateKeyPair() {
    try {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, KEY_STORE_PASSWORD);

      KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
      ECGenParameterSpec spec = new ECGenParameterSpec("secp521r1");
      kpg.initialize(spec);

      KeyPair kp = kpg.generateKeyPair();

      JcaX509v3CertificateBuilder certificateBuilder =
          new JcaX509v3CertificateBuilder(
              new X500NameBuilder().addRDN(BCStyle.CN, "localhost").build(),
              BigInteger.valueOf(new SecureRandom().nextInt()),
              Date.from(Instant.now().minus(10, ChronoUnit.DAYS)),
              Date.from(Instant.now().plus(10, ChronoUnit.DAYS)),
              new X500NameBuilder().addRDN(BCStyle.CN, "localhost").build(),
              kp.getPublic());

      X509CertificateHolder certificateHolder =
          certificateBuilder.build(
              new JcaContentSignerBuilder("SHA512withECDSA").build(kp.getPrivate()));

      X509Certificate certificate =
          new JcaX509CertificateConverter().getCertificate(certificateHolder);

      keyStore.setKeyEntry(
          "default", kp.getPrivate(), KEY_STORE_PASSWORD, new Certificate[] {certificate});
      return keyStore;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
