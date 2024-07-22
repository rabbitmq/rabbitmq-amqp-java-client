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

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ClientProperties {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientProperties.class);

  // We store the version property in an unusual way because relocating the package can rewrite the
  // key in the property
  // file, which results in spurious warnings being emitted at start-up.
  // see https://github.com/rabbitmq/rabbitmq-java-client/issues/436
  private static final char[] VERSION_PROPERTY =
      new char[] {
        'c', 'o', 'm', '.', 'r', 'a', 'b', 'b', 'i', 't', 'm', 'q', '.', 'c', 'l', 'i', 'e', 'n',
        't', '.', 'a', 'm', 'q', 'p', '.', 'v', 'e', 'r', 's', 'i', 'o', 'n'
      };

  public static final String VERSION = getVersion();

  public static final Map<String, Object> DEFAULT_CLIENT_PROPERTIES =
      Map.of(
          "product", "RabbitMQ",
          "version", ClientProperties.VERSION,
          "platform", "Java",
          "copyright", "Copyright (c) 2024 Broadcom Inc. and/or its subsidiaries.",
          "information",
              "Licensed under the Apache License version 2. See https://www.rabbitmq.com/.");

  private static String getVersion() {
    String version;
    try {
      version = getVersionFromPropertyFile();
    } catch (Exception e1) {
      LOGGER.warn("Couldn't get version from property file", e1);
      try {
        version = getVersionFromPackage();
      } catch (Exception e2) {
        LOGGER.warn("Couldn't get version with Package#getImplementationVersion", e1);
        version = getDefaultVersion();
      }
    }
    return version;
  }

  private static String getVersionFromPropertyFile() throws Exception {
    InputStream inputStream =
        ClientProperties.class
            .getClassLoader()
            .getResourceAsStream("rabbitmq-amqp-1-0-client.properties");
    Properties version = new Properties();
    try {
      version.load(inputStream);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    String propertyName = new String(VERSION_PROPERTY);
    String versionProperty = version.getProperty(propertyName);
    if (versionProperty == null) {
      throw new IllegalStateException("Couldn't find version property in property file");
    }
    return versionProperty;
  }

  private static String getVersionFromPackage() {
    if (ClientProperties.class.getPackage().getImplementationVersion() == null) {
      throw new IllegalStateException("Couldn't get version with Package#getImplementationVersion");
    }
    return ClientProperties.class.getPackage().getImplementationVersion();
  }

  private static String getDefaultVersion() {
    return "0.0.0";
  }

  private ClientProperties() {}
}
