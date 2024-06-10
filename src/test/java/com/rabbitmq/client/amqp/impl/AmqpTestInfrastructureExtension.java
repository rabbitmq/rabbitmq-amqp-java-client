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

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Environment;
import java.lang.reflect.Field;
import org.junit.jupiter.api.extension.*;

class AmqpTestInfrastructureExtension
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(AmqpTestInfrastructureExtension.class);

  private static ExtensionContext.Store store(ExtensionContext extensionContext) {
    return extensionContext.getRoot().getStore(NAMESPACE);
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    Environment environment = null;
    Field connectionField = field(context.getTestClass().get(), "connection");
    if (connectionField != null) {
      environment = TestUtils.environmentBuilder().build();
      store(context).put("environment", environment);
    }
    Field environmentField = field(context.getTestClass().get(), "environment");
    if (environmentField != null) {
      environment = environment == null ? TestUtils.environmentBuilder().build() : environment;
      store(context).put("environment", environment);
    }
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    Environment env = store(context).get("environment", Environment.class);
    if (env != null) {
      Field environmentField = field(context.getTestClass().get(), "environment");
      if (environmentField != null) {
        environmentField.setAccessible(true);
        environmentField.set(context.getTestInstance().get(), env);
      }
    }

    Field connectionField = field(context.getTestClass().get(), "connection");
    if (connectionField != null) {
      connectionField.setAccessible(true);
      Connection connection = env.connectionBuilder().build();
      connectionField.set(context.getTestInstance().get(), connection);
      store(context).put("connection", connection);
    }
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    Connection connection = store(context).get("connection", Connection.class);
    if (connection != null) {
      connection.close();
    }
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    Environment env = store(context).get("environment", Environment.class);
    if (env != null) {
      env.close();
    }
  }

  private static Field field(Class<?> cls, String name) {
    Field field = null;
    while (field == null && cls != null) {
      try {
        field = cls.getDeclaredField(name);
      } catch (NoSuchFieldException e) {
        cls = cls.getSuperclass();
      }
    }
    return field;
  }
}
