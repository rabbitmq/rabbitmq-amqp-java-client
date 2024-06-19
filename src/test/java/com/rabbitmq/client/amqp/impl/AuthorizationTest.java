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

import static com.rabbitmq.client.amqp.impl.Cli.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Environment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(AmqpTestInfrastructureExtension.class)
public class AuthorizationTest {

  private static final String VH = "test_amqp";
  private static final String USERNAME = "amqp";
  private static final String PASSWORD = "amqp";

  Environment environment;

  @BeforeAll
  static void init() throws Exception {
    addVhost(VH);
    addUser(USERNAME, PASSWORD);
    setPermissions(USERNAME, VH, "^amqp.*$");
    setPermissions("guest", VH, ".*");
  }

  @AfterAll
  static void tearDown() {
    deleteUser(USERNAME);
    deleteVhost(VH);
  }

  @Test
  void connectionWithInvalidCredentialsShouldThrowException() {
    assertThatThrownBy(
            () -> environment.connectionBuilder().username("foo").password("bar").build())
        .isInstanceOf(AmqpException.AmqpSecurityException.class);
  }

  @Test
  void connectionWithInvalidPermissionShouldThrowException() {
    assertThatThrownBy(
            () -> environment.connectionBuilder().username(USERNAME).password(PASSWORD).build())
        .isInstanceOf(AmqpException.AmqpSecurityException.class);
  }
}
