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
import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Environment;
import org.apache.qpid.protonj2.client.exceptions.ClientSessionRemotelyClosedException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(AmqpTestInfrastructureExtension.class)
public class AuthorizationTest {

  private static final String VH = "test_amqp";
  private static final String USERNAME = "amqp";
  private static final String PASSWORD = "amqp";

  Environment environment;
  String name;

  @BeforeAll
  static void initAll() throws Exception {
    addVhost(VH);
    addUser(USERNAME, PASSWORD);
    setPermissions(USERNAME, VH, "^amqp.*$");
    setPermissions("guest", VH, ".*");
  }

  @BeforeEach
  void init(TestInfo info) {
    this.name = TestUtils.name(info);
  }

  @AfterAll
  static void tearDownAll() {
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
  void connectionWithNoVirtualHostAccessShouldThrowException() {
    assertThatThrownBy(
            () -> environment.connectionBuilder().username(USERNAME).password(PASSWORD).build())
        .isInstanceOf(AmqpException.AmqpSecurityException.class);
  }

  @Test
  void entityCreationAttemptWithoutAuthorizationShouldThrowException() throws Exception {
    try (Connection c = userConnection()) {
      String authorizedName = "amqp" + name;
      c.management().queue(authorizedName).exclusive(true).declare();
      assertThatThrownBy(() -> c.management().queue(name).exclusive(true).declare())
          .isInstanceOf(AmqpException.AmqpSecurityException.class)
          .hasMessageContaining("access")
          .hasMessageContaining(name)
          .hasCauseInstanceOf(ClientSessionRemotelyClosedException.class);
      // management should recover
      waitAtMost(
          () -> {
            try {
              c.management().queueDeletion().delete(authorizedName);
              return true;
            } catch (AmqpException e) {
              return false;
            }
          });
    }
  }

  @Test
  void publishingToUnauthorizedExchangeShouldFail() {
    try (Connection uc = userConnection();
        Connection gc = guestConnection()) {
      try {
        gc.management().exchange(this.name).declare();
        assertThatThrownBy(() -> uc.publisherBuilder().exchange(this.name).build())
            .isInstanceOf(AmqpException.AmqpSecurityException.class)
            .hasMessageContaining("access")
            .hasMessageContaining(this.name);
      } finally {
        gc.management().exchangeDeletion().delete(this.name);
      }
    }
  }

  Connection userConnection() {
    return this.environment
        .connectionBuilder()
        .virtualHost(VH)
        .username(USERNAME)
        .password(PASSWORD)
        .build();
  }

  Connection guestConnection() {
    return this.environment.connectionBuilder().virtualHost(VH).build();
  }
}
