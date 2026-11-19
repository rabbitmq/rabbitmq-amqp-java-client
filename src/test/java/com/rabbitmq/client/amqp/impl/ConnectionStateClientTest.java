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
package com.rabbitmq.client.amqp.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Resource;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.DisconnectionEvent;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConnectionStateClientTest {

  EventExecutorGroup eventExecutorGroup;
  EventLoop loop;
  ConnectionStateClient.RecoverableConnection connection;
  ConnectionStateClient client;

  @BeforeEach
  void beforeEach() {
    eventExecutorGroup = new DefaultEventExecutorGroup(1);
    loop = new EventLoop(eventExecutorGroup);
    connection = mock(ConnectionStateClient.RecoverableConnection.class);
    when(connection.recoveryActivated()).thenReturn(true);
    client = new ConnectionStateClient(loop, connection);
  }

  @AfterEach
  void afterEach() throws Exception {
    client.close();
    loop.close();
    eventExecutorGroup.shutdownGracefully();
  }

  @Test
  void runIfCurrentShouldRunActionWhenEpochIsCurrent() {
    AtomicBoolean ran = new AtomicBoolean(false);
    client.runIfCurrent(client.epoch(), () -> ran.set(true));
    assertThat(ran).isTrue();
  }

  @Test
  void runIfCurrentShouldNotRunActionWhenEpochIsStale() {
    AtomicBoolean ran = new AtomicBoolean(false);
    long staleEpoch = client.epoch() - 1;
    assertThatThrownBy(() -> client.runIfCurrent(staleEpoch, () -> ran.set(true)))
        .isInstanceOf(StaleRecoveryException.class);
    assertThat(ran).isFalse();
  }

  @Test
  void runIfCurrentShouldNotRunActionWhenConnectionIsClosed() {
    AtomicBoolean ran = new AtomicBoolean(false);
    client.markClosed(null);
    long epoch = client.epoch();
    assertThatThrownBy(() -> client.runIfCurrent(epoch, () -> ran.set(true)))
        .isInstanceOf(StaleRecoveryException.class);
    assertThat(ran).isFalse();
  }

  @Test
  void runIfCurrentShouldPropagateActionFailure() {
    IllegalStateException failure = new IllegalStateException("simulated failure");
    assertThatThrownBy(() ->
            client.runIfCurrent(
                client.epoch(),
                () -> {
                  throw failure;
                }))
        .isSameAs(failure);
  }

  @Test
  void disconnectOfOtherNativeConnectionDuringCreationShouldNotTriggerRecovery() {
    Connection failed = mock(Connection.class);
    Connection opened = mock(Connection.class);
    when(connection.nativeConnection()).thenReturn(opened);
    client.handleDisconnect(client.epoch(), failed, disconnection());
    client.handleInitialOpen(opened);
    // late event of the failed native connection
    client.handleDisconnect(1, failed, disconnection());
    long epoch = client.epoch();

    assertThat(epoch).isEqualTo(1);
    verify(connection).updateState(Resource.State.OPEN, null);
    verify(connection, never()).submitRecoveryTask(any());

    client.handleDisconnect(1, opened, disconnection());
    assertThat(client.epoch()).isEqualTo(2);
    verify(connection).submitRecoveryTask(any());
  }

  @Test
  void disconnectOfOpenedNativeConnectionDuringCreationShouldTriggerRecovery() {
    Connection opened = mock(Connection.class);
    client.handleDisconnect(client.epoch(), opened, disconnection());
    client.handleInitialOpen(opened);
    long epoch = client.epoch();

    assertThat(epoch).isEqualTo(2);
    verify(connection, never()).updateState(Resource.State.OPEN, null);
    verify(connection).updateState(any(), any());
    verify(connection).submitRecoveryTask(any());
  }

  @Test
  void disconnectAfterCreationFailureShouldNotTriggerRecovery() {
    Connection failed = mock(Connection.class);
    client.handleDisconnect(1, failed, disconnection());
    client.markCreationFailed();
    for (long eventEpoch : new long[] {1, 2}) {
      client.handleDisconnect(eventEpoch, failed, disconnection());
      client.handleSessionFailure(
          eventEpoch, new AmqpException.AmqpConnectionException("simulated failure", null));
    }
    client.handleInitialOpen(failed);
    long epoch = client.epoch();

    assertThat(epoch).isEqualTo(2);
    verify(connection, never()).submitRecoveryTask(any());
    verify(connection, never()).close(any());
  }

  @Test
  void creationFailureShouldNotChangeConnectionState() {
    client.markCreationFailed();
    long epoch = client.epoch();

    assertThat(epoch).isEqualTo(2);
    verify(connection, never()).updateState(any(), any());
  }

  @Test
  void markClosedShouldUpdateConnectionState() {
    IllegalStateException cause = new IllegalStateException();
    client.markClosed(cause);
    long epoch = client.epoch();

    assertThat(epoch).isEqualTo(2);
    verify(connection).updateState(Resource.State.CLOSING, cause);
  }

  @Test
  void disconnectShouldCloseConnectionIfRecoveryIsNotActivated() {
    when(connection.recoveryActivated()).thenReturn(false);
    Connection opened = mock(Connection.class);
    when(connection.nativeConnection()).thenReturn(opened);
    client.handleInitialOpen(opened);
    client.handleDisconnect(1, opened, disconnection());
    long epoch = client.epoch();

    assertThat(epoch).isEqualTo(1);
    verify(connection).close(any());
    verify(connection, never()).submitRecoveryTask(any());
  }

  private static DisconnectionEvent disconnection() {
    return new DisconnectionEvent(
        "localhost", 5672, new ClientConnectionRemotelyClosedException("simulated failure"));
  }
}
