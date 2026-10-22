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
import static org.mockito.Mockito.mock;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConnectionStateClientTest {

  EventExecutorGroup eventExecutorGroup;
  EventLoop loop;
  ConnectionStateClient client;

  @BeforeEach
  void beforeEach() {
    eventExecutorGroup = new DefaultEventExecutorGroup(1);
    loop = new EventLoop(eventExecutorGroup);
    client =
        new ConnectionStateClient(loop, mock(ConnectionStateClient.RecoverableConnection.class));
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
    assertThatThrownBy(
            () ->
                client.runIfCurrent(
                    client.epoch(),
                    () -> {
                      throw failure;
                    }))
        .isSameAs(failure);
  }
}
