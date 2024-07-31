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

import static com.rabbitmq.client.amqp.impl.AmqpConnection.enforceAffinity;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

import com.rabbitmq.client.amqp.Address;
import com.rabbitmq.client.amqp.Management;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.qpid.protonj2.client.impl.ClientConnection;
import org.assertj.core.api.AbstractObjectAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AmqpConnectionAffinityTest {

  private static final String LEADER_NODENAME = "l";
  private static final Address LEADER_ADDRESS = new Address(LEADER_NODENAME, 5672);
  private static final String FOLLOWER1_NODENAME = "f1";
  private static final Address FOLLOWER1_ADDRESS = new Address(FOLLOWER1_NODENAME, 5672);
  private static final String FOLLOWER2_NODENAME = "f2";
  private static final Address FOLLOWER2_ADDRESS = new Address(FOLLOWER2_NODENAME, 5672);
  private static final String Q = "my-queue";

  AutoCloseable mocks;

  @Mock AmqpManagement management;

  @Mock Function<List<Address>, AmqpConnection.NativeConnectionWrapper> cf;

  @Mock ClientConnection nativeConnection;

  ConnectionUtils.AffinityCache cache;

  @BeforeEach
  void init() {
    mocks = MockitoAnnotations.openMocks(this);
    cache = new ConnectionUtils.AffinityCache();
  }

  @AfterEach
  void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  void noInfoLookupIfAlreadyInCache() {
    cache.queueInfo(info());
    when(cf.apply(anyList())).thenReturn(leaderConnection());
    AmqpConnection.NativeConnectionWrapper w = enforceAffinity(cf, management, affinity(), cache);
    assertThat(w).isLeader();
    verifyNoInteractions(management);
    verify(cf, times(1)).apply(anyList());
  }

  AmqpConnection.NativeConnectionWrapper leaderConnection() {
    return new AmqpConnection.NativeConnectionWrapper(
        this.nativeConnection, LEADER_NODENAME, LEADER_ADDRESS);
  }

  static ConnectionUtils.ConnectionAffinity affinity() {
    return new ConnectionUtils.ConnectionAffinity(Q, null);
  }

  static Management.QueueInfo info() {
    return info(LEADER_NODENAME, FOLLOWER1_NODENAME, FOLLOWER2_NODENAME);
  }

  static Management.QueueInfo info(String leader, String... followers) {
    Management.QueueType type = Management.QueueType.QUORUM;
    List<String> replicas = new ArrayList<>();
    replicas.add(leader);
    if (followers != null && followers.length > 0) {
      replicas.addAll(List.of(followers));
    }
    return new Management.QueueInfo() {

      @Override
      public String name() {
        return Q;
      }

      @Override
      public String leader() {
        return leader;
      }

      @Override
      public List<String> replicas() {
        return replicas;
      }

      @Override
      public Management.QueueType type() {
        return type;
      }

      @Override
      public boolean durable() {
        return false;
      }

      @Override
      public boolean autoDelete() {
        return false;
      }

      @Override
      public boolean exclusive() {
        return false;
      }

      @Override
      public Map<String, Object> arguments() {
        return Map.of();
      }

      @Override
      public long messageCount() {
        return 0;
      }

      @Override
      public int consumerCount() {
        return 0;
      }
    };
  }

  static NativeConnectionWrapperAssert assertThat(AmqpConnection.NativeConnectionWrapper wrapper) {
    return new NativeConnectionWrapperAssert(wrapper);
  }

  static class NativeConnectionWrapperAssert
      extends AbstractObjectAssert<
          NativeConnectionWrapperAssert, AmqpConnection.NativeConnectionWrapper> {

    private NativeConnectionWrapperAssert(AmqpConnection.NativeConnectionWrapper wrapper) {
      super(wrapper, NativeConnectionWrapperAssert.class);
    }

    NativeConnectionWrapperAssert hasNodename(String nodename) {
      isNotNull();
      if (!actual.nodename().equals(nodename)) {
        fail("Nodename should be '%s' but is '%s'", nodename, actual.nodename());
      }
      return this;
    }

    NativeConnectionWrapperAssert hasAddress(Address address) {
      isNotNull();
      if (!actual.address().equals(address)) {
        fail("Address should be '%s' but is '%s'", address, actual.address());
      }
      return this;
    }

    NativeConnectionWrapperAssert isLeader() {
      return this.hasNodename(LEADER_NODENAME).hasAddress(LEADER_ADDRESS);
    }
  }
}
