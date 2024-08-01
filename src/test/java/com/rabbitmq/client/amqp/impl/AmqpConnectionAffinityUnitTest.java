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

import static com.rabbitmq.client.amqp.Management.QueueType.QUORUM;
import static com.rabbitmq.client.amqp.Management.QueueType.STREAM;
import static com.rabbitmq.client.amqp.impl.AmqpConnection.enforceAffinity;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

import com.rabbitmq.client.amqp.Address;
import com.rabbitmq.client.amqp.ConnectionSettings;
import com.rabbitmq.client.amqp.Management;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.qpid.protonj2.client.impl.ClientConnection;
import org.assertj.core.api.AbstractObjectAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AmqpConnectionAffinityUnitTest {

  private static final String LEADER_NODENAME = "l";
  private static final Address LEADER_ADDRESS = new Address(LEADER_NODENAME, 5672);
  private static final String FOLLOWER1_NODENAME = "f1";
  private static final Address FOLLOWER1_ADDRESS = new Address(FOLLOWER1_NODENAME, 5672);
  private static final String FOLLOWER2_NODENAME = "f2";
  private static final Address FOLLOWER2_ADDRESS = new Address(FOLLOWER2_NODENAME, 5672);
  private static final String Q = "my-queue";
  private static final Map<String, Address> NODES =
      Map.of(
          LEADER_NODENAME, LEADER_ADDRESS,
          FOLLOWER1_NODENAME, FOLLOWER1_ADDRESS,
          FOLLOWER2_NODENAME, FOLLOWER2_ADDRESS);

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
  void infoInCache_ShouldLookUpInfoAndCheckIt_ShouldUseConnectionIfMatch() {
    cache.queueInfo(info());
    when(management.queueInfo(Q)).thenReturn(info());
    when(cf.apply(anyList())).thenReturn(leaderConnection());
    AmqpConnection.NativeConnectionWrapper w = enforceAffinity(cf, management, affinity(), cache);
    assertThat(w).isLeader();
    verify(management, times(1)).queueInfo(Q);
    verify(cf, times(1)).apply(anyList());
    verify(nativeConnection, never()).close();
    assertThat(cache).contains(info()).hasMapping(LEADER_NODENAME, LEADER_ADDRESS);
  }

  @Test
  void infoInCache_ShouldLookUpInfoAndCheckIt_ShouldRetryIfConnectionDoesNotMatch() {
    String initialLeader = LEADER_NODENAME;
    when(management.queueInfo(Q)).thenReturn(info(initialLeader));
    when(cf.apply(anyList())).thenReturn(leaderConnection());
    AmqpConnection.NativeConnectionWrapper w = enforceAffinity(cf, management, affinity(), cache);
    assertThat(w).hasNodename(initialLeader);

    String newLeader = FOLLOWER1_NODENAME;
    // the QQ leader moves to another node for some reason
    // the cache is stale, the management is the authority
    when(management.queueInfo(Q)).thenReturn(info(newLeader));
    when(cf.apply(anyList())).thenReturn(leaderConnection()).thenReturn(follower1Connection());
    // we want the returned connection to be on the new leader
    w = enforceAffinity(cf, management, affinity(), cache);
    assertThat(w).hasNodename(newLeader);
    verify(management, times(2)).queueInfo(Q);
    verify(cf, times(3)).apply(anyList());
    verify(nativeConnection, times(1)).close();
  }

  @Test
  void infoLookupIfNotInCache() {
    when(cf.apply(anyList())).thenReturn(leaderConnection());
    when(management.queueInfo(Q)).thenReturn(info());
    AmqpConnection.NativeConnectionWrapper w = enforceAffinity(cf, management, affinity(), cache);
    assertThat(w).isLeader();
    verify(management, times(1)).queueInfo(Q);
    verify(cf, times(1)).apply(anyList());
    verify(nativeConnection, never()).close();
    assertThat(cache).contains(info()).hasMapping(LEADER_NODENAME, LEADER_ADDRESS);
  }

  @Test
  void infoRefreshedIfInCacheFirstAndFirstAttemptDoesNotMatch() {
    cache.queueInfo(info(STREAM)); // the cache is not correct
    when(cf.apply(anyList())).thenReturn(follower1Connection()).thenReturn(leaderConnection());
    when(management.queueInfo(Q)).thenReturn(info());
    AmqpConnection.NativeConnectionWrapper w = enforceAffinity(cf, management, affinity(), cache);
    assertThat(w).isLeader();
    verify(management, times(1)).queueInfo(Q);
    verify(cf, times(2)).apply(anyList());
    verify(nativeConnection, times(1)).close();
    assertThat(cache)
        .contains(info())
        .hasMapping(FOLLOWER1_NODENAME, FOLLOWER1_ADDRESS)
        .hasMapping(LEADER_NODENAME, LEADER_ADDRESS);
  }

  @Test
  void useAddressReturnedByCache() {
    when(cf.apply(List.of(LEADER_ADDRESS))).thenReturn(leaderConnection());
    when(cf.apply(List.of(FOLLOWER1_ADDRESS))).thenReturn(follower1Connection());
    when(cf.apply(List.of(FOLLOWER2_ADDRESS))).thenReturn(follower2Connection());
    when(cf.apply(null)).thenReturn(follower1Connection());
    when(management.queueInfo(Q)).thenReturn(info());
    cache.nodenameToAddress(LEADER_NODENAME, LEADER_ADDRESS);
    AmqpConnection.NativeConnectionWrapper w = enforceAffinity(cf, management, affinity(), cache);
    assertThat(w).isLeader();
    verify(management, times(1)).queueInfo(Q);
    verify(cf, times(1)).apply(null);
    verify(cf, times(1)).apply(List.of(LEADER_ADDRESS));
    verifyNoMoreInteractions(cf);
    assertThat(cache)
        .contains(info())
        .hasMapping(FOLLOWER1_NODENAME, FOLLOWER1_ADDRESS)
        .hasMapping(LEADER_NODENAME, LEADER_ADDRESS);
  }

  AmqpConnection.NativeConnectionWrapper leaderConnection() {
    return new AmqpConnection.NativeConnectionWrapper(
        this.nativeConnection, LEADER_NODENAME, LEADER_ADDRESS);
  }

  AmqpConnection.NativeConnectionWrapper follower1Connection() {
    return new AmqpConnection.NativeConnectionWrapper(
        this.nativeConnection, FOLLOWER1_NODENAME, FOLLOWER1_ADDRESS);
  }

  AmqpConnection.NativeConnectionWrapper follower2Connection() {
    return new AmqpConnection.NativeConnectionWrapper(
        this.nativeConnection, FOLLOWER2_NODENAME, FOLLOWER2_ADDRESS);
  }

  AmqpConnection.NativeConnectionWrapper connection(String nodename) {
    return new AmqpConnection.NativeConnectionWrapper(
        this.nativeConnection, nodename, NODES.get(nodename));
  }

  static ConnectionUtils.ConnectionAffinity affinity() {
    return new ConnectionUtils.ConnectionAffinity(Q, ConnectionSettings.Affinity.Operation.PUBLISH);
  }

  static Management.QueueInfo info(String leader) {
    return new TestQueueInfo(
        Q, QUORUM, leader, List.of(LEADER_NODENAME, FOLLOWER1_NODENAME, FOLLOWER2_NODENAME));
  }

  static Management.QueueInfo info() {
    return info(
        Management.QueueType.QUORUM, LEADER_NODENAME, FOLLOWER1_NODENAME, FOLLOWER2_NODENAME);
  }

  static Management.QueueInfo info(Management.QueueType type) {
    return info(type, LEADER_NODENAME, FOLLOWER1_NODENAME, FOLLOWER2_NODENAME);
  }

  static Management.QueueInfo info(Management.QueueType type, String leader, String... followers) {
    List<String> replicas = new ArrayList<>();
    replicas.add(leader);
    if (followers != null && followers.length > 0) {
      replicas.addAll(List.of(followers));
    }
    return new TestQueueInfo(Q, type, leader, replicas);
  }

  static NativeConnectionWrapperAssert assertThat(AmqpConnection.NativeConnectionWrapper wrapper) {
    return new NativeConnectionWrapperAssert(wrapper);
  }

  static AffinityCacheAssert assertThat(ConnectionUtils.AffinityCache cache) {
    return new AffinityCacheAssert(cache);
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

  static class AffinityCacheAssert
      extends AbstractObjectAssert<AffinityCacheAssert, ConnectionUtils.AffinityCache> {

    private AffinityCacheAssert(ConnectionUtils.AffinityCache cache) {
      super(cache, AffinityCacheAssert.class);
    }

    AffinityCacheAssert contains(Management.QueueInfo info) {
      if (info == null) {
        throw new IllegalArgumentException("Expected queue info cannot be null");
      }
      isNotNull();

      Management.QueueInfo actualInfo = actual.queueInfo(info.name());
      if (!info.equals(actualInfo)) {
        fail("Queue info should be '%s' but is '%s'", info, actualInfo);
      }
      return this;
    }

    AffinityCacheAssert hasMapping(String nodename, Address address) {
      if (nodename == null || address == null) {
        throw new IllegalArgumentException("Expected nodename/address mapping cannot be null");
      }
      isNotNull();

      Address actualAddress = actual.nodenameToAddress(nodename);
      if (!address.equals(actualAddress)) {
        fail(
            "Mapping should be '%s => %s' but is '%s => %s'",
            nodename, address, nodename, actualAddress);
      }
      return this;
    }
  }

  private static class TestQueueInfo implements Management.QueueInfo {

    private final String name, leader;
    private final Management.QueueType type;
    private final List<String> replicas;

    private TestQueueInfo(
        String name, Management.QueueType type, String leader, List<String> replicas) {
      this.name = name;
      this.type = type;
      this.leader = leader;
      this.replicas = replicas;
    }

    @Override
    public String name() {
      return this.name;
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
    public Management.QueueType type() {
      return this.type;
    }

    @Override
    public Map<String, Object> arguments() {
      return Map.of();
    }

    @Override
    public String leader() {
      return this.leader;
    }

    @Override
    public List<String> replicas() {
      return this.replicas;
    }

    @Override
    public long messageCount() {
      return 0;
    }

    @Override
    public int consumerCount() {
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestQueueInfo that = (TestQueueInfo) o;
      return Objects.equals(name, that.name)
          && Objects.equals(leader, that.leader)
          && type == that.type
          && Objects.equals(replicas, that.replicas);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, leader, type, replicas);
    }

    @Override
    public String toString() {
      return "TestQueueInfo{"
          + "name='"
          + name
          + '\''
          + ", type="
          + type
          + ", leader='"
          + leader
          + '\''
          + ", replicas="
          + replicas
          + '}';
    }
  }
}
