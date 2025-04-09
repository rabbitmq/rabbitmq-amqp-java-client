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

import static com.rabbitmq.client.amqp.ConnectionSettings.Affinity.Operation.CONSUME;
import static com.rabbitmq.client.amqp.ConnectionSettings.Affinity.Operation.PUBLISH;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.ExceptionUtils.noRunningStreamMemberOnNode;
import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.Address;
import com.rabbitmq.client.amqp.AddressSelector;
import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.AmqpException.AmqpResourceClosedException;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.impl.TestUtils.Sync;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@DisabledIfNotCluster
public class ClusterTest {

  static final String[] URIS =
      new String[] {"amqp://localhost:5672", "amqp://localhost:5673", "amqp://localhost:5674"};
  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = BackOffDelayPolicy.fixed(ofMillis(100));
  Environment environment;
  Connection connection;
  Management management;
  String q, name;

  @BeforeEach
  void init(TestInfo info) {
    this.q = name(info);
    this.name = name(info);
    environment =
        TestUtils.environmentBuilder()
            .connectionSettings()
            .addressSelector(new RoundRobinAddressSelector())
            .uris(URIS)
            .environmentBuilder()
            .build();
    this.connection = environment.connectionBuilder().build();
    this.management = connection.management();
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  @EnumSource(names = {"QUORUM", "STREAM"})
  @ParameterizedTest
  void connectionsShouldBeMemberLocalReplicatedQueues(Management.QueueType type) {
    try {
      management.queue(q).type(type).declare();
      AmqpConnection consumeConnection = connection(b -> b.affinity().queue(q).operation(CONSUME));
      AmqpConnection publishConnection = connection(b -> b.affinity().queue(q).operation(PUBLISH));
      Management.QueueInfo info = connection.management().queueInfo(q);
      assertThat(publishConnection.connectionNodename()).isEqualTo(info.leader());
      assertThat(consumeConnection.connectionNodename())
          .isIn(info.members())
          .isNotEqualTo(info.leader());
      assertThat(Cli.listConnections()).hasSize(3);
    } finally {
      management.queueDelete(q);
    }
  }

  @Test
  void connectionShouldRecoverToNewQuorumQueueLeaderAfterItHasMoved() {
    try {
      management.queue(q).type(Management.QueueType.QUORUM).declare();
      Management.QueueInfo info = queueInfo();
      String initialLeader = info.leader();

      Sync recoveredSync = sync();
      AmqpConnection publishConnection =
          connection(
              b ->
                  b.name(name)
                      .listeners(recoveryListener(recoveredSync))
                      .affinity()
                      .queue(q)
                      .operation(PUBLISH));
      assertThat(publishConnection).hasNodename(initialLeader);

      String newLeader = moveQqLeader();

      Cli.closeConnection(name);
      assertThat(recoveredSync).completes();
      assertThat(publishConnection.connectionNodename()).isEqualTo(newLeader);
    } finally {
      management.queueDelete(q);
    }
  }

  @Test
  void publishToMovingQq() {
    try {
      management.queue(q).type(Management.QueueType.QUORUM).declare();

      AmqpConnection publishConnection = connection(b -> b.affinity().queue(q).operation(PUBLISH));
      assertThat(publishConnection).hasNodename(queueInfo().leader());

      Publisher publisher = publishConnection.publisherBuilder().queue(q).build();
      Sync publishSync = sync();
      publisher.publish(publisher.message().messageId(1L), ctx -> publishSync.down());
      assertThat(publishSync).completes();

      String initialLeader = deleteQqLeader();
      publishSync.reset();
      publisher.publish(publisher.message().messageId(2L), ctx -> publishSync.down());
      assertThat(publishSync).completes();

      addQqMember(initialLeader);
      publishSync.reset();
      publisher.publish(publisher.message().messageId(3L), ctx -> publishSync.down());
      assertThat(publishSync).completes();

      int messageCount = 3;
      assertThat(queueInfo()).hasMessageCount(messageCount);
      Sync consumeSync = sync(messageCount);
      Set<Long> messageIds = ConcurrentHashMap.newKeySet(3);
      connection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, msg) -> {
                messageIds.add(msg.messageIdAsLong());
                consumeSync.down();
                ctx.accept();
              })
          .build();
      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L, 2L, 3L);
    } finally {
      management.queueDelete(q);
    }
  }

  @Test
  void consumeFromMovingQq() {
    try {
      management.queue(q).type(Management.QueueType.QUORUM).declare();

      AmqpConnection consumeConnection = connection(b -> b.affinity().queue(q).operation(CONSUME));
      assertThat(consumeConnection).isOnFollower(queueInfo());

      Set<Long> messageIds = ConcurrentHashMap.newKeySet();
      Sync consumeSync = sync();
      consumeConnection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, msg) -> {
                messageIds.add(msg.messageIdAsLong());
                consumeSync.down();
                ctx.accept();
              })
          .build();

      Publisher publisher = connection.publisherBuilder().queue(q).build();
      Sync publishSync = sync();
      publisher.publish(publisher.message().messageId(1L), ctx -> publishSync.down());
      assertThat(publishSync).completes();
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L);
      consumeSync.reset();

      String follower = consumeConnection.connectionNodename();

      deleteQqMember(follower);

      publisher.publish(publisher.message().messageId(2L), ctx -> publishSync.down());
      assertThat(publishSync).completes();
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L, 2L);
      consumeSync.reset();

      addQqMember(follower);

      publisher.publish(publisher.message().messageId(3L), ctx -> publishSync.down());
      assertThat(publishSync).completes();
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L, 2L, 3L);
      consumeSync.reset();
    } finally {
      management.queueDelete(q);
    }
  }

  @Test
  void publishConsumeQuorumQueueWhenLeaderChanges() {
    try {
      management.queue(q).type(Management.QueueType.QUORUM).declare();

      AmqpConnection consumeConnection = connection(b -> b.affinity().queue(q).operation(CONSUME));
      assertThat(consumeConnection).isOnFollower(queueInfo());

      Set<Long> messageIds = ConcurrentHashMap.newKeySet();
      Sync consumeSync = sync();
      consumeConnection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, msg) -> {
                messageIds.add(msg.messageIdAsLong());
                consumeSync.down();
                ctx.accept();
              })
          .build();

      AmqpConnection publishConnection = connection(b -> b.affinity().queue(q).operation(PUBLISH));
      Publisher publisher = publishConnection.publisherBuilder().queue(q).build();
      Sync publishSync = sync();
      publisher.publish(publisher.message().messageId(1L), ctx -> publishSync.down());
      assertThat(publishSync).completes();
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L);
      consumeSync.reset();

      String initialLeader = queueInfo().leader();

      deleteQqMember(initialLeader);
      waitAtMost(() -> !initialLeader.equals(queueInfo().leader()));
      assertThat(queueInfo()).doesNotHaveLeader(initialLeader);

      publisher.publish(publisher.message().messageId(2L), ctx -> publishSync.down());
      assertThat(publishSync).completes();
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L, 2L);
      consumeSync.reset();

      addQqMember(initialLeader);

      publisher.publish(publisher.message().messageId(3L), ctx -> publishSync.down());
      assertThat(publishSync).completes();
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L, 2L, 3L);
      consumeSync.reset();
    } finally {
      management.queueDelete(q);
    }
  }

  @Test
  void consumeFromQuorumQueueWhenLeaderIsPaused() {
    management.queue(q).type(Management.QueueType.QUORUM).declare();
    Management.QueueInfo queueInfo = queueInfo();
    String initialLeader = queueInfo.leader();
    boolean nodePaused = false;
    try {
      AmqpConnection consumeConnection = connection(b -> b.affinity().queue(q).operation(CONSUME));
      assertThat(consumeConnection).isOnFollower(queueInfo());
      Management mgmt = consumeConnection.management();

      Set<Long> messageIds = ConcurrentHashMap.newKeySet();
      Sync consumeSync = sync();
      consumeConnection
          .consumerBuilder()
          .queue(q)
          .messageHandler(
              (ctx, msg) -> {
                messageIds.add(msg.messageIdAsLong());
                consumeSync.down();
                ctx.accept();
              })
          .build();

      AmqpConnection publishConnection = connection(b -> b.affinity().queue(q).operation(CONSUME));
      Publisher publisher = publishConnection.publisherBuilder().queue(q).build();
      assertThat(publishConnection).isOnFollower(queueInfo());

      Sync publishSync = sync();
      publisher.publish(publisher.message().messageId(1L), ctx -> publishSync.down());
      assertThat(publishSync).completes();
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L);
      consumeSync.reset();

      List<String> initialFollowers =
          queueInfo.members().stream().filter(n -> !n.equals(initialLeader)).collect(toList());
      assertThat(initialFollowers).isNotEmpty();

      Cli.pauseNode(initialLeader);
      nodePaused = true;

      publisher.publish(publisher.message().messageId(2L), ctx -> publishSync.down());

      assertThat(publishSync).completes(ofSeconds(20));
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L, 2L);
      consumeSync.reset();

      waitAtMost(() -> initialFollowers.contains(mgmt.queueInfo(q).leader()));
      assertThat(initialFollowers).contains(mgmt.queueInfo(q).leader());

      Cli.unpauseNode(initialLeader);
      nodePaused = false;

      publisher.publish(publisher.message().messageId(3L), ctx -> publishSync.down());
      assertThat(publishSync).completes();
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L, 2L, 3L);

      waitAtMost(() -> initialFollowers.contains(mgmt.queueInfo(q).leader()));
    } finally {
      if (nodePaused) {
        Cli.unpauseNode(initialLeader);
      }
      management.queueDelete(q);
    }
  }

  @Test
  void publishToRestartedStream() {
    try {
      management.queue(q).type(Management.QueueType.STREAM).declare();

      AmqpConnection publishConnection = connection(b -> b.affinity().queue(q).operation(PUBLISH));
      assertThat(publishConnection).isOnLeader(queueInfo());

      Publisher publisher = publishConnection.publisherBuilder().queue(q).build();
      Sync publishSync = sync();
      publisher.publish(publisher.message().messageId(1L), ctx -> publishSync.down());
      assertThat(publishSync).completes();

      restartStream();

      publishSync.reset();
      publisher.publish(publisher.message().messageId(2L), ctx -> publishSync.down());
      assertThat(publishSync).completes();

      int messageCount = 2;
      waitAtMost(() -> queueInfo().messageCount() == messageCount);
      Sync consumeSync = sync(messageCount);
      Set<Long> messageIds = ConcurrentHashMap.newKeySet(messageCount);
      connection.consumerBuilder().queue(q).stream()
          .offset(ConsumerBuilder.StreamOffsetSpecification.FIRST)
          .builder()
          .messageHandler(
              (ctx, msg) -> {
                messageIds.add(msg.messageIdAsLong());
                consumeSync.down();
                ctx.accept();
              })
          .build();
      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L, 2L);
    } finally {
      management.queueDelete(q);
    }
  }

  @Test
  void consumeFromRestartedStream() {
    try {
      management.queue(q).type(Management.QueueType.STREAM).declare();

      AmqpConnection consumeConnection = connection(b -> b.affinity().queue(q).operation(CONSUME));
      assertThat(consumeConnection).isOnFollower(queueInfo());

      Set<Long> messageIds = ConcurrentHashMap.newKeySet();
      Sync consumeSync = sync();
      consumeConnection.consumerBuilder().queue(q).stream()
          .offset(ConsumerBuilder.StreamOffsetSpecification.FIRST)
          .builder()
          .messageHandler(
              (ctx, msg) -> {
                messageIds.add(msg.messageIdAsLong());
                consumeSync.down();
                ctx.accept();
              })
          .build();

      Publisher publisher = connection.publisherBuilder().queue(q).build();
      Sync publishSync = sync();
      publisher.publish(publisher.message().messageId(1L), ctx -> publishSync.down());
      assertThat(publishSync).completes();
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L);
      consumeSync.reset();

      restartStream();

      publisher.publish(publisher.message().messageId(2L), ctx -> publishSync.down());
      assertThat(publishSync).completes();
      publishSync.reset();

      assertThat(consumeSync).completes();
      assertThat(messageIds).containsExactlyInAnyOrder(1L, 2L);
    } finally {
      management.queueDelete(q);
    }
  }

  @Test
  void connectionShouldBeOnOwningNodeWhenAffinityIsActivatedForClassicQueues(TestInfo info) {
    List<String> names = range(0, URIS.length).mapToObj(ignored -> name(info)).collect(toList());
    try {
      List<Connection> connections =
          stream(URIS)
              .map(
                  uri ->
                      connection(
                          b ->
                              b.uri(uri)
                                  .affinity()
                                  .strategy(
                                      ConnectionUtils
                                          .LEADER_FOR_PUBLISHING_MEMBERS_FOR_CONSUMING_STRATEGY)))
              .collect(toList());
      List<Management.QueueInfo> queueInfos =
          range(0, URIS.length)
              .mapToObj(
                  i ->
                      connections
                          .get(i)
                          .management()
                          .queue(names.get(i))
                          .type(Management.QueueType.CLASSIC)
                          .declare())
              .collect(toList());
      assertThat(queueInfos.stream().map(Management.QueueInfo::leader).collect(Collectors.toSet()))
          .hasSameSizeAs(URIS);

      List<AmqpConnection> connectionsWithAffinity =
          names.stream().map(n -> connection(b -> b.affinity().queue(n))).collect(toList());

      range(0, URIS.length)
          .forEach(
              i ->
                  assertThat(connectionsWithAffinity.get(i))
                      .hasNodename(queueInfos.get(i).leader()));

    } finally {
      names.forEach(n -> management.queueDelete(n));
    }
  }

  @Test
  void consumerOnNodeWithoutStreamMemberShouldThrow() {
    List<AmqpConnection> connections = List.of();
    try {
      int memberCount = URIS.length - 1;
      this.management.queue(this.name).stream().initialMemberCount(memberCount).queue().declare();
      waitAtMost(() -> this.management.queueInfo(this.name).members().size() == memberCount);

      List<String> members = this.management.queueInfo(this.name).members();

      connections =
          stream(URIS)
              .map(uri -> (AmqpConnection) this.environment.connectionBuilder().uri(uri).build())
              .collect(toList());

      Connection cWithMember =
          connections.stream()
              .filter(c -> members.contains(c.connectionNodename()))
              .findAny()
              .get();
      cWithMember
          .consumerBuilder()
          .queue(this.name)
          .messageHandler((ctx, msg) -> ctx.accept())
          .build();

      Connection cWithNoMember =
          connections.stream()
              .filter(c -> !members.contains(c.connectionNodename()))
              .findAny()
              .get();

      assertThatThrownBy(
              () ->
                  cWithNoMember
                      .consumerBuilder()
                      .queue(this.name)
                      .messageHandler((ctx, msg) -> ctx.accept())
                      .build())
          .isInstanceOf(AmqpResourceClosedException.class)
          .is(
              new Condition<>(
                  e -> noRunningStreamMemberOnNode((AmqpException) e),
                  "detected as a no-running-stream-member-on-connection-node exception"));

    } finally {
      this.connection.management().queueDelete(this.name);
      connections.forEach(Connection::close);
    }
  }

  String moveQqLeader() {
    String initialLeader = deleteQqLeader();
    addQqMember(initialLeader);
    String newLeader = queueInfo().leader();
    assertThat(newLeader).isNotEqualTo(initialLeader);
    return newLeader;
  }

  String deleteQqLeader() {
    return deleteLeader(this::deleteQqMember);
  }

  String deleteLeader(Consumer<String> deleteMemberOperation) {
    Management.QueueInfo info = queueInfo();
    String initialLeader = info.leader();
    int initialReplicaCount = info.members().size();
    deleteMemberOperation.accept(initialLeader);
    TestUtils.waitAtMost(() -> !initialLeader.equals(queueInfo().leader()));
    assertThat(queueInfo().members()).hasSize(initialReplicaCount - 1);
    return initialLeader;
  }

  void restartStream() {
    Cli.restartStream(this.q);
  }

  void deleteQqMember(String member) {
    Cli.deleteQuorumQueueMember(q, member);
  }

  void deleteStreamMember(String member) {
    Cli.deleteStreamMember(q, member);
  }

  void addQqMember(String newMember) {
    addMember(() -> Cli.addQuorumQueueMember(q, newMember));
  }

  void addStreamMember(String newMember) {
    addMember(() -> Cli.addStreamMember(q, newMember));
  }

  void addMember(Runnable addMemberOperation) {
    Management.QueueInfo info = queueInfo();
    int initialReplicaCount = info.members().size();
    addMemberOperation.run();
    TestUtils.waitAtMost(() -> queueInfo().members().size() == initialReplicaCount + 1);
  }

  Management.QueueInfo queueInfo() {
    return this.management.queueInfo(q);
  }

  AmqpConnection connection(java.util.function.Consumer<AmqpConnectionBuilder> operation) {
    AmqpConnectionBuilder builder = (AmqpConnectionBuilder) environment.connectionBuilder();
    builder
        .recovery()
        .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
        .connectionBuilder()
        .affinity()
        .strategy(ConnectionUtils.LEADER_FOR_PUBLISHING_FOLLOWERS_FOR_CONSUMING_STRATEGY);
    operation.accept(builder);
    return (AmqpConnection) builder.build();
  }

  private static Resource.StateListener recoveryListener(Sync sync) {
    return context -> {
      if (context.previousState() == Resource.State.RECOVERING
          && context.currentState() == Resource.State.OPEN) {
        sync.down();
      }
    };
  }

  private static class RoundRobinAddressSelector implements AddressSelector {

    private final AtomicInteger count = new AtomicInteger();

    @Override
    public Address select(List<Address> addresses) {
      if (addresses.isEmpty()) {
        throw new IllegalStateException("There should at least one node to connect to");
      } else if (addresses.size() == 1) {
        return addresses.get(0);
      } else {
        return addresses.get(count.getAndIncrement() % addresses.size());
      }
    }
  }
}
