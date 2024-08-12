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
import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.client.amqp.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DisabledIfNotCluster
public class RecoveryClusterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryClusterTest.class);

  static final Duration TIMEOUT = Duration.ofSeconds(10);
  static final String[] URIS =
      new String[] {"amqp://localhost:5672", "amqp://localhost:5673", "amqp://localhost:5674"};
  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = BackOffDelayPolicy.fixed(ofSeconds(1));
  static List<String> nodes;
  Environment environment;
  AmqpConnection connection;
  Management management;
  TestInfo testInfo;

  @BeforeAll
  static void initAll() {
    nodes = Cli.nodes();
  }

  @BeforeEach
  void init(TestInfo info) {
    environment =
        new AmqpEnvironmentBuilder().connectionSettings().uris(URIS).environmentBuilder().build();
    this.connection =
        (AmqpConnection)
            environment
                .connectionBuilder()
                .recovery()
                .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
                .connectionBuilder()
                .build();
    this.management = connection.management();
    this.testInfo = info;
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  @Test
  void clusterRestart() {
    int qqCount = 10;
    List<String> qqNames =
        IntStream.range(0, qqCount).mapToObj(ignored -> name("qq-")).collect(toList());
    List<PublisherState> publisherStates = Collections.emptyList();
    List<ConsumerState> consumerStates = Collections.emptyList();
    try {
      qqNames.forEach(n -> management.queue(n).type(Management.QueueType.QUORUM).declare());
      consumerStates =
          qqNames.stream()
              .map(
                  n ->
                      new ConsumerState(
                          n,
                          connection(b -> b.affinity().queue(n).operation(CONSUME).connection())))
              .collect(toList());

      publisherStates =
          qqNames.stream()
              .map(
                  n ->
                      new PublisherState(
                          n,
                          connection(b -> b.affinity().queue(n).operation(PUBLISH).connection())))
              .collect(toList());

      publisherStates.forEach(PublisherState::start);

      List<Sync> syncs =
          consumerStates.stream().map(s -> s.waitForNewMessages(10)).collect(toList());
      syncs.forEach(s -> assertThat(s).completes());

      nodes.forEach(
          n -> {
            LOGGER.info("Restarting node {}...", n);
            Cli.restartNode(n);
            LOGGER.info("Restarted node {}.", n);
          });
      LOGGER.info("Rebalancing...");
      Cli.rebalance();
      LOGGER.info("Rebalancing over.");

      waitAtMost(() -> connection.state() == Resource.State.OPEN);
      LOGGER.info("Test connection has recovered");

      qqNames.forEach(
          n -> waitAtMostNoException(TIMEOUT.multipliedBy(2), () -> management.queueInfo(n)));
      LOGGER.info("Retrieved info for each queue.");
      qqNames.forEach(
          n ->
              assertThat(management.queueInfo(n).replicas())
                  .hasSameSizeAs(nodes)
                  .containsExactlyInAnyOrderElementsOf(nodes));
      LOGGER.info("Checked replica info for each queue.");

      syncs = publisherStates.stream().map(s -> s.waitForNewMessages(10)).collect(toList());
      syncs.forEach(s -> assertThat(s).completes());
      LOGGER.info("Check publishers have recovered.");

      syncs = consumerStates.stream().map(s -> s.waitForNewMessages(10)).collect(toList());
      syncs.forEach(s -> assertThat(s).completes());
      LOGGER.info("Check consumers have recovered.");

      assertThat(publisherStates).allMatch(s -> s.state() == Resource.State.OPEN);
      assertThat(consumerStates).allMatch(s -> s.state() == Resource.State.OPEN);

      System.out.println("Queues:");
      qqNames.forEach(
          q -> {
            Management.QueueInfo queueInfo = management.queueInfo(q);
            System.out.printf(
                "Queue '%s': leader '%s', followers '%s'%n",
                q,
                queueInfo.leader(),
                queueInfo.replicas().stream()
                    .filter(n -> !n.equals(queueInfo.leader()))
                    .collect(toList()));
          });

      System.out.println("Publishers:");
      publisherStates.forEach(
          p -> System.out.printf("  queue %s, is on leader? %s%n", p.queue, p.isOnLeader()));

      System.out.println("Consumers:");
      consumerStates.forEach(
          p -> System.out.printf("  queue %s, is on member? %s%n", p.queue, p.isOnMember()));
    } finally {
      publisherStates.forEach(PublisherState::close);
      consumerStates.forEach(ConsumerState::close);
      qqNames.forEach(n -> management.queueDeletion().delete(n));
    }
  }

  String name(String prefix) {
    return prefix + TestUtils.name(this.testInfo);
  }

  private static class PublisherState implements AutoCloseable {

    private static final byte[] BODY = "hello".getBytes(StandardCharsets.UTF_8);

    final String queue;
    final AmqpConnection connection;
    final AmqpPublisher publisher;
    final AtomicInteger acceptedCount = new AtomicInteger();
    final AtomicReference<Resource.State> state = new AtomicReference<>();
    final AtomicBoolean stopped = new AtomicBoolean(false);
    volatile Thread task;
    final RateLimiter limiter = RateLimiter.create(10);
    final AtomicReference<Runnable> postAccepted = new AtomicReference<>(() -> {});

    private PublisherState(String queue, AmqpConnection connection) {
      this.queue = queue;
      this.connection = connection;
      this.publisher =
          (AmqpPublisher)
              connection
                  .publisherBuilder()
                  .queue(queue)
                  .listeners(context -> state.set(context.currentState()))
                  .build();
    }

    void start() {
      Publisher.Callback callback =
          ctx -> {
            if (ctx.status() == Publisher.Status.ACCEPTED) {
              acceptedCount.incrementAndGet();
              postAccepted.get().run();
            }
          };
      this.task =
          Utils.defaultThreadFactory()
              .newThread(
                  () -> {
                    while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
                      if (state.get() == Resource.State.OPEN) {
                        try {
                          this.limiter.acquire(1);
                          this.publisher.publish(publisher.message(BODY), callback);
                        } catch (Exception e) {

                        }
                      }
                    }
                  });
      this.task.start();
    }

    Sync waitForNewMessages(int messageCount) {
      TestUtils.Sync sync = TestUtils.sync(messageCount, () -> this.postAccepted.set(() -> {}));
      this.postAccepted.set(sync::down);
      return sync;
    }

    Resource.State state() {
      return this.publisher.state();
    }

    boolean isOnLeader() {
      return this.connection
          .management()
          .queueInfo(this.queue)
          .leader()
          .equals(this.connection.connectionNodename());
    }

    @Override
    public void close() {
      this.task.interrupt();
      this.publisher.close();
    }
  }

  private static class ConsumerState implements AutoCloseable {

    final String queue;
    final AmqpConsumer consumer;
    final AtomicInteger receivedCount = new AtomicInteger();
    final AtomicReference<Runnable> postHandle = new AtomicReference<>(() -> {});
    final AmqpConnection connection;

    private ConsumerState(String queue, AmqpConnection connection) {
      this.queue = queue;
      this.connection = connection;
      this.consumer =
          (AmqpConsumer)
              connection
                  .consumerBuilder()
                  .queue(queue)
                  .messageHandler(
                      (ctx, msg) -> {
                        receivedCount.incrementAndGet();
                        postHandle.get().run();
                        try {
                          ctx.accept();
                        } catch (Exception e) {

                        }
                      })
                  .build();
    }

    TestUtils.Sync waitForNewMessages(int messageCount) {
      TestUtils.Sync sync = TestUtils.sync(messageCount, () -> this.postHandle.set(() -> {}));
      this.postHandle.set(sync::down);
      return sync;
    }

    Resource.State state() {
      return this.consumer.state();
    }

    boolean isOnMember() {
      return this.connection
          .management()
          .queueInfo(this.queue)
          .replicas()
          .contains(this.connection.connectionNodename());
    }

    @Override
    public void close() {
      this.consumer.close();
    }
  }

  AmqpConnection connection(java.util.function.Consumer<AmqpConnectionBuilder> operation) {
    AmqpConnectionBuilder builder = (AmqpConnectionBuilder) environment.connectionBuilder();
    operation.accept(builder);
    return (AmqpConnection) builder.build();
  }
}
