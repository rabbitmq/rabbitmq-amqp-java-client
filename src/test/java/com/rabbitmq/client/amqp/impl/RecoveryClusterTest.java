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
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.impl.Assertions.assertThat;
import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static java.lang.String.format;
import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.client.amqp.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DisabledIfNotCluster
public class RecoveryClusterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryClusterTest.class);

  static final Duration TIMEOUT = Duration.ofSeconds(20);
  static final String[] URIS =
      new String[] {"amqp://localhost:5672", "amqp://localhost:5673", "amqp://localhost:5674"};
  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = BackOffDelayPolicy.fixed(ofSeconds(3));
  static List<String> nodes;
  Environment environment;
  AmqpConnection connection;
  Management management;
  TestInfo testInfo;

  @BeforeAll
  static void initAll() {
    nodes = Cli.nodes();
    LOGGER.info("Available processor(s): {}", Runtime.getRuntime().availableProcessors());
  }

  @BeforeEach
  void init(TestInfo info) {
    environment =
        new AmqpEnvironmentBuilder().connectionSettings().uris(URIS).environmentBuilder().build();
    this.connection = connection(b -> b.name("c-management").recovery().connectionBuilder());
    this.management = connection.management();
    this.testInfo = info;
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  private static class QueueConfiguration {

    private final String name;
    private final Management.QueueType type;
    private final boolean exclusive;
    private final UnaryOperator<Management.QueueSpecification> configurationCallback;

    private QueueConfiguration(
        String name,
        Management.QueueType type,
        boolean exclusive,
        UnaryOperator<Management.QueueSpecification> configurationCallback) {
      this.name = name;
      this.type = type;
      this.exclusive = exclusive;
      this.configurationCallback = configurationCallback;
    }
  }

  @Test
  void clusterRestart() {
    int queueCount = 10;
    List<Management.QueueType> queueTypes =
        List.of(
            Management.QueueType.STREAM,
            Management.QueueType.QUORUM,
            Management.QueueType.CLASSIC,
            Management.QueueType.CLASSIC);
    List<QueueConfiguration> queueConfigurations = queueConfigurations(queueTypes, queueCount);
    List<String> queueNames = queueConfigurations.stream().map(c -> c.name).collect(toList());
    List<PublisherState> publisherStates = Collections.emptyList();
    List<ConsumerState> consumerStates = Collections.emptyList();
    try {
      queueConfigurations.stream()
          .filter(c -> !c.exclusive)
          .forEach(c -> c.configurationCallback.apply(management.queue(c.name)).declare());
      AtomicInteger counter = new AtomicInteger(0);
      consumerStates =
          queueConfigurations.stream()
              .map(
                  conf -> {
                    AmqpConnection c;
                    String cName = "consumer-" + counter.getAndIncrement();
                    if (conf.exclusive) {
                      c = connection(b -> b.name(cName));
                      conf.configurationCallback.apply(c.management().queue(conf.name)).declare();
                      c.management()
                          .binding()
                          .sourceExchange("amq.direct")
                          .key(conf.name)
                          .destinationQueue(conf.name)
                          .bind();
                    } else {
                      boolean isolate = conf.type == Management.QueueType.STREAM;
                      c =
                          connection(
                              b ->
                                  b.name(cName)
                                      .isolateResources(isolate)
                                      .affinity()
                                      .queue(conf.name)
                                      .operation(CONSUME)
                                      .connection());
                    }
                    return new ConsumerState(conf.name, c);
                  })
              .collect(toList());

      counter.set(0);
      publisherStates =
          queueConfigurations.stream()
              .map(
                  c ->
                      new PublisherState(
                          c.name,
                          c.exclusive,
                          connection(
                              b ->
                                  b.name("publisher-" + counter.getAndIncrement())
                                      .affinity()
                                      .queue(c.name)
                                      .operation(PUBLISH)
                                      .connection())))
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

      waitAtMost(
          TIMEOUT,
          () -> connection.state() == OPEN,
          () -> format("Test connection state is %s, expecting %s", connection.state(), OPEN));
      LOGGER.info("Test connection has recovered");

      queueNames.forEach(
          n -> {
            LOGGER.info("Getting info for queue {}", n);
            waitAtMostNoException(TIMEOUT, () -> management.queueInfo(n));
          });
      LOGGER.info("Retrieved info for each queue.");

      queueConfigurations.forEach(
          c -> {
            if (c.type == Management.QueueType.QUORUM || c.type == Management.QueueType.STREAM) {
              assertThat(management.queueInfo(c.name).members())
                  .hasSameSizeAs(nodes)
                  .containsExactlyInAnyOrderElementsOf(nodes);
            } else {
              assertThat(management.queueInfo(c.name).members())
                  .hasSize(1)
                  .containsAnyElementsOf(nodes);
            }
          });

      LOGGER.info("Checked replica info for each queue.");

      syncs = publisherStates.stream().map(s -> s.waitForNewMessages(10)).collect(toList());
      syncs.forEach(
          s -> {
            LOGGER.info("Publishing messages ('{}')", s);
            assertThat(s).completes();
            LOGGER.info("Messages published and settled ('{}')", s);
          });
      LOGGER.info("Checked publishers have recovered.");

      syncs = consumerStates.stream().map(s -> s.waitForNewMessages(10)).collect(toList());
      syncs.forEach(
          s -> {
            LOGGER.info("Waiting for new messages ('{}')", s);
            assertThat(s).completes(Duration.ofSeconds(20));
            LOGGER.info("Expected messages received ('{}')", s);
          });
      LOGGER.info("Checked consumers have recovered.");

      assertThat(publisherStates).allMatch(s -> s.state() == OPEN);
      assertThat(consumerStates).allMatch(s -> s.state() == OPEN);

      System.out.println("Queues:");
      queueNames.forEach(
          q -> {
            Management.QueueInfo queueInfo = management.queueInfo(q);
            System.out.printf(
                "Queue '%s': leader '%s', followers '%s'%n",
                q,
                queueInfo.leader(),
                queueInfo.members().stream()
                    .filter(n -> !n.equals(queueInfo.leader()))
                    .collect(toList()));
          });

      System.out.println("Publishers:");
      publisherStates.forEach(
          p -> System.out.printf("  queue %s, is on leader? %s%n", p.queue, p.isOnLeader()));

      System.out.println("Consumers:");
      consumerStates.forEach(
          p -> System.out.printf("  queue %s, is on member? %s%n", p.queue, p.isOnMember()));
    } catch (Throwable e) {
      LOGGER.info("Test failed with {}", e.getMessage(), e);
      BiConsumer<AmqpConnection, ResourceBase> log =
          (c, r) -> {
            LOGGER.info("Connection {}: {}", c.name(), c.state());
            if (r != null) {
              LOGGER.info("Resource: {}", r.state());
            }
          };
      log.accept(this.connection, null);
      publisherStates.forEach(s -> log.accept(s.connection, s.publisher));
      consumerStates.forEach(s -> log.accept(s.connection, s.consumer));
      throw e;
    } finally {
      publisherStates.forEach(PublisherState::close);
      consumerStates.forEach(ConsumerState::close);
      queueConfigurations.stream()
          .filter(c -> !c.exclusive)
          .forEach(c -> management.queueDeletion().delete(c.name));
    }
  }

  @NotNull
  private List<QueueConfiguration> queueConfigurations(
      List<Management.QueueType> queueTypes, int queueCount) {
    AtomicInteger classicQueueCount = new AtomicInteger(0);
    return queueTypes.stream()
        .flatMap(
            (Function<Management.QueueType, Stream<QueueConfiguration>>)
                type ->
                    IntStream.range(0, queueCount)
                        .mapToObj(
                            ignored -> {
                              boolean exclusive =
                                  type == Management.QueueType.CLASSIC
                                      && classicQueueCount.incrementAndGet() > queueCount;
                              String prefix =
                                  type.name().toLowerCase() + (exclusive ? "-ex-" : "-");
                              String n = name(prefix);
                              UnaryOperator<Management.QueueSpecification> c =
                                  s -> s.type(type).exclusive(exclusive);
                              return new QueueConfiguration(n, type, exclusive, c);
                            }))
        .collect(toList());
  }

  String name(String prefix) {
    return prefix + TestUtils.name(this.testInfo);
  }

  private static class PublisherState implements AutoCloseable {

    private static final ThreadFactory THREAD_FACTORY =
        Utils.threadFactory("cluster-test-publisher-");

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

    private PublisherState(String queue, boolean exclusive, AmqpConnection connection) {
      this.queue = queue;
      this.connection = connection;
      PublisherBuilder builder =
          connection.publisherBuilder().listeners(context -> state.set(context.currentState()));
      builder = exclusive ? builder.exchange("amq.direct").key(queue) : builder.queue(queue);
      this.publisher = (AmqpPublisher) builder.build();
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
          THREAD_FACTORY.newThread(
              () -> {
                while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
                  if (state.get() == OPEN) {
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
      TestUtils.Sync sync =
          TestUtils.sync(
              messageCount, () -> this.postAccepted.set(() -> {}), "Publisher to '%s'", this.queue);
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
      this.stopped.set(true);
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
      TestUtils.Sync sync =
          TestUtils.sync(
              messageCount, () -> this.postHandle.set(() -> {}), "Consumer from '%s'", this.queue);
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
          .members()
          .contains(this.connection.connectionNodename());
    }

    @Override
    public void close() {
      this.consumer.close();
    }
  }

  AmqpConnection connection(java.util.function.Consumer<AmqpConnectionBuilder> operation) {
    AmqpConnectionBuilder builder =
        (AmqpConnectionBuilder)
            environment
                .connectionBuilder()
                .recovery()
                .backOffDelayPolicy(BACK_OFF_DELAY_POLICY)
                .connectionBuilder();
    operation.accept(builder);
    return (AmqpConnection) builder.build();
  }
}
