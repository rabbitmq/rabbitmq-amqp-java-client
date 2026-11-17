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

import com.rabbitmq.client.amqp.Address;
import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.ConnectionSettings;
import com.rabbitmq.client.amqp.Management;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectionUtils {

  static final RetryStrategy NO_RETRY_STRATEGY = Supplier::get;

  static final ConnectionSettings.AffinityStrategy
      LEADER_FOR_PUBLISHING_FOLLOWERS_FOR_CONSUMING_STRATEGY =
          new LeaderForPublishingFollowersForConsumingStrategy();

  static final ConnectionSettings.AffinityStrategy
      LEADER_FOR_PUBLISHING_MEMBERS_FOR_CONSUMING_STRATEGY =
          new LeaderForPublishingMembersForConsumingStrategy();

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionUtils.class);

  private ConnectionUtils() {}

  /**
   * Create a connection on a node with affinity with the queue of the context.
   *
   * <p>Affinity is best effort: the last connection is returned if no node with affinity is found
   * after a few attempts, or if the queue info cannot be looked up (e.g. the queue does not exist).
   * An {@link AmqpException.AmqpConnectionException} is thrown if the nodes with affinity cannot be
   * reached, so that the caller can try again later. The management resources are released and the
   * management is marked unavailable when an {@link AmqpException.AmqpConnectionException} is
   * thrown.
   */
  static AmqpConnection.NativeConnectionWrapper enforceAffinity(
      Function<List<Address>, AmqpConnection.NativeConnectionWrapper> connectionFactory,
      List<Address> addresses,
      AmqpManagement management,
      AffinityContext context,
      AffinityCache affinityCache,
      ConnectionSettings.AffinityStrategy strategy,
      RetryStrategy retryStrategy,
      String connectionName) {
    if (context == null) {
      // no affinity asked, we create a connection and return it
      return retryStrategy.maybeRetry(() -> connectionFactory.apply(null));
    }
    AmqpConnection.NativeConnectionWrapper connectionWrapper = null;
    try {
      Management.QueueInfo info = affinityCache.queueInfo(context.queue());
      // null when the queue info is not in the cache: no hint, any node
      List<String> nodesWithAffinity =
          info == null ? null : strategy.nodesWithAffinity(context, info);
      for (int attempt = 1; ; attempt++) {
        connectionWrapper = null;
        CandidateConnection candidate =
            connect(
                connectionFactory,
                addresses,
                addressHints(nodesWithAffinity, affinityCache),
                retryStrategy,
                context,
                connectionName);
        connectionWrapper = candidate.connection;
        String nodename = connectionWrapper.nodename();
        affinityCache.nodenameToAddress(nodename, connectionWrapper.address());
        if (attempt == 1) {
          // the cached queue info may be stale, e.g. the leader moved
          info =
              refreshQueueInfo(
                  management, context, affinityCache, retryStrategy, candidate.hintedFailure);
          if (info == null) {
            LOGGER.debug("Could not look up info for queue '{}'", context.queue());
            return connectionWrapper;
          }
          LOGGER.debug(
              "Looking affinity with queue '{}' (type = {}, leader = {}, replicas = {}) for '{}'",
              info.name(),
              info.type(),
              info.leader(),
              info.members(),
              connectionName);
          nodesWithAffinity = strategy.nodesWithAffinity(context, info);
        }
        LOGGER.trace(
            "Nodes matching affinity {}: {}, connected to {}.",
            context,
            nodesWithAffinity,
            nodename);
        if (nodesWithAffinity.contains(nodename)) {
          LOGGER.debug("Connected to node '{}' for '{}'", nodename, connectionName);
          return connectionWrapper;
        } else if (attempt == 5) {
          if (candidate.hintedFailure != null) {
            // the nodes with affinity cannot be reached, the caller retries later (recovery)
            throw candidate.hintedFailure;
          }
          LOGGER.debug(
              "Could not find affinity {} after {} attempt(s), using last connection for '{}'.",
              context,
              attempt,
              connectionName);
          return connectionWrapper;
        } else {
          LOGGER.trace("Affinity {} not found with node {}.", context, nodename);
          management.releaseResources(null);
          connectionWrapper.connection().close();
        }
      }
    } catch (AmqpException.AmqpConnectionException e) {
      management.releaseResources(e);
      try {
        if (connectionWrapper != null) {
          connectionWrapper.connection().close();
        }
      } catch (Exception ex) {
        LOGGER.debug(
            "Error while closing native connection while enforcing affinity: {}", ex.getMessage());
      }
      management.markUnavailable();
      LOGGER.info(
          "Cannot enforce affinity {} of '{}' because connection has been closed",
          context,
          connectionName,
          e);
      throw e;
    } catch (RuntimeException e) {
      LOGGER.info(
          "Cannot enforce affinity {} of '{}' error when looking up queue",
          context,
          connectionName,
          e);
      throw e;
    }
  }

  private static List<Address> addressHints(
      List<String> nodesWithAffinity, AffinityCache affinityCache) {
    if (nodesWithAffinity == null) {
      return null;
    }
    return nodesWithAffinity.stream()
        .map(affinityCache::nodenameToAddress)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private static CandidateConnection connect(
      Function<List<Address>, AmqpConnection.NativeConnectionWrapper> connectionFactory,
      List<Address> addresses,
      List<Address> addressHints,
      RetryStrategy retryStrategy,
      AffinityContext context,
      String connectionName) {
    try {
      return new CandidateConnection(
          retryStrategy.maybeRetry(() -> connectionFactory.apply(addressHints)), null);
    } catch (AmqpException.AmqpConnectionException e) {
      if (addressHints == null || addressHints.isEmpty()) {
        throw e;
      }
      LOGGER.debug(
          "Could not connect to nodes with affinity {} for '{}' ({}), trying the other addresses",
          context,
          connectionName,
          e.getMessage());
      // the hinted nodes are likely down, no need to try them again
      List<Address> fallbackAddresses =
          addresses.stream().filter(a -> !addressHints.contains(a)).collect(Collectors.toList());
      return new CandidateConnection(
          retryStrategy.maybeRetry(() ->
              connectionFactory.apply(fallbackAddresses.isEmpty() ? null : fallbackAddresses)),
          e);
    }
  }

  private static Management.QueueInfo refreshQueueInfo(
      AmqpManagement management,
      AffinityContext context,
      AffinityCache affinityCache,
      RetryStrategy retryStrategy,
      AmqpException.AmqpConnectionException hintedFailure) {
    try {
      return lookUpQueueInfo(management, context, affinityCache, retryStrategy);
    } catch (AmqpException e) {
      if (hintedFailure == null || e instanceof AmqpException.AmqpConnectionException) {
        throw e;
      }
      // the queue may be unavailable because its nodes cannot be reached
      hintedFailure.addSuppressed(e);
      throw hintedFailure;
    }
  }

  private static final class CandidateConnection {

    private final AmqpConnection.NativeConnectionWrapper connection;
    // the connection failure to the nodes with affinity, if any, before the fallback
    private final AmqpException.AmqpConnectionException hintedFailure;

    private CandidateConnection(
        AmqpConnection.NativeConnectionWrapper connection,
        AmqpException.AmqpConnectionException hintedFailure) {
      this.connection = connection;
      this.hintedFailure = hintedFailure;
    }
  }

  private static Management.QueueInfo lookUpQueueInfo(
      AmqpManagement management,
      AffinityContext affinity,
      AffinityCache cache,
      RetryStrategy retryStrategy) {
    return retryStrategy.maybeRetry(() -> {
      Management.QueueInfo info = null;
      management.init();
      try {
        info = management.queueInfo(affinity.queue());
        cache.queueInfo(info);
      } catch (AmqpException.AmqpEntityDoesNotExistException e) {
        LOGGER.debug("Queue '{}' does not exist.", affinity.queue());
        cache.clearQueueInfoEntry(affinity.queue());
        // we just return null, caller will have to return the last connection
      }
      return info;
    });
  }

  // TODO clean affinity cache (LRU or size-based)
  static class AffinityCache {

    private final ConcurrentMap<String, Management.QueueInfo> queueInfoCache =
        new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Address> nodenameToAddressMapping =
        new ConcurrentHashMap<>();

    AffinityCache queueInfo(Management.QueueInfo info) {
      this.queueInfoCache.put(info.name(), info);
      return this;
    }

    Management.QueueInfo queueInfo(String queue) {
      return this.queueInfoCache.get(queue);
    }

    void clearQueueInfoEntry(String queue) {
      this.queueInfoCache.remove(queue);
    }

    AffinityCache nodenameToAddress(String nodename, Address address) {
      if (nodename != null && !nodename.isBlank()) {
        this.nodenameToAddressMapping.put(nodename, address);
      }
      return this;
    }

    Address nodenameToAddress(String name) {
      return this.nodenameToAddressMapping.get(name);
    }
  }

  static class AffinityContext implements ConnectionSettings.AffinityContext {

    private final String queue;
    private final ConnectionSettings.Affinity.Operation operation;

    AffinityContext(String queue, ConnectionSettings.Affinity.Operation operation) {
      this.queue = queue;
      this.operation = operation;
    }

    public String queue() {
      return this.queue;
    }

    public ConnectionSettings.Affinity.Operation operation() {
      return this.operation;
    }

    @Override
    public String toString() {
      return "{" + "queue='" + queue + '\'' + ", operation=" + operation + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AffinityContext that = (AffinityContext) o;
      return Objects.equals(queue, that.queue) && operation == that.operation;
    }

    @Override
    public int hashCode() {
      return Objects.hash(queue, operation);
    }
  }

  static class LeaderForPublishingMembersForConsumingStrategy
      implements ConnectionSettings.AffinityStrategy {

    @Override
    public List<String> nodesWithAffinity(
        ConnectionSettings.AffinityContext context, Management.QueueInfo info) {
      List<String> nodesWithAffinity =
          (info.members() == null || info.members().isEmpty())
              ? Collections.emptyList()
              : List.copyOf(info.members());
      if (context.operation() == ConnectionSettings.Affinity.Operation.PUBLISH) {
        if (info.leader() != null && !info.leader().isBlank()) {
          nodesWithAffinity = List.of(info.leader());
        } else {
          nodesWithAffinity = Collections.emptyList();
        }
      }
      return nodesWithAffinity;
    }
  }

  static class LeaderForPublishingFollowersForConsumingStrategy
      implements ConnectionSettings.AffinityStrategy {

    @Override
    public List<String> nodesWithAffinity(
        ConnectionSettings.AffinityContext context, Management.QueueInfo info) {
      ConnectionSettings.Affinity.Operation operation = context.operation();
      String leader = info.leader();
      List<String> replicas = info.members() == null ? Collections.emptyList() : info.members();
      List<String> nodesWithAffinity;
      LOGGER.debug(
          "Trying to find affinity {} with leader = {}, replicas = {}", context, leader, replicas);
      if (info.type() == Management.QueueType.QUORUM
          || info.type() == Management.QueueType.STREAM) {
        // we may choose between leader and replicas
        if (operation == ConnectionSettings.Affinity.Operation.PUBLISH) {
          if (leader == null || leader.isBlank()) {
            nodesWithAffinity = Collections.emptyList();
          } else {
            nodesWithAffinity = List.of(leader);
          }
        } else if (operation == ConnectionSettings.Affinity.Operation.CONSUME) {
          List<String> followers =
              replicas.stream()
                  .filter(Objects::nonNull)
                  .filter(r -> !r.equals(leader))
                  .collect(Collectors.toList());
          if (!followers.isEmpty()) {
            nodesWithAffinity = List.copyOf(followers);
          } else if (leader != null && !leader.isBlank()) {
            nodesWithAffinity = List.of(leader);
          } else {
            nodesWithAffinity = Collections.emptyList();
          }
        } else {
          // we don't care about the operation, we just return a replica
          nodesWithAffinity = List.copyOf(replicas);
        }
      } else {
        // classic queue, leader and replica are the same
        nodesWithAffinity = List.copyOf(replicas);
      }
      LOGGER.debug("Nodes with affinity: {}", nodesWithAffinity);
      return nodesWithAffinity;
    }
  }

  @FunctionalInterface
  interface RetryStrategy {

    <T> T maybeRetry(Supplier<T> task);
  }
}
