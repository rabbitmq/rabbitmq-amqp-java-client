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
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionUtils.class);

  private ConnectionUtils() {}

  static AmqpConnection.NativeConnectionWrapper enforceAffinity(
      Function<List<Address>, AmqpConnection.NativeConnectionWrapper> connectionFactory,
      AmqpManagement management,
      ConnectionAffinity affinity,
      AffinityCache affinityCache) {
    // TODO add retry for sensitive operations in affinity mechanism
    if (affinity == null) {
      // no affinity asked, we create a connection and return it
      return connectionFactory.apply(null);
    }
    try {
      AmqpConnection.NativeConnectionWrapper pickedConnection = null;
      int attemptCount = 0;
      boolean queueInfoRefreshed = false;
      List<String> nodesWithAffinity = null;
      Management.QueueInfo info = affinityCache.queueInfo(affinity.queue());
      while (pickedConnection == null) {
        attemptCount++;
        AmqpConnection.NativeConnectionWrapper connectionWrapper = null;
        if (info == null) {
          connectionWrapper = connectionFactory.apply(null);
          info = lookUpQueueInfo(management, affinity, affinityCache);
          queueInfoRefreshed = true;
        }
        if (info == null) {
          // likely we could not look up the queue info, because e.g. the queue does not exist
          return connectionWrapper;
        }
        LOGGER.debug(
            "Looking affinity with queue '{}' (type = {}, leader = {}, replicas = {})",
            info.name(),
            info.type(),
            info.leader(),
            info.replicas());
        if (nodesWithAffinity == null) {
          nodesWithAffinity = findAffinity(affinity, info);
        }
        if (connectionWrapper == null) {
          List<Address> addressHints =
              nodesWithAffinity.stream()
                  .map(affinityCache::nodenameToAddress)
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList());
          connectionWrapper = connectionFactory.apply(addressHints);
        }
        LOGGER.debug("Nodes matching affinity {}: {}.", affinity, nodesWithAffinity);
        LOGGER.debug("Currently connected to node {}.", connectionWrapper.nodename());
        affinityCache.nodenameToAddress(connectionWrapper.nodename(), connectionWrapper.address());
        if (nodesWithAffinity.contains(connectionWrapper.nodename())) {
          if (!queueInfoRefreshed) {
            LOGGER.debug(
                "Found affinity, but refreshing queue information to check affinity is still valid.");
            info = lookUpQueueInfo(management, affinity, affinityCache);
            if (info == null) {
              LOGGER.debug("Could not look up info for queue '{}'", affinity.queue());
              pickedConnection = connectionWrapper;
            } else {
              nodesWithAffinity = findAffinity(affinity, info);
              queueInfoRefreshed = true;
              if (nodesWithAffinity.contains(connectionWrapper.nodename())) {
                pickedConnection = connectionWrapper;
              } else {
                LOGGER.debug("Affinity no longer valid, retrying.");
                management.releaseResources();
                connectionWrapper.connection().close();
              }
            }
          } else {
            pickedConnection = connectionWrapper;
          }
          if (pickedConnection != null) {
            LOGGER.debug("Returning connection to node {}", pickedConnection.nodename());
          }
        } else if (attemptCount == 5) {
          LOGGER.debug(
              "Could not find affinity {} after {} attempt(s), using last connection.",
              affinity,
              attemptCount);
          pickedConnection = connectionWrapper;
        } else {
          LOGGER.debug(
              "Affinity {} not found with node {}.", affinity, connectionWrapper.nodename());
          if (!queueInfoRefreshed) {
            info = lookUpQueueInfo(management, affinity, affinityCache);
            if (info != null) {
              nodesWithAffinity = findAffinity(affinity, info);
              queueInfoRefreshed = true;
            }
          }
          management.releaseResources();
          connectionWrapper.connection().close();
        }
      }
      return pickedConnection;
    } catch (RuntimeException e) {
      LOGGER.warn("Cannot enforce affinity {} of error when looking up queue", affinity, e);
      throw e;
    }
  }

  private static Management.QueueInfo lookUpQueueInfo(
      AmqpManagement management, ConnectionAffinity affinity, AffinityCache cache) {
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

  static List<String> findAffinity(ConnectionAffinity affinity, Management.QueueInfo info) {
    ConnectionSettings.Affinity.Operation operation = affinity.operation();
    String leader = info.leader();
    List<String> replicas = info.replicas() == null ? Collections.emptyList() : info.replicas();
    List<String> nodesWithAffinity;
    LOGGER.debug(
        "Trying to find affinity {} with leader = {}, replicas = {}", affinity, leader, replicas);
    if (info.type() == Management.QueueType.QUORUM || info.type() == Management.QueueType.STREAM) {
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

  static class ConnectionAffinity {

    private final String queue;
    private final ConnectionSettings.Affinity.Operation operation;

    ConnectionAffinity(String queue, ConnectionSettings.Affinity.Operation operation) {
      this.queue = queue;
      this.operation = operation;
    }

    String queue() {
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
      ConnectionAffinity that = (ConnectionAffinity) o;
      return Objects.equals(queue, that.queue) && operation == that.operation;
    }

    @Override
    public int hashCode() {
      return Objects.hash(queue, operation);
    }
  }
}
