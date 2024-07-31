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
import com.rabbitmq.client.amqp.ConnectionSettings;
import com.rabbitmq.client.amqp.Management;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionUtils.class);

  private ConnectionUtils() {}

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
