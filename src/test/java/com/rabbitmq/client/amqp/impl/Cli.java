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

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

abstract class Cli {

  private Cli() {}

  private static final Pattern CONNECTION_NAME_PATTERN =
      Pattern.compile("\"connection_name\",\"(?<name>[a-zA-Z0-9_\\-]+)?\"");
  private static final String DOCKER_PREFIX = "DOCKER:";
  private static final Map<String, String> DOCKER_NODES_TO_CONTAINERS =
      Map.of(
          "rabbit@node0", "rabbitmq0",
          "rabbit@node1", "rabbitmq1",
          "rabbit@node2", "rabbitmq2");

  public static String rabbitmqctlCommand() {
    String rabbitmqCtl = System.getProperty("rabbitmqctl.bin");
    if (rabbitmqCtl == null) {
      rabbitmqCtl = "DOCKER:rabbitmq";
    }
    if (rabbitmqCtl.startsWith(DOCKER_PREFIX)) {
      String containerId = rabbitmqCtl.split(":")[1];
      return "docker exec " + containerId + " rabbitmqctl";
    } else {
      return rabbitmqCtl;
    }
  }

  private static String rabbitmqQueuesCommand() {
    String rabbitmqctl = rabbitmqctlCommand();
    int lastIndex = rabbitmqctl.lastIndexOf("rabbitmqctl");
    if (lastIndex == -1) {
      throw new IllegalArgumentException("Not a valid rabbitqmctl command: " + rabbitmqctl);
    }
    return rabbitmqctl.substring(0, lastIndex) + "rabbitmq-queues";
  }

  private static String rabbitmqUpgradeCommand() {
    String rabbitmqctl = rabbitmqctlCommand();
    int lastIndex = rabbitmqctl.lastIndexOf("rabbitmqctl");
    if (lastIndex == -1) {
      throw new IllegalArgumentException("Not a valid rabbitqmctl command: " + rabbitmqctl);
    }
    return rabbitmqctl.substring(0, lastIndex) + "rabbitmq-upgrade";
  }

  static String rabbitmqStreamsCommand() {
    String rabbitmqctl = rabbitmqctlCommand();
    int lastIndex = rabbitmqctl.lastIndexOf("rabbitmqctl");
    if (lastIndex == -1) {
      throw new IllegalArgumentException("Not a valid rabbitqmctl command: " + rabbitmqctl);
    }
    return rabbitmqctl.substring(0, lastIndex) + "rabbitmq-streams";
  }

  static ProcessState rabbitmqctl(String command) {
    return executeCommand(rabbitmqctlCommand() + " " + command);
  }

  static ProcessState rabbitmqQueues(String command) {
    return executeCommand(rabbitmqQueuesCommand() + " " + command);
  }

  static ProcessState rabbitmqStreams(String command) {
    return executeCommand(rabbitmqStreamsCommand() + " " + command);
  }

  static ProcessState rabbitmqctlIgnoreError(String command) {
    return executeCommand(rabbitmqctlCommand() + " " + command, true);
  }

  private static ProcessState executeCommand(String command) {
    return executeCommand(command, false);
  }

  private static ProcessState executeCommand(String command, boolean ignoreError) {
    Process pr = executeCommandProcess(command);
    InputStreamPumpState inputState = new InputStreamPumpState(pr.getInputStream());
    InputStreamPumpState errorState = new InputStreamPumpState(pr.getErrorStream());

    int ev = waitForExitValue(pr, inputState, errorState);
    inputState.pump();
    errorState.pump();
    if (ev != 0 && !ignoreError) {
      throw new RuntimeException(
          "unexpected command exit value: "
              + ev
              + "\ncommand: "
              + command
              + "\n"
              + "\nstdout:\n"
              + inputState.buffer.toString()
              + "\nstderr:\n"
              + errorState.buffer.toString()
              + "\n");
    }
    return new ProcessState(inputState);
  }

  private static int waitForExitValue(
      Process pr, InputStreamPumpState inputState, InputStreamPumpState errorState) {
    while (true) {
      try {
        inputState.pump();
        errorState.pump();
        pr.waitFor();
        break;
      } catch (InterruptedException ignored) {
      }
    }
    return pr.exitValue();
  }

  private static Process executeCommandProcess(String command) {
    String[] finalCommand;
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      finalCommand = new String[4];
      finalCommand[0] = "C:\\winnt\\system32\\cmd.exe";
      finalCommand[1] = "/y";
      finalCommand[2] = "/c";
      finalCommand[3] = command;
    } else {
      finalCommand = new String[3];
      finalCommand[0] = "/bin/sh";
      finalCommand[1] = "-c";
      finalCommand[2] = command;
    }
    try {
      return Runtime.getRuntime().exec(finalCommand);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void startBroker() {
    rabbitmqctl("start_app");
  }

  public static void stopBroker() {
    rabbitmqctl("stop_app");
  }

  public static void addVhost(String vhost) {
    rabbitmqctl("add_vhost " + vhost);
  }

  public static void addUser(String username, String password) {
    rabbitmqctl(format("add_user %s %s", username, password));
  }

  public static void setPermissions(String username, String vhost, String permission) {
    setPermissions(username, vhost, asList(permission, permission, permission));
  }

  public static void setPermissions(String username, String vhost, List<String> permissions) {
    if (permissions.size() != 3) {
      throw new IllegalArgumentException();
    }
    rabbitmqctl(
        format(
            "set_permissions --vhost %s %s '%s' '%s' '%s'",
            vhost, username, permissions.get(0), permissions.get(1), permissions.get(2)));
  }

  public static void deleteUser(String username) {
    rabbitmqctl(format("delete_user %s", username));
  }

  public static void deleteVhost(String vhost) {
    rabbitmqctl("delete_vhost " + vhost);
  }

  public static void closeConnection(String clientProvidedName) {
    ConnectionInfo ci = findConnectionInfoFor(listConnections(), clientProvidedName);
    closeConnection(ci);
  }

  private static void closeConnection(ConnectionInfo ci) {
    rabbitmqctl("close_connection '" + ci.pid() + "' 'Closed via rabbitmqctl'");
  }

  static boolean exchangeExists(String exchange) {
    String output = rabbitmqctl("list_exchanges -s name").output();
    return Arrays.asList(output.split("\n")).contains(exchange);
  }

  static void addQuorumQueueMember(String queue, String node) {
    rabbitmqQueues(" add_member " + queue + " " + node);
  }

  static void deleteQuorumQueueMember(String queue, String node) {
    rabbitmqQueues(" delete_member " + queue + " " + node);
  }

  static void addStreamMember(String stream, String node) {
    rabbitmqStreams(" add_replica " + stream + " " + node);
  }

  static void deleteStreamMember(String stream, String node) {
    rabbitmqStreams(" delete_replica " + stream + " " + node);
  }

  static void restartStream(String stream) {
    rabbitmqStreams(" restart_stream " + stream);
  }

  static void pauseNode(String node) {
    String containerId = DOCKER_NODES_TO_CONTAINERS.get(node);
    Assert.notNull(containerId, "No container for node " + node);
    executeCommand("docker pause " + containerId);
  }

  static void unpauseNode(String node) {
    executeCommand("docker unpause " + nodeToDockerContainer(node));
  }

  static void restartNode(String node) {
    String container = nodeToDockerContainer(node);
    String dockerCommand = "docker exec " + container + " ";
    String rabbitmqUpgradeCommand = dockerCommand + "rabbitmq-upgrade ";
    executeCommand(rabbitmqUpgradeCommand + "await_online_quorum_plus_one -t 300");
    executeCommand(rabbitmqUpgradeCommand + "drain");
    executeCommand("docker stop " + container);
    executeCommand("docker start " + container);
    String otherContainer =
        DOCKER_NODES_TO_CONTAINERS.values().stream()
            .filter(c -> !c.endsWith(container))
            .findAny()
            .get();
    executeCommand(
        "docker exec "
            + otherContainer
            + " rabbitmqctl await_online_nodes "
            + DOCKER_NODES_TO_CONTAINERS.size());
    executeCommand(dockerCommand + "rabbitmqctl status");
  }

  static void rebalance() {
    rabbitmqQueues("rebalance all");
  }

  static List<String> nodes() {
    List<String> clusterNodes = new ArrayList<>();
    clusterNodes.add(rabbitmqctl("eval 'node().'").output().trim());
    List<String> nodes =
        Arrays.stream(
                rabbitmqctl("eval 'nodes().'")
                    .output()
                    .replace("[", "")
                    .replace("]", "")
                    .split(","))
            .map(String::trim)
            .collect(Collectors.toList());
    clusterNodes.addAll(nodes);
    return List.copyOf(clusterNodes);
  }

  private static String nodeToDockerContainer(String node) {
    String containerId = DOCKER_NODES_TO_CONTAINERS.get(node);
    Assert.notNull(containerId, "No container for node " + node);
    return containerId;
  }

  static List<ConnectionInfo> listConnections() {
    String output =
        rabbitmqctl("list_connections -q pid peer_port client_properties auth_mechanism").output();
    // output (header line presence depends on broker version):
    // pid	peer_port
    // <rabbit@mercurio.1.11491.0>	58713
    String[] allLines = output.split("\n");
    List<ConnectionInfo> result = new ArrayList<ConnectionInfo>();
    for (String line : allLines) {
      if (line != null && !line.trim().isEmpty()) {
        // line: <rabbit@mercurio.1.11491.0>	58713
        String[] columns = line.split("\t");
        // can be also header line, so ignoring NumberFormatException
        try {
          int peerPort = Integer.parseInt(columns[1]);
          String clientProperties = columns[2];
          String clientProvidedName = extractConnectionName(clientProperties);
          String authMechanism = columns[3];
          result.add(
              new ConnectionInfo(
                  columns[0], peerPort, clientProperties, clientProvidedName, authMechanism));
        } catch (NumberFormatException e) {
          // OK
        }
      }
    }
    return result;
  }

  private static ConnectionInfo findConnectionInfoFor(
      List<ConnectionInfo> xs, String clientProvidedName) {
    Predicate<ConnectionInfo> predicate = ci -> clientProvidedName.equals(ci.clientProvidedName());
    return xs.stream().filter(predicate).findFirst().orElse(null);
  }

  static List<QueueInfo> listQueues() {
    String output =
        rabbitmqctl("list_queues -q name,messages,messages_ready,messages_unacknowledged").output();
    String[] allLines = output.split("\n");
    List<QueueInfo> result = new ArrayList<>();
    for (int i = 1; i < allLines.length; i++) {
      String line = allLines[i];
      if (line != null && !line.trim().isEmpty()) {
        String[] columns = line.split("\t");
        result.add(
            new QueueInfo(
                columns[0],
                Integer.parseInt(columns[1]),
                Integer.parseInt(columns[2]),
                Integer.parseInt(columns[3])));
      }
    }
    return result;
  }

  static QueueInfo queueInfo(String q) {
    return listQueues().stream().filter(info -> q.equals(info.name())).findFirst().get();
  }

  public static AutoCloseable diskAlarm() throws Exception {
    return new CallableAutoCloseable(
        () -> {
          setDiskAlarm();
          return null;
        },
        () -> {
          clearDiskAlarm();
          return null;
        });
  }

  public static AutoCloseable memoryAlarm() throws Exception {
    return new CallableAutoCloseable(
        () -> {
          setMemoryAlarm();
          return null;
        },
        () -> {
          clearMemoryAlarm();
          return null;
        });
  }

  private static void setDiskAlarm() {
    setResourceAlarm("disk");
  }

  private static void clearDiskAlarm() {
    clearResourceAlarm("disk");
  }

  private static void setMemoryAlarm() {
    setResourceAlarm("memory");
  }

  static void clearMemoryAlarm() {
    clearResourceAlarm("memory");
  }

  private static void setResourceAlarm(String source) {
    rabbitmqctl("eval 'rabbit_alarm:set_alarm({{resource_limit, " + source + ", node()}, []}).'");
  }

  private static void clearResourceAlarm(String source) {
    rabbitmqctl("eval 'rabbit_alarm:clear_alarm({resource_limit, " + source + ", node()}).'");
  }

  static class ConnectionInfo {
    private final String pid;
    private final int peerPort;
    private final String clientProperties;
    private final String clientProvidedName;
    private final String authMechanism;

    ConnectionInfo(
        String pid,
        int peerPort,
        String clientProperties,
        String clientProvidedName,
        String authMechanism) {
      this.pid = pid;
      this.peerPort = peerPort;
      this.clientProperties = clientProperties;
      this.clientProvidedName = clientProvidedName;
      this.authMechanism = authMechanism;
    }

    String pid() {
      return pid;
    }

    int peerPort() {
      return peerPort;
    }

    String clientProperties() {
      return clientProperties;
    }

    String clientProvidedName() {
      return clientProvidedName;
    }

    String authMechanism() {
      return authMechanism;
    }

    @Override
    public String toString() {
      return "ConnectionInfo{"
          + "pid='"
          + pid
          + '\''
          + ", peerPort="
          + peerPort
          + ", clientProperties='"
          + clientProperties
          + '\''
          + ", clientProvidedName='"
          + clientProvidedName
          + '\''
          + ", authMechanism='"
          + authMechanism
          + '\''
          + '}';
    }
  }

  static class QueueInfo {
    private final String name;
    private final int messsageCount;
    private final int readyMessageCount;
    private final int unackedMessageCount;

    QueueInfo(String name, int messsageCount, int readyMessageCount, int unackedMessageCount) {
      this.name = name;
      this.messsageCount = messsageCount;
      this.readyMessageCount = readyMessageCount;
      this.unackedMessageCount = unackedMessageCount;
    }

    String name() {
      return name;
    }

    int messsageCount() {
      return messsageCount;
    }

    int readyMessageCount() {
      return readyMessageCount;
    }

    int unackedMessageCount() {
      return unackedMessageCount;
    }
  }

  static class ProcessState {

    private final InputStreamPumpState inputState;

    ProcessState(InputStreamPumpState inputState) {
      this.inputState = inputState;
    }

    String output() {
      return inputState.buffer.toString();
    }
  }

  private static class InputStreamPumpState {

    private final BufferedReader reader;
    private final StringBuilder buffer;

    private InputStreamPumpState(InputStream in) {
      this.reader = new BufferedReader(new InputStreamReader(in));
      this.buffer = new StringBuilder();
    }

    void pump() {
      String line;
      while (true) {
        try {
          if ((line = reader.readLine()) == null) break;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        buffer.append(line).append("\n");
      }
    }
  }

  private static String extractConnectionName(String clientProperties) {
    if (clientProperties.contains("\"connection_name\",")) {
      Matcher matcher = CONNECTION_NAME_PATTERN.matcher(clientProperties);
      matcher.find();
      return matcher.group("name");
    } else {
      return null;
    }
  }

  static String hostname() {
    return executeCommand("hostname").output();
  }

  private static final class CallableAutoCloseable implements AutoCloseable {

    private final Callable<Void> end;

    private CallableAutoCloseable(Callable<Void> start, Callable<Void> end) throws Exception {
      this.end = end;
      start.call();
    }

    @Override
    public void close() throws Exception {
      this.end.call();
    }
  }
}
