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
package com.rabbitmq.model.amqp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract class Cli {

  private Cli() {}

  private static final Pattern CONNECTION_NAME_PATTERN =
      Pattern.compile("\"connection_name\",\"(?<name>[a-zA-Z0-9\\-]+)?\"");
  private static final String DOCKER_PREFIX = "DOCKER:";

  public static String rabbitmqctlCommand() {
    String rabbitmqCtl = System.getProperty("rabbitmqctl.bin");
    if (rabbitmqCtl == null) {
      throw new IllegalStateException("Please define the rabbitmqctl.bin system property");
    }
    if (rabbitmqCtl.startsWith(DOCKER_PREFIX)) {
      String containerId = rabbitmqCtl.split(":")[1];
      return "docker exec " + containerId + " rabbitmqctl";
    } else {
      return rabbitmqCtl;
    }
  }

  private static ProcessState rabbitmqctl(String command) {
    return executeCommand(rabbitmqctlCommand() + " " + command);
  }

  private static ProcessState executeCommand(String command) {
    Process pr = executeCommandProcess(command);
    InputStreamPumpState inputState = new InputStreamPumpState(pr.getInputStream());
    InputStreamPumpState errorState = new InputStreamPumpState(pr.getErrorStream());

    int ev = waitForExitValue(pr, inputState, errorState);
    inputState.pump();
    errorState.pump();
    if (ev != 0) {
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

  public static void closeConnection(String clientProvidedName) {
    ConnectionInfo ci = findConnectionInfoFor(listConnections(), clientProvidedName);
    closeConnection(ci);
  }

  private static void closeConnection(ConnectionInfo ci) {
    rabbitmqctl("close_connection '" + ci.getPid() + "' 'Closed via rabbitmqctl'");
  }

  static List<ConnectionInfo> listConnections() {
    String output = rabbitmqctl("list_connections -q pid peer_port client_properties").output();
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
          int peerPort = Integer.valueOf(columns[1]);
          String clientProperties = columns[2];
          String clientProvidedName = extractConnectionName(clientProperties);
          result.add(
              new ConnectionInfo(columns[0], peerPort, clientProperties, clientProvidedName));
        } catch (NumberFormatException e) {
          // OK
        }
      }
    }
    return result;
  }

  private static ConnectionInfo findConnectionInfoFor(
      List<ConnectionInfo> xs, String clientProvidedName) {
    Predicate<ConnectionInfo> predicate =
        ci -> clientProvidedName.equals(ci.getClientProvidedName());
    return xs.stream().filter(predicate).findFirst().orElse(null);
  }

  private static class ConnectionInfo {
    private final String pid;
    private final int peerPort;
    private final String clientProperties;
    private final String clientProvidedName;

    ConnectionInfo(String pid, int peerPort, String clientProperties, String clientProvidedName) {
      this.pid = pid;
      this.peerPort = peerPort;
      this.clientProperties = clientProperties;
      this.clientProvidedName = clientProvidedName;
    }

    String getPid() {
      return pid;
    }

    int getPeerPort() {
      return peerPort;
    }

    String getClientProperties() {
      return clientProperties;
    }

    String getClientProvidedName() {
      return clientProvidedName;
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
          + '}';
    }
  }

  private static class ProcessState {

    private final InputStreamPumpState inputState;

    ProcessState(InputStreamPumpState inputState) {
      this.inputState = inputState;
    }

    private String output() {
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
          if (!((line = reader.readLine()) != null)) break;
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
}
