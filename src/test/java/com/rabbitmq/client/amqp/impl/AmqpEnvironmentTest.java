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

import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.Environment;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class AmqpEnvironmentTest {

  @Test
  void closingEnvironmentShouldStopInternalEventLoopThreadsPromptly() {
    List<Thread> eventLoopThreads;
    try (Environment environment = TestUtils.environmentBuilder().build()) {
      // opening a connection starts the environment internal event loop threads
      environment.connectionBuilder().build();
      eventLoopThreads = eventLoopThreads(environment);
      assertThat(eventLoopThreads).isNotEmpty().allMatch(Thread::isAlive);
    }
    // the environment is closed by the try-with-resources block, its internal event loop threads
    // are not daemon threads and used to stay alive for Netty's default graceful shutdown quiet
    // period (2 seconds), which kept the JVM from exiting once the application was done (#434)
    waitAtMost(Duration.ofSeconds(1), () -> eventLoopThreads.stream().noneMatch(Thread::isAlive));
  }

  private static List<Thread> eventLoopThreads(Environment environment) {
    String id = environment.toString().substring("rabbitmq-amqp-".length());
    String prefix = "rabbitmq-amqp-environment-" + id + "-";
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith(prefix))
        .filter(
            t ->
                t.getName().contains("event-loop-")
                    || t.getName().contains("connection-state-loop-"))
        .collect(Collectors.toList());
  }
}
