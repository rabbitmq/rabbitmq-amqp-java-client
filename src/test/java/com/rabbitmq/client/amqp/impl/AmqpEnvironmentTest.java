// Copyright (c) 2026 Broadcom. All Rights Reserved.
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

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class AmqpEnvironmentTest {

  @Test
  void closingEnvironmentShouldStopInternalEventLoopThreadsPromptly() {
    AmqpEnvironment environment = (AmqpEnvironment) TestUtils.environmentBuilder().build();
    try {
      // opening a connection starts the event loop threads
      environment.connectionBuilder().build();
      List<Thread> eventLoopThreads = eventLoopThreads(environment);
      assertThat(eventLoopThreads).hasSize(2).allMatch(Thread::isAlive);

      long start = System.nanoTime();
      environment.close();
      Duration closeDuration = Duration.ofNanos(System.nanoTime() - start);

      // the threads are not daemon threads, lingering ones delay JVM exit (#434)
      assertThat(closeDuration).isLessThan(Duration.ofSeconds(2));
      waitAtMost(Duration.ofSeconds(1), () -> eventLoopThreads.stream().noneMatch(Thread::isAlive));
    } finally {
      environment.close();
    }
  }

  private static List<Thread> eventLoopThreads(AmqpEnvironment environment) {
    String prefix = String.format("rabbitmq-amqp-environment-%d-", environment.id());
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t ->
            t.getName().startsWith(prefix + "event-loop-")
                || t.getName().startsWith(prefix + "connection-state-loop-"))
        .collect(Collectors.toList());
  }
}
