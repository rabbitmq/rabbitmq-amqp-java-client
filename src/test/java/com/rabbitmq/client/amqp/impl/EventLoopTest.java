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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;

public class EventLoopTest {

  static ExecutorService executorService;
  EventLoop<State> loop;

  @BeforeAll
  static void beforeAll() {
    executorService = Executors.newSingleThreadExecutor();
  }

  @BeforeEach
  void beforeEach() {
    loop = new EventLoop<>(State::new, "test", executorService);
  }

  @AfterEach
  void afterEach() {
    loop.close();
  }

  @AfterAll
  static void afterAll() {
    executorService.shutdownNow();
  }

  @Test
  void submitTasks() {
    AtomicInteger buffer = new AtomicInteger();
    loop.submit(s -> s.a = 42);
    loop.submit(s -> buffer.set(s.a));
    assertThat(buffer).hasValue(42);

    loop.submit(
        s -> {
          s.a = 1;
          s.b = 2;
        });
    loop.submit(s -> buffer.set(s.a));
    assertThat(buffer).hasValue(1);
    loop.submit(s -> buffer.set(s.b));
    assertThat(buffer).hasValue(2);

    loop.close();
    assertThatThrownBy(() -> loop.submit(s -> {})).isInstanceOf(IllegalStateException.class);
  }

  static class State {

    private int a, b;
  }
}
