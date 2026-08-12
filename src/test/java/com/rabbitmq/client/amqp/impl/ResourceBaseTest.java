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

import static com.rabbitmq.client.amqp.Resource.State.CLOSED;
import static com.rabbitmq.client.amqp.Resource.State.CLOSING;
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.Resource.State.RECOVERING;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.Resource;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;

public class ResourceBaseTest {

  @Test
  void stateDoesNotGoBackToOpenOnceClosing() {
    List<Resource.State[]> transitions = new CopyOnWriteArrayList<>();
    TestResource resource =
        new TestResource(
            List.of(
                ctx ->
                    transitions.add(
                        new Resource.State[] {ctx.previousState(), ctx.currentState()})),
            Runnable::run);

    resource.state(OPEN);
    resource.state(CLOSING);
    resource.state(CLOSED);

    // late writes from a concurrent recovery must not resurrect the closed resource
    resource.state(OPEN);
    resource.state(RECOVERING);

    assertThat(resource.state()).isEqualTo(CLOSED);
    assertThat(transitions)
        .noneMatch(t -> t[0] == CLOSED && t[1] != CLOSED)
        .noneMatch(t -> t[0] == CLOSING && t[1] != CLOSED);
  }

  @Test
  void closedIsReachableFromClosing() {
    TestResource resource = new TestResource(List.of(), Runnable::run);

    resource.state(OPEN);
    resource.state(CLOSING);
    resource.state(CLOSED);

    assertThat(resource.state()).isEqualTo(CLOSED);
  }

  private static class TestResource extends ResourceBase {

    private TestResource(List<Resource.StateListener> listeners, Executor executor) {
      super(listeners, executor);
    }
  }
}
