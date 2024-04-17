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

import static com.rabbitmq.model.amqp.TestUtils.assertThat;

import com.rabbitmq.model.Environment;
import com.rabbitmq.model.Resource;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

public class IdleTimeoutTest {

  @Test
  void connectionShouldCloseAutomaticallyIfNoCommunicationWithBroker() throws Exception {
    Duration idleTimeout = Duration.ofSeconds(2);
    ToxiproxyClient client = new ToxiproxyClient("localhost", 8474);
    Proxy rabbitmqProxy = client.getProxyOrNull("rabbitmq");
    if (rabbitmqProxy != null) {
      rabbitmqProxy.delete();
    }
    rabbitmqProxy = client.createProxy("rabbitmq", "localhost:5673", "localhost:5672");
    try (Environment environment =
        TestUtils.environmentBuilder()
            .connectionSettings()
            .port(5673)
            .environmentBuilder()
            .build()) {
      CountDownLatch closedLatch = new CountDownLatch(1);
      environment
          .connectionBuilder()
          .recovery()
          .activated(false)
          .connectionBuilder()
          .idleTimeout(idleTimeout)
          .listeners(
              context -> {
                if (context.currentState() == Resource.State.CLOSED) {
                  closedLatch.countDown();
                }
              })
          .build();
      rabbitmqProxy
          .toxics()
          .latency("latency", ToxicDirection.DOWNSTREAM, idleTimeout.multipliedBy(10).toMillis());
      assertThat(closedLatch).completes();
    } finally {
      rabbitmqProxy.delete();
    }
  }
}
