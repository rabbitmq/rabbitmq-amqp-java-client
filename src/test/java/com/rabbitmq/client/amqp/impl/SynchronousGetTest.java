// Copyright (c) 2025 Broadcom. All Rights Reserved.
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

import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.SynchronousConsumer;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SynchronousGetTest {

  @Test
  void test() {
    SynchronousConsumer consumer = null;

    SynchronousConsumer.Response response = consumer.get(Duration.ofSeconds(10));
    Message message = response.message();
    // process message...
    response.context().accept();

    List<SynchronousConsumer.Response> responses = consumer.get(10, Duration.ofSeconds(10));
    Consumer.BatchContext batch = null;
    for (SynchronousConsumer.Response r : responses) {
      if (batch == null) {
        batch = r.context().batch(responses.size());
      }
      batch.add(r.context());
      Message msg = r.message();
      // process message...
    }
    batch.accept();
  }
}
