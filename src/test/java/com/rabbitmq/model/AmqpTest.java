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
package com.rabbitmq.model;

import static com.rabbitmq.model.TestUtils.CountDownLatchConditions.completed;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.*;
import com.rabbitmq.model.amqp.AmqpEnvironmentBuilder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class AmqpTest {

  @Test
  void queueDeclareDeletePublishConsume(TestInfo info) {
    String q = TestUtils.name(info);
    Environment environment = new AmqpEnvironmentBuilder().build();
    try {
      environment.management().queue().name(q).quorum().queue().declare();
      String address = "/amq/queue/" + q;
      Publisher publisher = environment.publisherBuilder().address(address).build();

      CountDownLatch confirmLatch = new CountDownLatch(1);
      UUID messageId = UUID.randomUUID();
      publisher.publish(
          publisher
              .message()
              .addData("hello".getBytes(StandardCharsets.UTF_8))
              .messageId(messageId),
          context -> {
            if (messageId.equals(context.message().messageId())
                && context.status() == Publisher.ConfirmationStatus.CONFIRMED) {
              confirmLatch.countDown();
            }
          });

      assertThat(confirmLatch).is(completed());

      CountDownLatch consumeLatch = new CountDownLatch(1);
      environment
          .consumerBuilder()
          .address(address)
          .messageHandler(
              (context, message) -> {
                context.accept();
                if (message.messageId().equals(messageId)) {
                  consumeLatch.countDown();
                }
              })
          .build();
      assertThat(consumeLatch).is(completed());
    } finally {
      environment.management().queueDeletion().delete(q);
      environment.close();
    }
  }
}
