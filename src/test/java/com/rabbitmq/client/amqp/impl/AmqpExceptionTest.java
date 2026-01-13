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

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.AmqpException;
import org.junit.jupiter.api.Test;

class AmqpExceptionTest {

  @Test
  void percentageSignShouldNotCrash() {
    String explanation =
        "no queue '/queues/amq.rabbitmq.reply-to.g1h2AA5yZXBseUA2MzU2NjAyOAF2N5IAAAAAaV6Pjg%3D%3D.2AhYyMAxyDz7HiO7prAvqg%3D%3D' in vhost 'dfi'";
    AmqpException amqpException = new AmqpException(explanation);
    assertThat(amqpException).hasMessageContaining(explanation);
  }
}
