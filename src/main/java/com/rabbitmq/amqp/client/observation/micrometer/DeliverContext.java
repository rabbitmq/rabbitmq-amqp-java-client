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
package com.rabbitmq.amqp.client.observation.micrometer;

import com.rabbitmq.amqp.client.Message;
import io.micrometer.observation.transport.ReceiverContext;

public class DeliverContext extends ReceiverContext<Message> {

  private final String exchange;
  private final String routingKey;
  private final String queue;
  private final int payloadSizeBytes;
  private final String messageId;
  private final String correlationId;

  DeliverContext(String exchange, String routingKey, String queue, Message message) {
    super(
        (carrier, key) -> {
          Object result = carrier.annotation(key);
          if (result == null) {
            result = carrier.property(key);
          }
          return result == null ? null : result.toString();
        });
    if (exchange == null && routingKey == null) {
      String to = message.to();
      String[] exRk = Utils.exchangeRoutingKeyFromTo(to);
      this.exchange = exRk[0];
      this.routingKey = exRk[1];
    } else {
      this.exchange = exchange;
      this.routingKey = routingKey;
    }
    this.queue = queue;
    this.payloadSizeBytes = message.body() == null ? 0 : message.body().length;
    this.messageId = message.messageId() == null ? null : message.messageId().toString();
    this.correlationId =
        message.correlationId() == null ? null : message.correlationId().toString();
    setCarrier(message);
  }

  public String exchange() {
    return this.exchange;
  }

  public String routingKey() {
    return this.routingKey;
  }

  public String queue() {
    return this.queue;
  }

  public int payloadSizeBytes() {
    return this.payloadSizeBytes;
  }

  public String messageId() {
    return this.messageId;
  }

  public String correlationId() {
    return this.correlationId;
  }
}
