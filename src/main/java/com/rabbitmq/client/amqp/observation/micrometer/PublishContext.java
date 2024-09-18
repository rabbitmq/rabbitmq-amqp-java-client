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
package com.rabbitmq.client.amqp.observation.micrometer;

import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.ObservationCollector;
import io.micrometer.observation.transport.SenderContext;

/** Publish context. */
public class PublishContext extends SenderContext<Message> {

  private final String exchange;
  private final String routingKey;
  private final int payloadSizeBytes;
  private final String messageId;
  private final String correlationId;
  private final String peerAddress;
  private final int peerPort;

  PublishContext(
      String exchange,
      String routingKey,
      Message message,
      ObservationCollector.ConnectionInfo connectionInfo) {
    super((carrier, key, value) -> carrier.annotation(Utils.annotationKey(key), value));
    if (exchange == null && routingKey == null) {
      String to = message.to();
      String[] exRk = Utils.exchangeRoutingKeyFromTo(to);
      this.exchange = exRk[0];
      this.routingKey = exRk[1];
    } else {
      this.exchange = exchange;
      this.routingKey = routingKey;
    }
    this.payloadSizeBytes = message.body() == null ? 0 : message.body().length;
    this.messageId = message.messageId() == null ? null : message.messageId().toString();
    this.correlationId =
        message.correlationId() == null ? null : message.correlationId().toString();
    this.peerAddress = connectionInfo == null ? "" : connectionInfo.peerAddress();
    this.peerPort = connectionInfo == null ? 0 : connectionInfo.peerPort();
    setCarrier(message);
  }

  public String exchange() {
    return this.exchange;
  }

  public String routingKey() {
    return this.routingKey;
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

  public String peerAddress() {
    return this.peerAddress;
  }

  public int peerPort() {
    return this.peerPort;
  }
}
