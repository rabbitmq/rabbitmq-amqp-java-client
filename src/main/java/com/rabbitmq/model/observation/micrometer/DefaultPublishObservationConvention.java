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
package com.rabbitmq.model.observation.micrometer;

import com.rabbitmq.model.observation.micrometer.AmqpObservationDocumentation.HighCardinalityTags;
import com.rabbitmq.model.observation.micrometer.AmqpObservationDocumentation.LowCardinalityTags;
import io.micrometer.common.KeyValues;
import io.micrometer.common.util.StringUtils;

public class DefaultPublishObservationConvention implements PublishObservationConvention {

  private static final String OPERATION = "publish";
  private static final String OPERATION_SUFFIX = " " + OPERATION;

  @Override
  public String getName() {
    return "rabbitmq.amqp.publish";
  }

  @Override
  public String getContextualName(PublishContext context) {
    return exchange(context.exchange()) + OPERATION_SUFFIX;
  }

  private String exchange(String destination) {
    return StringUtils.isNotBlank(destination) ? destination : "amq.default";
  }

  @Override
  public KeyValues getLowCardinalityKeyValues(PublishContext context) {
    return KeyValues.of(
        LowCardinalityTags.MESSAGING_OPERATION.withValue(OPERATION),
        LowCardinalityTags.MESSAGING_SYSTEM.withValue("rabbitmq"),
        LowCardinalityTags.NET_PROTOCOL_NAME.withValue("amqp"),
        LowCardinalityTags.NET_PROTOCOL_VERSION.withValue("1.0"));
  }

  @Override
  public KeyValues getHighCardinalityKeyValues(PublishContext context) {
    KeyValues keyValues =
        KeyValues.of(
            HighCardinalityTags.MESSAGING_DESTINATION_NAME.withValue(context.exchange()),
            HighCardinalityTags.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES.withValue(
                String.valueOf(context.payloadSizeBytes())),
            HighCardinalityTags.NET_SOCK_PEER_ADDR.withValue(context.peerAddress()),
            HighCardinalityTags.NET_SOCK_PEER_PORT.withValue(String.valueOf(context.peerPort())));
    if (context.routingKey() != null) {
      keyValues =
          keyValues.and(HighCardinalityTags.MESSAGING_ROUTING_KEY.withValue(context.routingKey()));
    }
    if (context.messageId() != null) {
      keyValues =
          keyValues.and(HighCardinalityTags.MESSAGING_MESSAGE_ID.withValue(context.messageId()));
    }
    if (context.correlationId() != null) {
      keyValues =
          keyValues.and(
              HighCardinalityTags.MESSAGING_MESSAGE_CONVERSATION_ID.withValue(
                  context.correlationId()));
    }
    return keyValues;
  }
}
