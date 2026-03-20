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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.client.impl.ClientMessageSupport;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.junit.jupiter.api.Test;

public class AmqpMessageTest {

  @Test
  void toShouldBePathEncoded() {
    assertThat(msg().toAddress().exchange("foo bar").message().to())
        .isEqualTo("/exchanges/foo%20bar");
  }

  @Test
  void replyToShouldBePathEncoded() {
    assertThat(msg().replyToAddress().exchange("foo bar").message().replyTo())
        .isEqualTo("/exchanges/foo%20bar");
  }

  @Test
  void shouldBeNonDurableOnlyIfExplicitlySet() throws Exception {
    AmqpMessage msg = msg();
    // durable by default
    assertThat(msg.enforceDurability().nativeMessage().durable()).isTrue();
    // non-durable explicitly set
    msg = msg();
    msg.durable(false);
    assertThat(msg.enforceDurability().nativeMessage().durable()).isFalse();
    // durable explicitly set
    msg = msg();
    msg.durable(true);
    assertThat(msg.enforceDurability().nativeMessage().durable()).isTrue();
  }

  @Test
  void bodyShouldHandleAmqpValueWithBinary() throws Exception {
    byte[] payload = {1, 2, 3};
    Encoder encoder = CodecFactory.getDefaultEncoder();
    try (ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(256)) {
      encoder.writeObject(buffer, encoder.newEncoderState(), new AmqpValue<>(new Binary(payload)));
      org.apache.qpid.protonj2.client.Message<?> decoded =
          ClientMessageSupport.decodeMessage(buffer, null);
      AmqpMessage message = new AmqpMessage(decoded);
      assertThat(message.body()).isEqualTo(payload);
    }
  }

  private static AmqpMessage msg() {
    return new AmqpMessage();
  }
}
