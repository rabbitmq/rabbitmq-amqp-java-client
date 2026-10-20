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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.netty.Netty4ProtonBufferAllocator;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.client.impl.ClientMessageSupport;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.security.SaslInit;
import org.junit.jupiter.api.Test;

public class BinaryDecodingTest {

  // decoded binary values must not retain the (pooled) read buffer
  @Test
  void decodedMessageBinaryValuesShouldNotRetainSourceBuffer() throws Exception {
    ClientMessage<byte[]> message = ClientMessage.create();
    message.body("hello".getBytes(UTF_8));
    message.userId("guest".getBytes(UTF_8));
    message.messageId(new Binary("message-id".getBytes(UTF_8)));
    message.correlationId(new Binary("correlation-id".getBytes(UTF_8)));
    message.property("binary-property", new Binary("property".getBytes(UTF_8)));
    ProtonBuffer encoded = ClientMessageSupport.encodeMessage(message, null);

    ByteBuf source = pooledDirectCopy(encoded);
    Message<?> decoded;
    try (ProtonBuffer input = readOnlyWrap(source)) {
      decoded = ClientMessageSupport.decodeMessage(input, null);
    }

    assertThat(source.refCnt()).isZero();
    assertThat(decoded.userId()).isEqualTo("guest".getBytes(UTF_8));
    assertThat(decoded.messageId()).isEqualTo(new Binary("message-id".getBytes(UTF_8)));
    assertThat(decoded.correlationId()).isEqualTo(new Binary("correlation-id".getBytes(UTF_8)));
    assertThat(decoded.property("binary-property"))
        .isEqualTo(new Binary("property".getBytes(UTF_8)));
    assertThat((byte[]) decoded.body()).isEqualTo("hello".getBytes(UTF_8));
  }

  @Test
  void decodedSaslBinaryValuesShouldNotRetainSourceBuffer() throws Exception {
    SaslInit init = new SaslInit();
    init.setMechanism(Symbol.valueOf("PLAIN"));
    init.setInitialResponse(new Binary("\0guest\0guest".getBytes(UTF_8)));
    Encoder encoder = CodecFactory.getSaslEncoder();
    ProtonBuffer encoded = ProtonBufferAllocator.defaultAllocator().allocate();
    encoder.writeObject(encoded, encoder.newEncoderState(), init);

    ByteBuf source = pooledDirectCopy(encoded);
    SaslInit decoded;
    try (ProtonBuffer input = readOnlyWrap(source)) {
      Decoder decoder = CodecFactory.getSaslDecoder();
      decoded = (SaslInit) decoder.readObject(input, decoder.newDecoderState());
    }

    assertThat(source.refCnt()).isZero();
    assertThat(new Binary(decoded.getInitialResponse()))
        .isEqualTo(new Binary("\0guest\0guest".getBytes(UTF_8)));
  }

  private static ByteBuf pooledDirectCopy(ProtonBuffer buffer) {
    byte[] bytes = new byte[buffer.getReadableBytes()];
    buffer.copyInto(buffer.getReadOffset(), bytes, 0, bytes.length);
    ByteBuf source = PooledByteBufAllocator.DEFAULT.directBuffer(bytes.length);
    source.writeBytes(bytes);
    return source;
  }

  private static ProtonBuffer readOnlyWrap(ByteBuf source) {
    return new Netty4ProtonBufferAllocator(PooledByteBufAllocator.DEFAULT)
        .wrap(source)
        .convertToReadOnly();
  }
}
