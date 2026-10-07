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

import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.Arrays;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.buffer.impl.ProtonCompositeBufferImpl;
import org.apache.qpid.protonj2.buffer.netty.Netty4ProtonBufferAllocator;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.client.impl.ClientMessageSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ProtonCompositeBufferTest {

  // a frame read from several socket reads is a composite buffer, the payload is a copy of it
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void copyStartingInAChunkOtherThanTheFirstShouldCopyTheRequestedBytes(boolean readOnly) {
    Netty4ProtonBufferAllocator allocator =
        new Netty4ProtonBufferAllocator(UnpooledByteBufAllocator.DEFAULT);
    ProtonCompositeBuffer composite = new ProtonCompositeBufferImpl(allocator);
    byte[] content = new byte[39];
    for (int i = 0; i < content.length; i++) {
      content[i] = (byte) i;
    }
    composite.append(allocator.wrap(Unpooled.wrappedBuffer(content, 0, 10)));
    composite.append(allocator.wrap(Unpooled.wrappedBuffer(content, 10, 20)));
    composite.append(allocator.wrap(Unpooled.wrappedBuffer(content, 30, 9)));
    if (readOnly) {
      composite.convertToReadOnly();
    }

    // starts in the second chunk and ends 3 bytes before its end
    ProtonBuffer copy = composite.copy(15, 12, readOnly);

    assertThat(copy.getReadableBytes()).isEqualTo(12);
    byte[] copied = new byte[12];
    copy.copyInto(0, copied, 0, 12);
    assertThat(copied).isEqualTo(Arrays.copyOfRange(content, 15, 27));
  }

  // a small message read in 3 socket reads or more
  @Test
  void messageSplitInThreeChunksShouldBeDecoded() throws Exception {
    Netty4ProtonBufferAllocator allocator =
        new Netty4ProtonBufferAllocator(UnpooledByteBufAllocator.DEFAULT);
    ClientMessage<byte[]> message = ClientMessage.create();
    message.body("18575".getBytes(UTF_8));
    message.annotation("x-stream-offset", 18575L);
    message.annotation("x-stream-chunk-id", 18000L);
    ProtonBuffer encoded = ClientMessageSupport.encodeMessage(message, null);
    byte[] bytes = new byte[encoded.getReadableBytes()];
    encoded.copyInto(encoded.getReadOffset(), bytes, 0, bytes.length);

    for (int first = 1; first < bytes.length - 1; first++) {
      for (int second = first + 1; second < bytes.length; second++) {
        ProtonCompositeBuffer composite = new ProtonCompositeBufferImpl(allocator);
        composite.append(allocator.wrap(Unpooled.wrappedBuffer(bytes, 0, first)));
        composite.append(allocator.wrap(Unpooled.wrappedBuffer(bytes, first, second - first)));
        composite.append(
            allocator.wrap(Unpooled.wrappedBuffer(bytes, second, bytes.length - second)));
        composite.convertToReadOnly();

        Message<?> decoded = ClientMessageSupport.decodeMessage(composite, null);

        assertThat((byte[]) decoded.body())
            .as("split at %d and %d", first, second)
            .isEqualTo("18575".getBytes(UTF_8));
        assertThat(decoded.annotation("x-stream-offset")).isEqualTo(18575L);
      }
    }
  }
}
