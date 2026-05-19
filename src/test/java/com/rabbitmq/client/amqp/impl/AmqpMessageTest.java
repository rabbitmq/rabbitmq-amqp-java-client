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

import static com.rabbitmq.client.amqp.impl.Tuples.triple;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.impl.Tuples.Triple;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.client.impl.ClientMessageSupport;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.junit.jupiter.api.Test;

public class AmqpMessageTest {

  private static AmqpMessage msg() {
    return new AmqpMessage();
  }

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

  @Test
  void bodyShouldHandleAmqpValueWithString() throws Exception {
    String payload = "hello world";
    Encoder encoder = CodecFactory.getDefaultEncoder();
    try (ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(256)) {
      encoder.writeObject(buffer, encoder.newEncoderState(), new AmqpValue<>(payload));
      org.apache.qpid.protonj2.client.Message<?> decoded =
          ClientMessageSupport.decodeMessage(buffer, null);
      AmqpMessage message = new AmqpMessage(decoded);
      assertThat(message.body()).isEqualTo(payload.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  void bodyConverterShouldExposeFirstBodySectionValue() throws Exception {
    Encoder encoder = CodecFactory.getDefaultEncoder();

    List<Triple<Object, Message.Converter<?, ?>, Object>> testCases =
        List.of(
            // Supported types by default converter
            triple(new AmqpValue<>("hello world"), Object::toString, "hello world"),

            // AmqpValue with various AMQP 1.0 types that should fail with default converter
            triple(new AmqpValue<>(42), (Integer i) -> i.toString(), "42"),
            triple(new AmqpValue<>(3.14159), (Double d) -> String.format("%.5f", d), "3.14159"),
            triple(new AmqpValue<>(true), (Boolean b) -> b ? "TRUE" : "FALSE", "TRUE"),
            triple(new AmqpValue<>(true), (Boolean b) -> b, Boolean.TRUE),
            triple(
                new AmqpValue<>(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")),
                (UUID uuid) -> uuid.toString().toUpperCase(),
                "123E4567-E89B-12D3-A456-426614174000"),
            triple(
                new AmqpValue<>(1640995200000L),
                (Long timestamp) -> Long.toString(timestamp),
                "1640995200000"),
            triple(
                new AmqpValue<>(Symbol.getSymbol("test-symbol")),
                (Symbol symbol) -> symbol.toString(),
                "test-symbol"),
            triple(
                new AmqpValue<>(new UnsignedByte((byte) 255)),
                (UnsignedByte ub) -> ub.toString(),
                "255"),
            triple(
                new AmqpValue<>(new UnsignedShort((short) 32767)),
                (UnsignedShort us) -> us.toString(),
                "32767"),
            triple(
                new AmqpValue<>(new UnsignedInteger(2147483647)),
                (UnsignedInteger ui) -> ui.toString(),
                "2147483647"),
            triple(
                new AmqpValue<>(UnsignedLong.valueOf("18446744073709551615")),
                (UnsignedLong ul) -> ul.toString(),
                "18446744073709551615"),
            triple(new AmqpValue<>('X'), (Character c) -> c.toString(), "X"),
            triple(
                new AmqpValue<>(Arrays.asList("item1", "item2", "item3")),
                (List<?> list) -> String.join(",", (CharSequence[]) list.toArray(new String[0])),
                "item1,item2,item3"),
            triple(
                new AmqpValue<>(Map.of("key1", "value1", "key2", "value2")),
                (Map<?, ?> map) -> "map with " + map.size() + " entries",
                "map with 2 entries"),

            // AmqpSequence with various content types
            triple(
                new AmqpSequence<>(Arrays.asList("seq1", "seq2", "seq3")),
                (List<?> seq) -> String.join("|", (CharSequence[]) seq.toArray(new String[0])),
                "seq1|seq2|seq3"),
            triple(
                new AmqpSequence<>(Arrays.asList(1, 2, 3, 4, 5)),
                (List<?> seq) -> seq.stream().mapToInt(i -> (Integer) i).sum() + "",
                "15"),
            triple(
                new AmqpSequence<>(Arrays.asList(Map.of("name", "Alice"), Map.of("name", "Bob"))),
                (List<?> seq) -> seq.size() + " items",
                "2 items"),

            // Data sections (single)
            triple(
                new Data(new byte[] {1, 2, 3, 4, 5}),
                (byte[] data) -> Arrays.toString(data),
                "[1, 2, 3, 4, 5]"),
            triple(
                new Data("test data".getBytes(StandardCharsets.UTF_8)),
                (byte[] data) -> new String(data, StandardCharsets.UTF_8),
                "test data"));

    for (Triple<Object, Message.Converter<?, ?>, Object> testCase : testCases) {
      try (ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(256)) {
        Object body = testCase.v1();
        Message.Converter<?, ?> converter = testCase.v2();
        Object expected = testCase.v3();
        encoder.writeObject(buffer, encoder.newEncoderState(), body);
        org.apache.qpid.protonj2.client.Message<?> decoded =
            ClientMessageSupport.decodeMessage(buffer, null);
        AmqpMessage message = new AmqpMessage(decoded);
        assertThat(message.body(converter)).isEqualTo(expected);
      }
    }
  }

  @Test
  void defaultConverterShouldFailOnUnsupportedTypes() throws Exception {
    Encoder encoder = CodecFactory.getDefaultEncoder();

    // Test various AMQP 1.0 types that should fail with the default converter
    Object[] unsupportedPayloads = {
      new AmqpValue<>(42),
      new AmqpValue<>(3.14159),
      new AmqpValue<>(true),
      new AmqpValue<>(UUID.randomUUID()),
      new AmqpValue<>(System.currentTimeMillis()),
      new AmqpValue<>(Symbol.getSymbol("test")),
      new AmqpValue<>(new UnsignedByte((byte) 255)),
      new AmqpValue<>(new UnsignedShort((short) 32767)),
      new AmqpValue<>(new UnsignedInteger(2147483647)),
      new AmqpValue<>(UnsignedLong.MAX_VALUE),
      new AmqpValue<>('X'),
      new AmqpValue<>(Arrays.asList("item1", "item2")),
      new AmqpValue<>(Map.of("key", "value")),
      new AmqpSequence<>(Arrays.asList("seq1", "seq2")),
      new AmqpSequence<>(Arrays.asList(1, 2, 3))
    };

    for (Object payload : unsupportedPayloads) {
      try (ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(256)) {
        encoder.writeObject(buffer, encoder.newEncoderState(), payload);
        org.apache.qpid.protonj2.client.Message<?> decoded =
            ClientMessageSupport.decodeMessage(buffer, null);
        AmqpMessage message = new AmqpMessage(decoded);

        assertThatThrownBy(message::body)
            .isInstanceOf(AmqpException.class)
            .hasMessageContaining("Unsupported body type:");
      }
    }
  }

  @Test
  void messageWithMultiSectionsShouldGetOnlyFirstSection() throws Exception {
    // Create a message with multiple Data sections
    org.apache.qpid.protonj2.client.Message<?> nativeMessage =
        org.apache.qpid.protonj2.client.Message.create();

    nativeMessage.toAdvancedMessage().clearBodySections();
    nativeMessage
        .toAdvancedMessage()
        .addBodySection(new Data("First ".getBytes(StandardCharsets.UTF_8)));
    nativeMessage
        .toAdvancedMessage()
        .addBodySection(new Data("Second ".getBytes(StandardCharsets.UTF_8)));
    nativeMessage
        .toAdvancedMessage()
        .addBodySection(new Data("Third".getBytes(StandardCharsets.UTF_8)));

    AmqpMessage message = new AmqpMessage(nativeMessage);

    // The default body() method should only return the first section
    assertThat(message.body()).isEqualTo("First ".getBytes(StandardCharsets.UTF_8));

    // Access all sections via converter
    assertThat(
            message.body(
                (Object body) -> {
                  // body(Converter) delegates to the underlying single-body view and therefore
                  // observes only the first body section when multiple sections are present.
                  return new String((byte[]) body, StandardCharsets.UTF_8);
                }))
        .isEqualTo("First ");
  }

  @Test
  void multipleAmqpSequenceSectionsShouldGetOnlyFirstSequence() throws Exception {
    Encoder encoder = CodecFactory.getDefaultEncoder();

    try (ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(1024)) {
      // Encode multiple AmqpSequence sections
      encoder.writeObject(
          buffer,
          encoder.newEncoderState(),
          new AmqpSequence<>(Arrays.asList("first", "sequence")));
      encoder.writeObject(
          buffer,
          encoder.newEncoderState(),
          new AmqpSequence<>(Arrays.asList("second", "sequence")));

      org.apache.qpid.protonj2.client.Message<?> decoded =
          ClientMessageSupport.decodeMessage(buffer, null);
      AmqpMessage message = new AmqpMessage(decoded);

      // Access the first sequence
      assertThat(message.body((List<?> seq) -> seq.size())).isEqualTo(2);
      assertThat(message.body((List<?> seq) -> seq.get(0))).isEqualTo("first");
    }
  }

  @Test
  void bodySectionsConverterShouldExposeAllBodySections() throws Exception {
    Encoder encoder = CodecFactory.getDefaultEncoder();

    List<Triple<Object, Message.SectionsConverter<?, ?>, Object>> testCases =
        List.of(
            // Single Data section
            triple(
                new Data("single data".getBytes(StandardCharsets.UTF_8)),
                (List<byte[]> sections) -> sections.size() + " data section(s)",
                "1 data section(s)"),

            // Multiple Data sections
            triple(
                Arrays.asList(
                    new Data("first ".getBytes(StandardCharsets.UTF_8)),
                    new Data("second ".getBytes(StandardCharsets.UTF_8)),
                    new Data("third".getBytes(StandardCharsets.UTF_8))),
                (List<byte[]> sections) ->
                    sections.stream()
                        .map(data -> new String(data, StandardCharsets.UTF_8))
                        .collect(java.util.stream.Collectors.joining()),
                "first second third"),

            // Single AmqpSequence section with strings
            triple(
                new AmqpSequence<>(Arrays.asList("hello", "world", "test")),
                (List<List<String>> sections) -> sections.get(0).size() + " items in sequence",
                "3 items in sequence"),

            // Multiple AmqpSequence sections
            triple(
                Arrays.asList(
                    new AmqpSequence<>(Arrays.asList("seq1", "item1")),
                    new AmqpSequence<>(Arrays.asList("seq2", "item2", "item3"))),
                (List<List<String>> sections) ->
                    sections.stream().mapToInt(List::size).sum()
                        + " total items across "
                        + sections.size()
                        + " sequences",
                "5 total items across 2 sequences"),

            // Single AmqpValue with String
            triple(
                new AmqpValue<>("hello from amqp value"),
                (List<String> sections) -> "AmqpValue: " + sections.get(0),
                "AmqpValue: hello from amqp value"),

            // Single AmqpValue with byte array
            triple(
                new AmqpValue<>(new Binary("binary data".getBytes(StandardCharsets.UTF_8))),
                (List<Binary> sections) -> "Binary length: " + sections.get(0).getLength(),
                "Binary length: 11"));

    for (Triple<Object, Message.SectionsConverter<?, ?>, Object> testCase : testCases) {
      try (ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(1024)) {
        Object payload = testCase.v1();
        Message.SectionsConverter<?, ?> converter = testCase.v2();
        Object expected = testCase.v3();

        if (payload instanceof java.util.List) {
          // Multiple sections case
          @SuppressWarnings("unchecked")
          List<Object> sections = (List<Object>) payload;
          for (Object section : sections) {
            encoder.writeObject(buffer, encoder.newEncoderState(), section);
          }
        } else {
          // Single section case
          encoder.writeObject(buffer, encoder.newEncoderState(), payload);
        }

        org.apache.qpid.protonj2.client.Message<?> decoded =
            ClientMessageSupport.decodeMessage(buffer, null);
        AmqpMessage message = new AmqpMessage(decoded);
        assertThat(message.body(converter)).isEqualTo(expected);
      }
    }
  }

  @Test
  void bodySectionsConverterShouldReceiveEmptyListWhenMessageHasNoBodySections() throws Exception {
    org.apache.qpid.protonj2.client.Message<?> nativeMessage =
        org.apache.qpid.protonj2.client.Message.create();

    nativeMessage.toAdvancedMessage().clearBodySections();

    AmqpMessage message = new AmqpMessage(nativeMessage);

    assertThat(message.body((List<Object> sections) -> sections)).isEmpty();
  }

  @Test
  void bodyConverterShouldReceiveNullWhenMessageHasNoBody() {
    AmqpMessage message = new AmqpMessage(org.apache.qpid.protonj2.client.Message.create());

    assertThat(message.body((Object body) -> body)).isNull();
  }
}
