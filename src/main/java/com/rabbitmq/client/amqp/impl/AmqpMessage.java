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

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Message;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Decimal128;
import org.apache.qpid.protonj2.types.Decimal32;
import org.apache.qpid.protonj2.types.Decimal64;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;

final class AmqpMessage implements Message {

  private static final byte[] EMPTY_BODY = new byte[0];

  private final ClientMessage<?> delegate;

  private boolean durableIsSet = false;

  AmqpMessage() {
    this(EMPTY_BODY);
  }

  AmqpMessage(byte[] body) {
    this(ClientMessage.create(new Data(body)));
  }

  AmqpMessage(org.apache.qpid.protonj2.client.Message<?> delegate) {
    // ClientMessage is the only implementation and its accessors do not throw ClientException
    this.delegate = (ClientMessage<?>) delegate;
  }

  // properties

  @Override
  public Object messageId() {
    return this.delegate.messageId();
  }

  @Override
  public String messageIdAsString() {
    return (String) this.delegate.messageId();
  }

  @Override
  public long messageIdAsLong() {
    return ((UnsignedLong) this.delegate.messageId()).longValue();
  }

  @Override
  public byte[] messageIdAsBinary() {
    return ((Binary) this.delegate.messageId()).asByteArray();
  }

  @Override
  public UUID messageIdAsUuid() {
    return (UUID) this.delegate.messageId();
  }

  @Override
  public Object correlationId() {
    return this.delegate.correlationId();
  }

  @Override
  public String correlationIdAsString() {
    return (String) this.delegate.correlationId();
  }

  @Override
  public long correlationIdAsLong() {
    return ((UnsignedLong) this.delegate.correlationId()).longValue();
  }

  @Override
  public byte[] correlationIdAsBinary() {
    return ((Binary) this.delegate.correlationId()).asByteArray();
  }

  @Override
  public UUID correlationIdAsUuid() {
    return (UUID) this.delegate.correlationId();
  }

  @Override
  public byte[] userId() {
    return this.delegate.userId();
  }

  @Override
  public String to() {
    return this.delegate.to();
  }

  @Override
  public String subject() {
    return this.delegate.subject();
  }

  @Override
  public String replyTo() {
    return this.delegate.replyTo();
  }

  @Override
  public Message messageId(Object id) {
    this.delegate.messageId(id);
    return this;
  }

  @Override
  public Message messageId(String id) {
    this.delegate.messageId(id);
    return this;
  }

  @Override
  public Message messageId(long id) {
    this.delegate.messageId(new UnsignedLong(id));
    return this;
  }

  @Override
  public Message messageId(byte[] id) {
    this.delegate.messageId(new Binary(id));
    return this;
  }

  @Override
  public Message messageId(UUID id) {
    this.delegate.messageId(id);
    return this;
  }

  @Override
  public Message correlationId(Object correlationId) {
    this.delegate.correlationId(correlationId);
    return this;
  }

  @Override
  public Message correlationId(String correlationId) {
    this.delegate.correlationId(correlationId);
    return this;
  }

  @Override
  public Message correlationId(long correlationId) {
    this.delegate.correlationId(UnsignedLong.valueOf(correlationId));
    return this;
  }

  @Override
  public Message correlationId(byte[] correlationId) {
    this.delegate.correlationId(new Binary(correlationId));
    return this;
  }

  @Override
  public Message correlationId(UUID correlationId) {
    this.delegate.correlationId(correlationId);
    return this;
  }

  @Override
  public Message userId(byte[] userId) {
    this.delegate.userId(userId);
    return this;
  }

  @Override
  public Message to(String address) {
    this.delegate.to(address);
    return this;
  }

  @Override
  public Message subject(String subject) {
    this.delegate.subject(subject);
    return this;
  }

  @Override
  public Message replyTo(String replyTo) {
    this.delegate.replyTo(replyTo);
    return this;
  }

  @Override
  public Message contentType(String contentType) {
    this.delegate.contentType(contentType);
    return this;
  }

  @Override
  public Message contentEncoding(String contentEncoding) {
    this.delegate.contentEncoding(contentEncoding);
    return this;
  }

  @Override
  public Message absoluteExpiryTime(long absoluteExpiryTime) {
    this.delegate.absoluteExpiryTime(absoluteExpiryTime);
    return this;
  }

  @Override
  public Message creationTime(long creationTime) {
    this.delegate.creationTime(creationTime);
    return this;
  }

  @Override
  public Message groupId(String groupID) {
    this.delegate.groupId(groupID);
    return this;
  }

  @Override
  public Message groupSequence(int groupSequence) {
    this.delegate.groupSequence(groupSequence);
    return this;
  }

  @Override
  public Message replyToGroupId(String groupId) {
    this.delegate.replyToGroupId(groupId);
    return this;
  }

  @Override
  public String contentType() {
    return this.delegate.contentType();
  }

  @Override
  public String contentEncoding() {
    return this.delegate.contentEncoding();
  }

  @Override
  public long absoluteExpiryTime() {
    return this.delegate.absoluteExpiryTime();
  }

  @Override
  public long creationTime() {
    return this.delegate.creationTime();
  }

  @Override
  public String groupId() {
    return this.delegate.groupId();
  }

  @Override
  public int groupSequence() {
    return this.delegate.groupSequence();
  }

  @Override
  public boolean hasGroupSequence() {
    return this.delegate.hasGroupSequence();
  }

  @Override
  public String replyToGroupId() {
    return this.delegate.replyToGroupId();
  }

  // application properties

  @Override
  public Object property(String key) {
    Object value = this.delegate.property(key);
    if (value instanceof Binary) {
      return ((Binary) value).asByteArray();
    } else {
      return value;
    }
  }

  @Override
  public Message property(String key, boolean value) {
    this.delegate.property(key, value);
    return this;
  }

  @Override
  public Message property(String key, byte value) {
    this.delegate.property(key, value);
    return this;
  }

  @Override
  public Message property(String key, short value) {
    this.delegate.property(key, value);
    return this;
  }

  @Override
  public Message property(String key, int value) {
    this.delegate.property(key, value);
    return this;
  }

  @Override
  public Message property(String key, long value) {
    this.delegate.property(key, value);
    return this;
  }

  @Override
  public Message propertyUnsigned(String key, byte value) {
    this.delegate.property(key, new UnsignedByte(value));
    return this;
  }

  @Override
  public Message propertyUnsigned(String key, short value) {
    this.delegate.property(key, new UnsignedShort(value));
    return this;
  }

  @Override
  public Message propertyUnsigned(String key, int value) {
    this.delegate.property(key, new UnsignedInteger(value));
    return this;
  }

  @Override
  public Message propertyUnsigned(String key, long value) {
    this.delegate.property(key, new UnsignedLong(value));
    return this;
  }

  @Override
  public Message property(String key, float value) {
    this.delegate.property(key, value);
    return this;
  }

  @Override
  public Message property(String key, double value) {
    this.delegate.property(key, value);
    return this;
  }

  @Override
  public Message propertyDecimal32(String key, BigDecimal value) {
    this.delegate.property(key, new Decimal32(value));
    return this;
  }

  @Override
  public Message propertyDecimal64(String key, BigDecimal value) {
    this.delegate.property(key, new Decimal64(value));
    return this;
  }

  @Override
  public Message propertyDecimal128(String key, BigDecimal value) {
    this.delegate.property(key, new Decimal128(value));
    return this;
  }

  @Override
  public Message property(String key, char value) {
    this.delegate.property(key, value);
    return this;
  }

  @Override
  public Message propertyTimestamp(String key, long value) {
    this.delegate.property(key, new Date(value));
    return this;
  }

  @Override
  public Message property(String key, UUID value) {
    this.delegate.property(key, value);
    return this;
  }

  @Override
  public Message property(String key, byte[] value) {
    this.delegate.property(key, new Binary(value));
    return this;
  }

  @Override
  public Message property(String key, String value) {
    this.delegate.property(key, value);
    return this;
  }

  @Override
  public Message propertySymbol(String key, String value) {
    this.delegate.property(key, Symbol.getSymbol(value));
    return this;
  }

  @Override
  public boolean hasProperty(String key) {
    return this.delegate.hasProperty(key);
  }

  @Override
  public boolean hasProperties() {
    return this.delegate.hasProperties();
  }

  @Override
  public Object removeProperty(String key) {
    return this.delegate.removeProperty(key);
  }

  @Override
  public Message forEachProperty(BiConsumer<String, Object> action) {
    this.delegate.forEachProperty(action);
    return this;
  }

  // application data

  @Override
  public Message body(byte[] body) {
    this.delegate.clearBodySections();
    this.delegate.addBodySection(new Data(body));
    return this;
  }

  private static final Message.Converter<Object, byte[]> DEFAULT_CONVERTER =
      value -> {
        if (value == null) {
          return null;
        } else if (value instanceof byte[]) {
          return (byte[]) value;
        } else if (value instanceof Binary) {
          return ((Binary) value).asByteArray();
        } else if (value instanceof String) {
          return ((String) value).getBytes(StandardCharsets.UTF_8);
        } else {
          throw new AmqpException("Unsupported body type: " + value.getClass().getName());
        }
      };

  @Override
  public byte[] body() {
    return DEFAULT_CONVERTER.convert(this.delegate.body());
  }

  @Override
  public <I, O> O body(Converter<I, O> converter) {
    @SuppressWarnings("unchecked")
    I value = (I) this.delegate.body();
    return converter.convert(value);
  }

  @Override
  public <I, O> O body(SectionsConverter<I, O> converter) {
    Collection<Section<?>> sections = this.delegate.bodySections();
    List<I> sectionValues = new ArrayList<>(sections.size());

    for (Section<?> section : sections) {
      @SuppressWarnings("unchecked")
      I value = (I) section.getValue();
      sectionValues.add(value);
    }

    return converter.convert(sectionValues);
  }

  // header section

  @Override
  public Message durable(boolean durable) {
    this.durableIsSet = true;
    this.delegate.durable(durable);
    return this;
  }

  @Override
  public boolean durable() {
    return this.delegate.durable();
  }

  @Override
  public long deliveryCount() {
    return this.delegate.deliveryCount();
  }

  @Override
  public Message priority(byte priority) {
    this.delegate.priority(priority);
    return this;
  }

  @Override
  public byte priority() {
    return this.delegate.priority();
  }

  @Override
  public Message ttl(Duration ttl) {
    if (ttl == null) {
      throw new IllegalArgumentException("TTL cannot be null");
    }
    this.delegate.timeToLive(ttl.toMillis());
    return this;
  }

  @Override
  public Duration ttl() {
    return Duration.ofMillis(this.delegate.timeToLive());
  }

  @Override
  public boolean firstAcquirer() {
    return this.delegate.firstAcquirer();
  }

  // message annotations

  @Override
  public Object annotation(String key) {
    return this.delegate.annotation(key);
  }

  @Override
  public Message annotation(String key, Object value) {
    Utils.validateMessageAnnotationKey(key);
    this.delegate.annotation(key, value);
    return this;
  }

  @Override
  public boolean hasAnnotation(String key) {
    return this.delegate.hasAnnotation(key);
  }

  @Override
  public boolean hasAnnotations() {
    return this.delegate.hasAnnotations();
  }

  @Override
  public Object removeAnnotation(String key) {
    return this.delegate.removeAnnotation(key);
  }

  @Override
  public Message forEachAnnotation(BiConsumer<String, Object> action) {
    this.delegate.forEachAnnotation(action);
    return this;
  }

  @Override
  public MessageAddressBuilder toAddress() {
    return new DefaultMessageAddressBuilder(this, DefaultMessageAddressBuilder.TO_CALLBACK);
  }

  @Override
  public MessageAddressBuilder replyToAddress() {
    return new DefaultMessageAddressBuilder(this, DefaultMessageAddressBuilder.REPLY_TO_CALLBACK);
  }

  AmqpMessage enforceDurability() {
    if (!this.durableIsSet) {
      this.delegate.durable(true);
    }
    return this;
  }

  private static class DefaultMessageAddressBuilder
      extends DefaultAddressBuilder<MessageAddressBuilder> implements MessageAddressBuilder {

    private static final BiConsumer<Message, String> TO_CALLBACK = Message::to;
    private static final BiConsumer<Message, String> REPLY_TO_CALLBACK = Message::replyTo;

    private final Message message;
    private final BiConsumer<Message, String> buildCallback;

    private DefaultMessageAddressBuilder(
        Message message, BiConsumer<Message, String> buildCallback) {
      super(null);
      this.message = message;
      this.buildCallback = buildCallback;
    }

    @Override
    MessageAddressBuilder result() {
      return this;
    }

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public Message message() {
      this.buildCallback.accept(this.message, this.address());
      return this.message;
    }
  }

  ClientMessage<?> nativeMessage() {
    return this.delegate;
  }
}
