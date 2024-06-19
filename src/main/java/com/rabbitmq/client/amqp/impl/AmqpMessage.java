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

import static com.rabbitmq.client.amqp.impl.ExceptionUtils.convert;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Message;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.*;

class AmqpMessage implements Message {

  private static final byte[] EMPTY_BODY = new byte[0];

  private final org.apache.qpid.protonj2.client.Message<byte[]> delegate;

  AmqpMessage() {
    this(org.apache.qpid.protonj2.client.Message.create(EMPTY_BODY));
  }

  AmqpMessage(byte[] body) {
    this(org.apache.qpid.protonj2.client.Message.create(body));
  }

  AmqpMessage(org.apache.qpid.protonj2.client.Message<byte[]> delegate) {
    this.delegate = delegate;
  }

  // properties

  @Override
  public Object messageId() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::messageId);
  }

  @Override
  public String messageIdAsString() {
    return returnFromDelegate(m -> (String) m.messageId());
  }

  @Override
  public long messageIdAsLong() {
    return returnFromDelegate(m -> ((UnsignedLong) m.messageId()).longValue());
  }

  @Override
  public byte[] messageIdAsBinary() {
    return returnFromDelegate(m -> ((Binary) m.messageId()).asByteArray());
  }

  @Override
  public UUID messageIdAsUuid() {
    return returnFromDelegate(m -> (UUID) m.messageId());
  }

  @Override
  public Object correlationId() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::correlationId);
  }

  @Override
  public String correlationIdAsString() {
    return returnFromDelegate(m -> (String) m.correlationId());
  }

  @Override
  public long correlationIdAsLong() {
    return returnFromDelegate(m -> ((UnsignedLong) m.correlationId())).longValue();
  }

  @Override
  public byte[] correlationIdAsBinary() {
    return returnFromDelegate(m -> ((Binary) m.correlationId()).asByteArray());
  }

  @Override
  public UUID correlationIdAsUuid() {
    return returnFromDelegate(m -> (UUID) m.correlationId());
  }

  @Override
  public byte[] userId() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::userId);
  }

  @Override
  public String to() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::to);
  }

  @Override
  public String subject() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::subject);
  }

  @Override
  public String replyTo() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::replyTo);
  }

  @Override
  public Message messageId(Object id) {
    callOnDelegate(m -> m.messageId(id));
    return this;
  }

  @Override
  public Message messageId(String id) {
    callOnDelegate(m -> m.messageId(id));
    return this;
  }

  @Override
  public Message messageId(long id) {
    callOnDelegate(m -> m.messageId(new UnsignedLong(id)));
    return this;
  }

  @Override
  public Message messageId(byte[] id) {
    callOnDelegate(m -> m.messageId(new Binary(id)));
    return this;
  }

  @Override
  public Message messageId(UUID id) {
    callOnDelegate(m -> m.messageId(id));
    return this;
  }

  @Override
  public Message correlationId(Object correlationId) {
    callOnDelegate(m -> m.correlationId(correlationId));
    return this;
  }

  @Override
  public Message correlationId(String correlationId) {
    callOnDelegate(m -> m.correlationId(correlationId));
    return this;
  }

  @Override
  public Message correlationId(long correlationId) {
    callOnDelegate(m -> m.correlationId(UnsignedLong.valueOf(correlationId)));
    return this;
  }

  @Override
  public Message correlationId(byte[] correlationId) {
    callOnDelegate(m -> m.correlationId(new Binary(correlationId)));
    return this;
  }

  @Override
  public Message correlationId(UUID correlationId) {
    callOnDelegate(m -> m.correlationId(correlationId));
    return this;
  }

  @Override
  public Message userId(byte[] userId) {
    callOnDelegate(m -> m.userId(userId));
    return this;
  }

  @Override
  public Message to(String address) {
    callOnDelegate(m -> m.to(address));
    return this;
  }

  @Override
  public Message subject(String subject) {
    callOnDelegate(m -> m.subject(subject));
    return this;
  }

  @Override
  public Message replyTo(String replyTo) {
    callOnDelegate(m -> m.replyTo(replyTo));
    return this;
  }

  @Override
  public Message contentType(String contentType) {
    callOnDelegate(m -> m.contentType(contentType));
    return this;
  }

  @Override
  public Message contentEncoding(String contentEncoding) {
    callOnDelegate(m -> m.contentEncoding(contentEncoding));
    return this;
  }

  @Override
  public Message absoluteExpiryTime(long absoluteExpiryTime) {
    callOnDelegate(m -> m.absoluteExpiryTime(absoluteExpiryTime));
    return this;
  }

  @Override
  public Message creationTime(long creationTime) {
    callOnDelegate(m -> m.creationTime(creationTime));
    return this;
  }

  @Override
  public Message groupId(String groupID) {
    callOnDelegate(m -> m.groupId(groupID));
    return this;
  }

  @Override
  public Message groupSequence(int groupSequence) {
    callOnDelegate(m -> m.groupSequence(groupSequence));
    return this;
  }

  @Override
  public Message replyToGroupId(String groupId) {
    callOnDelegate(m -> replyToGroupId(groupId));
    return this;
  }

  @Override
  public String contentType() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::contentType);
  }

  @Override
  public String contentEncoding() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::contentEncoding);
  }

  @Override
  public long absoluteExpiryTime() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::absoluteExpiryTime);
  }

  @Override
  public long creationTime() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::creationTime);
  }

  @Override
  public String groupId() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::groupId);
  }

  @Override
  public int groupSequence() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::groupSequence);
  }

  @Override
  public String replyToGroupId() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::replyToGroupId);
  }

  // application properties

  @Override
  public Object property(String key) {
    return returnFromDelegate(m -> m.property(key));
  }

  @Override
  public Message property(String key, boolean value) {
    callOnDelegate(m -> m.property(key, value));
    return this;
  }

  @Override
  public Message property(String key, byte value) {
    callOnDelegate(m -> m.property(key, value));
    return this;
  }

  @Override
  public Message property(String key, short value) {
    callOnDelegate(m -> m.property(key, value));
    return this;
  }

  @Override
  public Message property(String key, int value) {
    callOnDelegate(m -> m.property(key, value));
    return this;
  }

  @Override
  public Message property(String key, long value) {
    callOnDelegate(m -> m.property(key, value));
    return this;
  }

  @Override
  public Message propertyUnsigned(String key, byte value) {
    callOnDelegate(m -> m.property(key, new UnsignedByte(value)));
    return this;
  }

  @Override
  public Message propertyUnsigned(String key, short value) {
    callOnDelegate(m -> m.property(key, new UnsignedShort(value)));
    return this;
  }

  @Override
  public Message propertyUnsigned(String key, int value) {
    callOnDelegate(m -> m.property(key, new UnsignedInteger(value)));
    return this;
  }

  @Override
  public Message propertyUnsigned(String key, long value) {
    callOnDelegate(m -> m.property(key, new UnsignedLong(value)));
    return this;
  }

  @Override
  public Message property(String key, float value) {
    callOnDelegate(m -> m.property(key, value));
    return this;
  }

  @Override
  public Message property(String key, double value) {
    callOnDelegate(m -> m.property(key, value));
    return this;
  }

  @Override
  public Message propertyDecimal32(String key, BigDecimal value) {
    callOnDelegate(m -> m.property(key, new Decimal32(value)));
    return this;
  }

  @Override
  public Message propertyDecimal64(String key, BigDecimal value) {
    callOnDelegate(m -> m.property(key, new Decimal64(value)));
    return this;
  }

  @Override
  public Message propertyDecimal128(String key, BigDecimal value) {
    callOnDelegate(m -> m.property(key, new Decimal128(value)));
    return this;
  }

  @Override
  public Message property(String key, char value) {
    callOnDelegate(m -> m.property(key, value));
    return this;
  }

  @Override
  public Message propertyTimestamp(String key, long value) {
    callOnDelegate(m -> m.property(key, new Date(value)));
    return this;
  }

  @Override
  public Message property(String key, UUID value) {
    callOnDelegate(m -> m.property(key, value));
    return this;
  }

  @Override
  public Message property(String key, byte[] value) {
    callOnDelegate(m -> m.property(key, new Binary(value)));
    return this;
  }

  @Override
  public Message property(String key, String value) {
    callOnDelegate(m -> m.property(key, value));
    return this;
  }

  @Override
  public Message propertySymbol(String key, String value) {
    callOnDelegate(m -> m.property(key, Symbol.getSymbol(value)));
    return this;
  }

  @Override
  public boolean hasProperty(String key) {
    return returnFromDelegate(m -> m.hasProperty(key));
  }

  @Override
  public boolean hasProperties() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::hasProperties);
  }

  @Override
  public Object removeProperty(String key) {
    return returnFromDelegate(m -> m.removeProperty(key));
  }

  @Override
  public Message forEachProperty(BiConsumer<String, Object> action) {
    callOnDelegate(m -> m.forEachProperty(action));
    return this;
  }

  // application data

  @Override
  public Message body(byte[] body) {
    try {
      this.delegate.body(body);
    } catch (ClientException e) {
      throw convert(e);
    }
    return this;
  }

  @Override
  public byte[] body() {
    try {
      return this.delegate.body();
    } catch (ClientException e) {
      throw convert(e);
    }
  }

  // header section
  @Override
  public boolean durable() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::durable);
  }

  @Override
  public Message priority(byte priority) {
    callOnDelegate(m -> m.priority(priority));
    return this;
  }

  @Override
  public byte priority() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::priority);
  }

  @Override
  public Message ttl(Duration ttl) {
    if (ttl == null) {
      throw new IllegalArgumentException("TTL cannot be null");
    }
    callOnDelegate(m -> m.timeToLive(ttl.toMillis()));
    return this;
  }

  @Override
  public Duration ttl() {
    return returnFromDelegate(m -> Duration.ofMillis(m.timeToLive()));
  }

  @Override
  public boolean firstAcquirer() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::firstAcquirer);
  }

  // message annotations

  @Override
  public Object annotation(String key) {
    return returnFromDelegate(m -> m.annotation(key));
  }

  @Override
  public Message annotation(String key, Object value) {
    callOnDelegate(m -> m.annotation(key, value));
    return this;
  }

  @Override
  public boolean hasAnnotation(String key) {
    return returnFromDelegate(m -> m.hasAnnotation(key));
  }

  @Override
  public boolean hasAnnotations() {
    return returnFromDelegate(org.apache.qpid.protonj2.client.Message::hasAnnotations);
  }

  @Override
  public Object removeAnnotation(String key) {
    return returnFromDelegate(m -> m.removeAnnotation(key));
  }

  @Override
  public Message forEachAnnotation(BiConsumer<String, Object> action) {
    callOnDelegate(m -> m.forEachAnnotation(action));
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
    public Message message() {
      this.buildCallback.accept(this.message, this.address());
      return this.message;
    }
  }

  private void callOnDelegate(CallableConsumer call) {
    try {
      call.accept(this.delegate);
    } catch (ClientException e) {
      throw convert(e);
    }
  }

  private <E> E returnFromDelegate(MessageFunctionCallable<E> call) {
    try {
      return call.call(this.delegate);
    } catch (ClientException e) {
      throw new AmqpException(e);
    }
  }

  private interface CallableConsumer {

    void accept(org.apache.qpid.protonj2.client.Message<?> message) throws ClientException;
  }

  private interface MessageFunctionCallable<T> {

    T call(org.apache.qpid.protonj2.client.Message<?> message) throws ClientException;
  }

  org.apache.qpid.protonj2.client.Message<?> nativeMessage() {
    return this.delegate;
  }
}
