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
package com.rabbitmq.model.amqp;

import com.rabbitmq.model.Message;
import com.rabbitmq.model.ModelException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.*;

class AmqpMessage implements Message {

  private final org.apache.qpid.protonj2.client.Message<byte[]> delegate;

  AmqpMessage() {
    this(org.apache.qpid.protonj2.client.Message.create());
  }

  AmqpMessage(org.apache.qpid.protonj2.client.Message<byte[]> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Message addData(byte[] data) {
    callOnDelegate(m -> m.body(data));
    return this;
  }

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

  private void callOnDelegate(CallableConsumer call) {
    try {
      call.accept(this.delegate);
    } catch (ClientException e) {
      throw new ModelException(e);
    }
  }

  private <T> T returnFromDelegate(MessageFunctionCallable<T> call) {
    try {
      return call.call(this.delegate);
    } catch (ClientException e) {
      throw new ModelException(e);
    }
  }

  private interface CallableConsumer {

    void accept(org.apache.qpid.protonj2.client.Message<byte[]> message) throws ClientException;
  }

  private interface MessageFunctionCallable<T> {

    T call(org.apache.qpid.protonj2.client.Message<?> message) throws ClientException;
  }

  org.apache.qpid.protonj2.client.Message<?> nativeMessage() {
    return this.delegate;
  }
}
