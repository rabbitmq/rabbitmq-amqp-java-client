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
package com.rabbitmq.client.amqp;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import java.util.function.BiConsumer;

public interface Message {

  // properties
  Object messageId();

  String messageIdAsString();

  long messageIdAsLong();

  byte[] messageIdAsBinary();

  UUID messageIdAsUuid();

  Object correlationId();

  String correlationIdAsString();

  long correlationIdAsLong();

  byte[] correlationIdAsBinary();

  UUID correlationIdAsUuid();

  byte[] userId();

  String to();

  String subject();

  String replyTo();

  Message messageId(Object id);

  Message messageId(String id);

  Message messageId(long id);

  Message messageId(byte[] id);

  Message messageId(UUID id);

  Message correlationId(Object correlationId);

  Message correlationId(String correlationId);

  Message correlationId(long correlationId);

  Message correlationId(byte[] correlationId);

  Message correlationId(UUID correlationId);

  Message userId(byte[] userId);

  Message to(String address);

  Message subject(String subject);

  Message replyTo(String replyTo);

  Message contentType(String contentType);

  Message contentEncoding(String contentEncoding);

  Message absoluteExpiryTime(long absoluteExpiryTime);

  Message creationTime(long creationTime);

  Message groupId(String groupID);

  Message groupSequence(int groupSequence);

  Message replyToGroupId(String groupId);

  String contentType();

  String contentEncoding();

  long absoluteExpiryTime();

  long creationTime();

  String groupId();

  int groupSequence();

  String replyToGroupId();

  // application properties
  Object property(String key);

  Message property(String key, boolean value);

  Message property(String key, byte value);

  Message property(String key, short value);

  Message property(String key, int value);

  Message property(String key, long value);

  Message propertyUnsigned(String key, byte value);

  Message propertyUnsigned(String key, short value);

  Message propertyUnsigned(String key, int value);

  Message propertyUnsigned(String key, long value);

  Message property(String key, float value);

  Message property(String key, double value);

  Message propertyDecimal32(String key, BigDecimal value);

  Message propertyDecimal64(String key, BigDecimal value);

  Message propertyDecimal128(String key, BigDecimal value);

  Message property(String key, char value);

  Message propertyTimestamp(String key, long value);

  Message property(String key, UUID value);

  Message property(String key, byte[] value);

  Message property(String key, String value);

  Message propertySymbol(String key, String value);

  boolean hasProperty(String key);

  boolean hasProperties();

  Object removeProperty(String key);

  Message forEachProperty(BiConsumer<String, Object> action);

  // application data
  Message body(byte[] body);

  byte[] body();

  // header section
  boolean durable();

  Message priority(byte priority);

  byte priority();

  Message ttl(Duration ttl);

  Duration ttl();

  boolean firstAcquirer();

  // message annotations
  Object annotation(String key);

  Message annotation(String key, Object value);

  boolean hasAnnotation(String key);

  boolean hasAnnotations();

  Object removeAnnotation(String key);

  Message forEachAnnotation(BiConsumer<String, Object> action);

  MessageAddressBuilder toAddress();

  MessageAddressBuilder replyToAddress();

  interface MessageAddressBuilder extends AddressBuilder<MessageAddressBuilder> {

    Message message();
  }
}
