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
package com.rabbitmq.model;

import java.math.BigDecimal;
import java.util.UUID;

public interface Message<T> {

  Message<T> body(T body);

  T body();

  // properties
  Object messageId();

  String messageIdAsString();

  long messageIdAsLong();

  byte[] messageIdAsBinary();

  UUID messageIdAsUuid();

  byte[] userId();

  String to();

  String subject();

  String replyTo();

  Message<T> messageId(String id);

  Message<T> messageId(long id);

  Message<T> messageId(byte[] id);

  Message<T> messageId(UUID id);

  Message<T> userId(byte[] userId);

  Message<T> to(String address);

  Message<T> subject(String subject);

  Message<T> replyTo(String replyTo);

  // TODO handle remaining properties

  Object property(String key);

  Message<T> property(String key, boolean value);

  Message<T> property(String key, byte value);

  Message<T> property(String key, short value);

  Message<T> property(String key, int value);

  Message<T> property(String key, long value);

  Message<T> propertyUnsigned(String key, byte value);

  Message<T> propertyUnsigned(String key, short value);

  Message<T> propertyUnsigned(String key, int value);

  Message<T> propertyUnsigned(String key, long value);

  Message<T> property(String key, float value);

  Message<T> property(String key, double value);

  Message<T> propertyDecimal32(String key, BigDecimal value);

  Message<T> propertyDecimal64(String key, BigDecimal value);

  Message<T> propertyDecimal128(String key, BigDecimal value);

  Message<T> property(String key, char value);

  Message<T> propertyTimestamp(String key, long value);

  Message<T> property(String key, UUID value);

  Message<T> property(String key, byte[] value);

  Message<T> property(String key, String value);

  Message<T> propertySymbol(String key, String value);

  boolean hasProperty(String key);

  boolean hasProperties();

  Object removeProperty(String key);

  // TODO support iteration over message application properties

  // TODO support message annotations
  // TODO support message headers

  MessageAddressBuilder<T> address();

  interface MessageAddressBuilder<T> extends AddressBuilder<MessageAddressBuilder<T>> {

    Message<T> message();
  }
}
