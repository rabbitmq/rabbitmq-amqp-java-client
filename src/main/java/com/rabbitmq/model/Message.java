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

public interface Message {

  // TODO support the possible body types
  Message addData(byte[] data);

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

  Message messageId(String id);

  Message messageId(long id);

  Message messageId(byte[] id);

  Message messageId(UUID id);

  Message userId(byte[] userId);

  Message to(String address);

  Message subject(String subject);

  Message replyTo(String replyTo);

  // TODO handle remaining properties

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

  // TODO support iteration over message application properties

  // TODO support message annotations
  // TODO support message headers

  AddressBuilder address();

  interface AddressBuilder {

    AddressBuilder exchange(String exchange);

    AddressBuilder key(String key);

    Message message();
  }
}
