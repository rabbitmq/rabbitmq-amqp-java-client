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

/**
 * AMQP 1.0 message.
 *
 * @see <a
 *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format">AMQP
 *     1.0 Message Format</a>
 */
public interface Message {

  // properties

  /**
   * Get message ID.
   *
   * @return the message ID
   */
  Object messageId();

  /**
   * Get message ID as a string.
   *
   * @return the message ID as a string
   */
  String messageIdAsString();

  /**
   * Get message ID as a long.
   *
   * @return the message ID as a long
   */
  long messageIdAsLong();

  /**
   * Get message ID as an array of bytes.
   *
   * @return the message ID as an array of bytes
   */
  byte[] messageIdAsBinary();

  /**
   * Get message ID as a UUID.
   *
   * @return the message ID as a UUID
   */
  UUID messageIdAsUuid();

  /**
   * Get message correlation ID.
   *
   * @return the message correlation ID
   */
  Object correlationId();

  /**
   * Get message correlation ID as a string.
   *
   * @return the message correlation ID as a string
   */
  String correlationIdAsString();

  /**
   * Get message correlation ID as a long.
   *
   * @return the message correlation ID as a long
   */
  long correlationIdAsLong();

  /**
   * Get message correlation ID as an array of bytes.
   *
   * @return the message correlation ID as an array of bytes
   */
  byte[] correlationIdAsBinary();

  /**
   * Get message correlation ID as a UUID.
   *
   * @return the message correlation ID as a UUID
   */
  UUID correlationIdAsUuid();

  /**
   * Get user ID
   *
   * @return the user ID
   */
  byte[] userId();

  /**
   * Get the to field.
   *
   * @return the to field
   */
  String to();

  /**
   * Get the subject.
   *
   * @return the subject
   */
  String subject();

  /**
   * Get the reply-to field.
   *
   * @return the reply-to field
   */
  String replyTo();

  /**
   * Set the message ID.
   *
   * @param id ID
   * @return the message
   */
  Message messageId(Object id);

  /**
   * Set the message ID (string).
   *
   * @param id message ID
   * @return the message
   */
  Message messageId(String id);

  /**
   * Set the message ID (long).
   *
   * @param id message ID
   * @return the message
   */
  Message messageId(long id);

  /**
   * Set the message ID (array of bytes).
   *
   * @param id message ID
   * @return the message
   */
  Message messageId(byte[] id);

  /**
   * Set the message ID (UUID).
   *
   * @param id message ID
   * @return the message
   */
  Message messageId(UUID id);

  /**
   * Set the message correlation ID.
   *
   * @param correlationId correlation ID
   * @return the message
   */
  Message correlationId(Object correlationId);

  /**
   * Set the message correlation ID (string).
   *
   * @param correlationId correlation ID
   * @return the message
   */
  Message correlationId(String correlationId);

  /**
   * Set the message correlation ID (long).
   *
   * @param correlationId correlation ID
   * @return the message
   */
  Message correlationId(long correlationId);

  /**
   * Set the message correlation ID (array of bytes).
   *
   * @param correlationId correlation ID
   * @return the message
   */
  Message correlationId(byte[] correlationId);

  /**
   * Set the message correlation ID (UUID).
   *
   * @param correlationId correlation ID
   * @return the message
   */
  Message correlationId(UUID correlationId);

  /**
   * Set the user ID.
   *
   * @param userId user ID
   * @return the message
   */
  Message userId(byte[] userId);

  /**
   * Set the to field.
   *
   * <p>Prefer using {@link #toAddress()} to build the target address.
   *
   * @param address to address
   * @return the message
   * @see #toAddress()
   */
  Message to(String address);

  /**
   * Set the message subject.
   *
   * @param subject message subject
   * @return the message
   */
  Message subject(String subject);

  /**
   * Set the reply-to field.
   *
   * <p>Prefer using {@link #replyToAddress()} to build the reply-to address.
   *
   * @param replyTo reply-to field
   * @return the message
   * @see #replyToAddress()
   */
  Message replyTo(String replyTo);

  /**
   * Set the content-type.
   *
   * @param contentType content-type
   * @return the message
   */
  Message contentType(String contentType);

  /**
   * Set the content-encoding.
   *
   * @param contentEncoding content-encoding
   * @return the message
   */
  Message contentEncoding(String contentEncoding);

  /**
   * Set the expiry time.
   *
   * @param absoluteExpiryTime expiry time
   * @return the message
   */
  Message absoluteExpiryTime(long absoluteExpiryTime);

  /**
   * Set the creation time.
   *
   * @param creationTime creation time
   * @return the message
   */
  Message creationTime(long creationTime);

  /**
   * Set the group ID.
   *
   * @param groupID group ID
   * @return the message
   */
  Message groupId(String groupID);

  /**
   * Set the position of the message in its group.
   *
   * @param groupSequence group sequence
   * @return the message
   */
  Message groupSequence(int groupSequence);

  /**
   * Set the reply-to group ID.
   *
   * @param groupId reply-to group ID
   * @return the message
   */
  Message replyToGroupId(String groupId);

  /**
   * Get the content-type.
   *
   * @return the content-type
   */
  String contentType();

  /**
   * Get the content-encoding.
   *
   * @return the content-encoding
   */
  String contentEncoding();

  /**
   * Get the expiry time.
   *
   * @return the expiry time
   */
  long absoluteExpiryTime();

  /**
   * Get the creation time.
   *
   * @return the creation time
   */
  long creationTime();

  /**
   * Get the group ID.
   *
   * @return the group ID
   */
  String groupId();

  /**
   * Get the message position in its group.
   *
   * @return the group sequence
   */
  int groupSequence();

  /**
   * Get the reply-to group ID.
   *
   * @return the reply-to group ID
   */
  String replyToGroupId();

  /**
   * Get the value of an application property.
   *
   * @param key property key
   * @return
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-application-properties">AMQP
   *     1.0 Message Application Properties</a>
   */
  Object property(String key);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, boolean value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, byte value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, short value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, int value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, long value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message propertyUnsigned(String key, byte value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message propertyUnsigned(String key, short value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message propertyUnsigned(String key, int value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message propertyUnsigned(String key, long value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, float value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, double value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message propertyDecimal32(String key, BigDecimal value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message propertyDecimal64(String key, BigDecimal value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message propertyDecimal128(String key, BigDecimal value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, char value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message propertyTimestamp(String key, long value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, UUID value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, byte[] value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message property(String key, String value);

  /**
   * Set an application property.
   *
   * @param key property key
   * @param value property value
   * @return the message
   */
  Message propertySymbol(String key, String value);

  /**
   * Check an application property is set.
   *
   * @param key property name
   * @return true if set, false otherwise
   */
  boolean hasProperty(String key);

  /**
   * Whether at least one application property is set on the message.
   *
   * @return true if the message has at least one application property, false otherwise
   */
  boolean hasProperties();

  /**
   * Remove an application property.
   *
   * @param key property key
   * @return the previous property value, if any
   */
  Object removeProperty(String key);

  /**
   * Iterate over application properties.
   *
   * @param action action to execute for each application property
   * @return the message
   */
  Message forEachProperty(BiConsumer<String, Object> action);

  /**
   * Set the body of the message.
   *
   * @param body message body
   * @return the message
   */
  Message body(byte[] body);

  /**
   * Get the message body.
   *
   * @return the message body
   */
  byte[] body();

  /**
   * Whether the message is durable.
   *
   * @return true if durable, false otherwise
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-header">AMQP
   *     1.0 Message Header</a>
   */
  boolean durable();

  /**
   * Set the priority of the message.
   *
   * @param priority message priority
   * @return the message
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-header">AMQP
   *     1.0 Message Header</a>
   */
  Message priority(byte priority);

  /**
   * Get the message priority.
   *
   * @return the message priority
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-header">AMQP
   *     1.0 Message Header</a>
   */
  byte priority();

  /**
   * Set the message TTL.
   *
   * @param ttl message TTL
   * @return the message
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-header">AMQP
   *     1.0 Message Header</a>
   */
  Message ttl(Duration ttl);

  /**
   * Get the message TTL.
   *
   * @return the message TTL
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-header">AMQP
   *     1.0 Message Header</a>
   */
  Duration ttl();

  /**
   * Whether the message may have been acquired by another link.
   *
   * @return true if the message has not been acquired by another link, false if it may have been
   *     already acquired by another link
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-header">AMQP
   *     1.0 Message Header</a>
   */
  boolean firstAcquirer();

  /**
   * Get the value of a message annotation.
   *
   * @param key annotation key
   * @return the annotation value
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-message-annotations">AMQP
   *     1.0 Message Annotations</a>
   */
  Object annotation(String key);

  /**
   * Set the value of a message annotation.
   *
   * @param key annotation key
   * @param value annotation value
   * @return the message
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-message-annotations">AMQP
   *     1.0 Message Annotations</a>
   */
  Message annotation(String key, Object value);

  /**
   * Whether a message annotation is set.
   *
   * @param key annotation key
   * @return true if the annotation is set, false otherwise
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-message-annotations">AMQP
   *     1.0 Message Annotations</a>
   */
  boolean hasAnnotation(String key);

  /**
   * Whether the message has message annotation.
   *
   * @return true if the message has annotations, false otherwise
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-message-annotations">AMQP
   *     1.0 Message Annotations</a>
   */
  boolean hasAnnotations();

  /**
   * Remove an annotation.
   *
   * @param key annotation key
   * @return the previous value of the annotation, if any
   * @see <a
   *     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-message-annotations">AMQP
   *     1.0 Message Annotations</a>
   */
  Object removeAnnotation(String key);

  /**
   * Iterate over message annotations.
   *
   * @param action action to execute for each message annotation
   * @return the message
   */
  Message forEachAnnotation(BiConsumer<String, Object> action);

  /**
   * {@link AddressBuilder} for the {@link #to()} field.
   *
   * @return the address builder
   */
  MessageAddressBuilder toAddress();

  /**
   * {@link AddressBuilder} for the {@link #replyTo()} field.
   *
   * @return the address builder
   */
  MessageAddressBuilder replyToAddress();

  /** Message {@link AddressBuilder}. */
  interface MessageAddressBuilder extends AddressBuilder<MessageAddressBuilder> {

    Message message();
  }
}
