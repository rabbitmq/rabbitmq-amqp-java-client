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
import java.util.List;
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
   * Get the message body as bytes.
   *
   * <p>This is a convenience API for byte-oriented payload access. It is intended for message
   * bodies that can be represented as a byte array, such as Data sections and compatible AmqpValue
   * payloads (for example String values converted to UTF-8 bytes).
   *
   * <p>For AMQP body types that are not naturally byte-oriented, such as AmqpSequence or typed
   * AmqpValue payloads, use {@link #body(Converter)} or {@link #body(SectionsConverter)}.
   *
   * @return the message body as bytes
   * @throws AmqpException if the body cannot be represented as bytes
   */
  byte[] body();

  /**
   * Get the message body converted using the specified converter.
   *
   * <p>This method provides access to the message body content in its native AMQP 1.0 format,
   * allowing applications to handle non-binary payloads and complex message body structures.
   * According to the <a
   * href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format">AMQP
   * 1.0 message format specification</a>, a message body can contain various section types
   * including AmqpValue, AmqpSequence, and Data sections.
   *
   * <p>This method extracts the value from the <strong>first body section only</strong> and passes
   * it to the converter. For messages with multiple body sections, use {@link
   * #body(SectionsConverter)} instead.
   *
   * <p>The converter input type must match the runtime type of the value exposed by the underlying
   * AMQP implementation for the first body section. For example, applications may receive {@code
   * byte[]}, {@code String}, {@code List<?>}, or implementation-specific AMQP types such as {@code
   * Binary}.
   *
   * <p>If the converter expects a type that does not match the runtime body value type, a {@link
   * ClassCastException} may be thrown by the converter invocation.
   *
   * <p><strong>Implementation Note:</strong> This method exposes values coming from the underlying
   * AMQP implementation (currently Qpid Proton-J2). Applications using this API should treat these
   * values as implementation-coupled runtime types rather than stable library-defined model types.
   * Changes in the underlying implementation may affect the concrete runtime types observed by
   * applications using this API.
   *
   * @param <I> the input type expected by the converter (the type of the body section value)
   * @param <O> the output type returned by the converter
   * @param converter the converter to transform the body section value
   * @return the converted body value
   * @throws AmqpException if an error occurs while accessing the message body
   * @see #body(SectionsConverter) for multi-section message bodies
   * @see #body() for simple binary body access
   */
  <I, O> O body(Converter<I, O> converter);

  /**
   * Get the message body with access to all body sections using the specified sections converter.
   *
   * <p>This method provides complete access to multi-section message bodies as defined in the <a
   * href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format">AMQP
   * 1.0 message format specification</a>. AMQP 1.0 messages can contain multiple body sections of
   * the same type:
   *
   * <ul>
   *   <li><strong>Multiple Data sections</strong> - Each containing binary data, typically
   *       concatenated logically
   *   <li><strong>Multiple AmqpSequence sections</strong> - Each containing a list of structured
   *       values
   *   <li><strong>Single AmqpValue section</strong> - Containing a single AMQP value of any type
   * </ul>
   *
   * <p>The converter receives a list of all section values from the message body, allowing
   * applications to process complex multi-section payloads that cannot be handled by the
   * single-section {@link #body(Converter)} method.
   *
   * <p>The converter input type must match the runtime type of each section value exposed by the
   * underlying AMQP implementation. All body sections in a valid AMQP message are expected to be of
   * the same section kind, but applications should still treat the values as implementation exposed
   * runtime objects.
   *
   * <p>If the converter expects a type that does not match the runtime section value type, a {@link
   * ClassCastException} may be thrown by the converter invocation.
   *
   * <p><strong>Implementation Note:</strong> This method exposes values coming from the underlying
   * AMQP implementation (currently Qpid Proton-J2). Applications using this API should treat these
   * values as implementation-coupled runtime types rather than stable library-defined model types.
   * Changes in the underlying implementation may affect the concrete runtime types observed by
   * applications using this API.
   *
   * @param <I> the input type expected by the converter (the type of each body section value)
   * @param <O> the output type returned by the converter
   * @param converter the sections converter to transform all body section values
   * @return the converted body value from all sections
   * @throws AmqpException if an error occurs while accessing the message body sections
   * @see #body(Converter) for single-section message bodies
   * @see #body() for simple binary body access
   */
  <I, O> O body(SectionsConverter<I, O> converter);

  /**
   * Mark the message as durable or not.
   *
   * <p>Messages are durable by default, use <code>false</code> to make them explicitly non-durable.
   *
   * <p>Durability depends also on the queue messages end up in (e.g. quorum queues and streams
   * always store messages durably).
   *
   * @param durable true for a durable message, false for a non-durable message
   * @return the message
   */
  Message durable(boolean durable);

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

  /**
   * Functional interface for converting a single AMQP body section value.
   *
   * <p>This converter is used with {@link #body(Converter)} to transform the value from the first
   * body section of an AMQP 1.0 message into a desired output type. The input type {@code I}
   * corresponds to the native type of the body section value as exposed by the underlying AMQP
   * implementation.
   *
   * <p><strong>Common Input Types:</strong>
   *
   * <ul>
   *   <li>{@code byte[]} - for Data sections containing binary data
   *   <li>{@code String} - for AmqpValue sections containing strings
   *   <li>{@code List<?>} - for AmqpSequence sections containing lists
   *   <li>Various AMQP types - for AmqpValue sections with typed content (Integer, UUID, Symbol,
   *       etc.)
   * </ul>
   *
   * @param <I> the input type (body section value type from the AMQP implementation)
   * @param <O> the output type (application-specific converted type)
   * @see #body(Converter)
   */
  @FunctionalInterface
  interface Converter<I, O> {

    /**
     * Convert the given AMQP body section value to the desired output type.
     *
     * @param value the body section value from the AMQP message
     * @return the converted value
     */
    O convert(I value);
  }

  /**
   * Functional interface for converting multiple AMQP body section values.
   *
   * <p>This converter is used with {@link #body(SectionsConverter)} to process all body sections
   * from an AMQP 1.0 message. According to the AMQP 1.0 specification, a message body can contain
   * multiple sections of the same type, and this converter provides access to all of them.
   *
   * <p><strong>Multi-Section Scenarios:</strong>
   *
   * <ul>
   *   <li><strong>Multiple Data sections</strong> - List of {@code byte[]} arrays, typically
   *       concatenated
   *   <li><strong>Multiple AmqpSequence sections</strong> - List of {@code List<?>} objects
   *   <li><strong>Single AmqpValue section</strong> - List with one element of the AmqpValue type
   * </ul>
   *
   * <p>The converter receives all section values as a list, allowing applications to aggregate,
   * concatenate, or otherwise process complex multi-section message bodies that exceed the
   * capabilities of single-section processing.
   *
   * @param <I> the input type (body section value type from the AMQP implementation)
   * @param <O> the output type (application-specific converted type)
   * @see #body(SectionsConverter)
   */
  @FunctionalInterface
  interface SectionsConverter<I, O> {
    /**
     * Convert the given list of AMQP body section values to the desired output type.
     *
     * <p>The list contains all body sections from the message in their original order. For Data
     * sections, this typically means multiple {@code byte[]} arrays. For AmqpSequence sections,
     * this means multiple {@code List<?>} objects. For AmqpValue sections, this typically contains
     * a single element.
     *
     * @param sections the list of body section values from the AMQP message
     * @return the converted value
     */
    O convert(List<I> sections);
  }
}
