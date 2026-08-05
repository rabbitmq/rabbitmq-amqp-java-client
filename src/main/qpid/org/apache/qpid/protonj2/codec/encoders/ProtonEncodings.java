/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.codec.encoders;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Static method that perform some of the most common type encodings
 * that have well defined formats that don't need further lookups or
 * null checks etc.
 */
public final class ProtonEncodings {

    private ProtonEncodings() {}

    /**
     * Write the byte value that carries the unsigned integer using the most compact
     * AMQP encoding possible based on the value of the integer provided.
     *
     * @param buffer
     * 		The buffer where the encoding should be written to
     * @param value
     * 		The unsigned integer value to write carried in a java primitive
     */
    public static void writeUnsignedInteger(ProtonBuffer buffer, byte value) {
        if (value == 0) {
            buffer.writeByte(EncodingCodes.UINT0);
        } else {
            buffer.writeByte(EncodingCodes.SMALLUINT);
            buffer.writeByte(value);
        }
    }

    /**
     * Write the integer value that carries the unsigned integer using the most compact
     * AMQP encoding possible based on the value of the integer provided.
     *
     * @param buffer
     * 		The buffer where the encoding should be written to
     * @param value
     * 		The unsigned integer value to write carried in a java primitive
     */
    public static void writeUnsignedInteger(ProtonBuffer buffer, int value) {
        if (value == 0) {
            buffer.writeByte(EncodingCodes.UINT0);
        } else if (value > 0 && value <= 255) {
            buffer.writeByte(EncodingCodes.SMALLUINT);
            buffer.writeByte((byte) value);
        } else {
            buffer.writeByte(EncodingCodes.UINT);
            buffer.writeInt(value);
        }
    }

    /**
     * Write the long value that carries the unsigned integer using the most compact
     * AMQP encoding possible based on the value of the long provided. The value must
     * has already been checked for overflow by the object calling this method.
     *
     * @param buffer
     * 		The buffer where the encoding should be written to
     * @param value
     * 		The unsigned integer value to write carried in a java primitive
     */
    public static void writeUnsignedInteger(ProtonBuffer buffer, long value) {
        if (value == 0) {
            buffer.writeByte(EncodingCodes.UINT0);
        } else if (value > 0 && value <= 255) {
            buffer.writeByte(EncodingCodes.SMALLUINT);
            buffer.writeByte((byte) value);
        } else {
            buffer.writeByte(EncodingCodes.UINT);
            buffer.writeInt((int) value);
        }
    }

    /**
     * Write the byte value that carries the unsigned long value using the most compact
     * AMQP encoding possible based on the value of the long.
     *
     * @param buffer
     * 		The buffer where the encoding should be written to
     * @param value
     * 		The unsigned long value to write carried in a java primitive
     */
    public static void writeUnsignedLong(ProtonBuffer buffer, byte value) {
        if (value == 0) {
            buffer.writeByte(EncodingCodes.ULONG0);
        } else {
            buffer.writeByte(EncodingCodes.SMALLULONG);
            buffer.writeByte(value);
        }
    }

    /**
     * Write the long value that carries the unsigned long value using the most compact
     * AMQP encoding possible based on the value of the long.
     *
     * @param buffer
     * 		The buffer where the encoding should be written to
     * @param value
     * 		The unsigned long value to write carried in a java primitive
     */
    public static void writeUnsignedLong(ProtonBuffer buffer, long value) {
        if (value == 0) {
            buffer.writeByte(EncodingCodes.ULONG0);
        } else if (value > 0 && value <= 255) {
            buffer.writeByte(EncodingCodes.SMALLULONG);
            buffer.writeByte((byte) value);
        } else {
            buffer.writeByte(EncodingCodes.ULONG);
            buffer.writeLong(value);
        }
    }

    /**
     * Write the Symbol value into to provided buffer using the most compact AMQP
     * encoding possible based on the size of the Symbol contents.
     *
     * @param buffer
     * 		The buffer where the encoding should be written to
     * @param value
     * 		The Symbol value carried in the given String to write
     */
    public static void writeSymbol(ProtonBuffer buffer, String value) {
        final int symbolBytes = value.length();

        if (symbolBytes <= 255) {
            buffer.writeByte(EncodingCodes.SYM8);
            buffer.writeByte((byte) symbolBytes);
        } else {
            buffer.writeByte(EncodingCodes.SYM32);
            buffer.writeInt(symbolBytes);
        }

        buffer.writeBytes(value.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Write the Symbol value into to provided buffer using the most compact AMQP
     * encoding possible based on the size of the Symbol contents.
     *
     * @param buffer
     * 		The buffer where the encoding should be written to
     * @param value
     * 		The Symbol value to write into the buffer
     */
    public static void writeSymbol(ProtonBuffer buffer, Symbol value) {
        final int symbolBytes = value.getLength();

        if (symbolBytes <= 255) {
            buffer.writeByte(EncodingCodes.SYM8);
            buffer.writeByte((byte) symbolBytes);
        } else {
            buffer.writeByte(EncodingCodes.SYM32);
            buffer.writeInt(symbolBytes);
        }

        value.writeTo(buffer);
    }

    /**
     * Write the String value into to provided buffer using the most compact AMQP
     * encoding possible based on the size of the String contents.
     *
     * @param buffer
     * 		The buffer where the encoding should be written to
     * @param value
     * 		The String value to write into the buffer
     */
    public static void writeString(ProtonBuffer buffer, EncoderState state, String value) {
        // We are pessimistic and assume larger strings will encode
        // at the max 4 bytes per character instead of calculating
        if (value.length() > 64) {
            buffer.writeByte(EncodingCodes.STR32);
            writeLargeString(buffer, state, value);
        } else {
            buffer.writeByte(EncodingCodes.STR8);
            writeSmallString(buffer, state, value);
        }
    }

    private static void writeSmallString(ProtonBuffer buffer, EncoderState state, String value) {
        buffer.writeByte((byte) 0);

        int startIndex = buffer.getWriteOffset();

        // Write the full string value
        state.encodeUTF8(buffer, value);

        // Move back and write the size into the size slot
        buffer.setByte(startIndex - Byte.BYTES, (byte) (buffer.getWriteOffset() - startIndex));
    }

    private static void writeLargeString(ProtonBuffer buffer, EncoderState state, String value) {
        buffer.writeInt(0);

        int startIndex = buffer.getWriteOffset();

        // Write the full string value
        state.encodeUTF8(buffer, value);

        // Move back and write the size into the size slot
        buffer.setInt(startIndex - Integer.BYTES, buffer.getWriteOffset() - startIndex);
    }

    /**
     * Write the Binary value into to provided buffer using the most compact AMQP
     * encoding possible based on the size of the buffer contents.
     *
     * @param buffer
     * 		The buffer where the encoding should be written to
     * @param value
     * 		The ProtonBuffer value to write into the buffer as a Binary encoding
     */
    public static void writeBinary(ProtonBuffer buffer, ProtonBuffer value) {
        if (value.getReadableBytes() > 255) {
            buffer.ensureWritable(value.getReadableBytes() + Long.BYTES);
            buffer.writeByte(EncodingCodes.VBIN32);
            buffer.writeInt(value.getReadableBytes());
        } else {
            buffer.ensureWritable(value.getReadableBytes() + Short.BYTES);
            buffer.writeByte(EncodingCodes.VBIN8);
            buffer.writeByte((byte) value.getReadableBytes());
        }

        value.copyInto(value.getReadOffset(), buffer, buffer.getWriteOffset(), value.getReadableBytes());
        buffer.advanceWriteOffset(value.getReadableBytes());
    }

    /**
     * Write the DeliveryTag bytes value into to provided buffer using the most compact AMQP
     * encoding possible based on the size of the tag buffer contents.
     *
     * @param buffer
     * 		The buffer where the encoding should be written to
     * @param value
     * 		The DeliveryTag value to write into the buffer as a Binary encoding
     */
    public static void writeDeliveryTag(ProtonBuffer buffer, DeliveryTag value) {
        final int tagLength = value.tagLength();

        if (tagLength > 255) {
            buffer.writeByte(EncodingCodes.VBIN32);
            buffer.writeInt(tagLength);
        } else {
            buffer.writeByte(EncodingCodes.VBIN8);
            buffer.writeByte((byte) tagLength);
        }

        value.writeTo(buffer);
    }
}
