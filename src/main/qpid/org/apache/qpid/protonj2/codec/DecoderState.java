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
package org.apache.qpid.protonj2.codec;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;

/**
 * Retains state of decode either between calls or across decode iterations
 */
public interface DecoderState {

    /**
     * Resets any intermediate state back to default values.
     *
     * @return this {@link DecoderState} instance.
     */
    DecoderState reset();

    /**
     * @return the decoder that created this state object
     */
    Decoder getDecoder();

    /**
     * Given a set of UTF-8 encoded bytes decode and return the String that
     * represents that UTF-8 value.
     *
     * @param buffer
     *      A buffer containing the UTF-8 encoded bytes to be decoded.
     * @param length
     *      The number of bytes in the passed buffer that comprise the UTF-8 encoding.
     *
     * @return a String that represents the UTF-8 decoded bytes.
     *
     * @throws DecodeException if an error occurs while decoding the string value.
     */
    String decodeUTF8(ProtonBuffer buffer, int length) throws DecodeException;

    /**
     * Sets the configured maximum depth that nested types such as Lists, Maps and Arrays
     * can have before a {@link DecodeException} is thrown to allow the decoder to error
     * in cases where the depth of encoding exceeds what the environment is thought to be
     * able to support.
     *
     * @param limit
     * 		The configured limit on decoding types with nested element structures.
     */
    default DecoderState setDepthLimit(int limit) {
        return this;
    }

    /**
     * Gets the configured maximum depth that nested types such as Lists, Maps and Arrays
     * can have before a {@link DecodeException} is thrown.
     *
     * @return the configured maximum depth of nested types.
     */
    default int getDepthLimit() {
        return Integer.MAX_VALUE;
    }

    /**
     * During decode of AMQP types which can be comprised of a nesting of other AMQP types
     * the such as Lists, Maps and Arrays, the depth is increased to track the amount of
     * type nesting that comprises the type being decoded. Implementations can use this
     * value to impose limits on the depth of nested objects within complex types and throw
     * an {@link DecodeException} if that depth value is reached.
     *
     * @return this {@link DecoderState} instance for chaining.
     *
     * @throws DecodeException if a set decoding depth limit is exceeded.
     */
    default DecoderState increaseDepth() throws DecodeException {
        return this;
    }

    /**
     * Called once decoding of one level of a nested type completes to reduce to the previous
     * level before proceeding to the next element on the current level if any.
     *
     * @return this {@link DecoderState} instance for chaining.
     */
    default DecoderState decreaseDepth() {
        return this;
    }

    /**
     * Gets the configured maximum number of elements that can be decoded from an array
     * encoded with the zero width AMQP types (Null, UInt0. ULong0, List0, Boolean_False
     * and Boolean_True). A return value of zero indicates that the decoder should allow
     * any zero sized arrays of zero width types and throw for all others (the new default).
     *
     * @return the configured max number of zero width array encoding elements.
     */
    default int getMaxZeroWidthArrayElements() {
        return 0;
    }

    /**
     * Sets the configured maximum number of elements that can be decoded from an array
     * encoded with the zero width AMQP types (Null, UInt0. ULong0, List0, Boolean_False
     * and Boolean_True). These are uncommon encodings and can lead to small encodings
     * with large memory costs at decode which makes them discouraged for normal use.
     * <p>
     * It is recommended that for implementations that implement this limit configuration
     * the default be zero meaning zero width array encodings are disabled and will always
     * throw a {@link DecodeException}
     *
     * @param maxElements
     * 		The configured max elements allowed in arrays encoded for zero width types.
     *
     * @return this {@link DecoderState} instance for chaining.
     */
    default DecoderState setMaxZeroWidthArrayElements(int maxElements) {
        throw new UnsupportedOperationException("Default implementation cannot set a limit");
    }
}
