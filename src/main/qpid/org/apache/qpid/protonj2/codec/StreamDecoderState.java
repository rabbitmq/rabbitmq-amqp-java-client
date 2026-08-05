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

import java.io.InputStream;

/**
 * Retains state of the {@link InputStream} based decode either between calls or across decode iterations
 */
public interface StreamDecoderState {

    // Default value applied to stream decoder max count limits for various encoding to be read from
    // from an input stream such as max string size, max list size etc.
    public static final int DEFAULT_MAX_ALLOCATION_LIMIT = Integer.MAX_VALUE - 8;

    /**
     * Resets any intermediate state back to default values.
     *
     * @return this {@link StreamDecoderState} instance.
     */
    StreamDecoderState reset();

    /**
     * @return the {@link StreamDecoder} that created this state object
     */
    StreamDecoder getDecoder();

    /**
     * Given a stream that will provide UTF-8 encoded bytes, decode and return the String that
     * represents that UTF-8 value.
     *
     * @param stream
     *      A stream from which the UTF-8 encoded bytes are to be decoded.
     * @param length
     *      The number of bytes in the passed {@link InputStream} that comprise the UTF-8 encoding.
     *
     * @return a String that represents the UTF-8 decoded bytes.
     *
     * @throws DecodeException if an error occurs while decoding the string value.
     */
    String decodeUTF8(InputStream stream, int length) throws DecodeException;

    /**
     * Sets the configured maximum depth that nested types such as Lists, Maps and Arrays
     * can have before a {@link DecodeException} is thrown to allow the decoder to errors
     * in cases where the depth of encoding exceeds that the environment can support.
     *
     * @param limit
     * 		The configured limit on decoding types with nested element structures.
     *
     * @return this {@link StreamDecoderState} instance for chaining.
     *
     * @throws DecodeException if a set decoding depth limit is exceeded.
     */
    default StreamDecoderState setDepthLimit(int limit) {
        return this;
    }

    /**
     * Gets the configured maximum depth that nested types such as Lists, Maps and Arrays
     * can have before a {@link DecodeException} is thrown to allow the decoder to errors
     * in cases where the depth of encoding exceeds that the environment can support.
     *
     * @return the configured maximum depth of nested types.
     */
    default int getDepthLimit() {
        return Integer.MAX_VALUE;
    }

    /**
     * During decode of AMQP types which can be comprised of a nesting of other AMQP types
     * the depth is increased to track the amount of type nesting that comprises the type
     * being decoded. Implementations can use this value to impose limits on the depth of
     * nested objects within complex types and throw an {@link DecodeException} if that
     * depth value is exceeded.
     *
     * @return this {@link StreamDecoderState} instance for chaining.
     *
     * @throws DecodeException if a set decoding depth limit is exceeded.
     */
    default StreamDecoderState increaseDepth() throws DecodeException {
        return this;
    }

    /**
     * During decode as on level of complex type decode completes the level value is decreased
     *
     * @return this {@link StreamDecoderState} instance for chaining.
     */
    default StreamDecoderState decreaseDepth() {
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
     * @return this {@link StreamDecoderState} instance for chaining.
     */
    default StreamDecoderState setMaxZeroWidthArrayElements(int maxElements) {
        throw new UnsupportedOperationException("Default implementation cannot set a limit");
    }

    /**
     * Gets the configured maximum size of an encoded string this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded string size supported.
     */
    default int getMaxStringSize() {
        return DEFAULT_MAX_ALLOCATION_LIMIT;
    }

    /**
     * Sets the configured maximum size of an encoded string this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecoderState} instance for chaining.
     */
    default StreamDecoderState setMaxStringSize(int maximumSize) {
        throw new UnsupportedOperationException("Default implementation cannot set a limit");
    }

    /**
     * Gets the configured maximum size of an encoded array this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded array size supported.
     */
    default int getMaxArraySize() {
        return DEFAULT_MAX_ALLOCATION_LIMIT;
    }

    /**
     * Sets the configured maximum size of an encoded array this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecoderState} instance for chaining.
     */
    default StreamDecoderState setMaxArraySize(int maximumSize) {
        throw new UnsupportedOperationException("Default implementation cannot set a limit");
    }

    /**
     * Gets the configured maximum indicated encoded size of a List this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded list elements supported.
     */
    default int getMaxListSize() {
        return DEFAULT_MAX_ALLOCATION_LIMIT;
    }

    /**
     * Sets the configured maximum indicated encoded size of a List this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecoderState} instance for chaining.
     */
    default StreamDecoderState setMaxListSize(int maximumSize) {
        throw new UnsupportedOperationException("Default implementation cannot set a limit");
    }

    /**
     * Gets the configured maximum indicated encoded size of a List this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded list size supported.
     */
    default int getMaxMapSize() {
        return DEFAULT_MAX_ALLOCATION_LIMIT;
    }

    /**
     * Sets the configured maximum indicated encoded size of a Map this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecoderState} instance for chaining.
     */
    default StreamDecoderState setMaxMapSize(int maximumSize) {
        throw new UnsupportedOperationException("Default implementation cannot set a limit");
    }

    /**
     * Gets the configured maximum length of an encoded Binary this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded binary length supported.
     */
    default int getMaxBinarySize() {
        return DEFAULT_MAX_ALLOCATION_LIMIT;
    }

    /**
     * Gets the configured maximum size of an encoded Binary this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecoderState} instance for chaining.
     */
    default StreamDecoderState setMaxBinarySize(int maximumSize) {
        throw new UnsupportedOperationException("Default implementation cannot set a limit");
    }

    /**
     * Gets the configured maximum size of an encoded Symbol this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded Symbol length supported.
     */
    default int getMaxSymbolSize() {
        return DEFAULT_MAX_ALLOCATION_LIMIT;
    }

    /**
     * Gets the configured maximum length of an encoded Symbol this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecoderState} instance for chaining.
     */
    default StreamDecoderState setMaxSymbolSize(int maximumSize) {
        throw new UnsupportedOperationException("Default implementation cannot set a limit");
    }

}
