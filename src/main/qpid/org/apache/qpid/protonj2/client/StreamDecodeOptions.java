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
package org.apache.qpid.protonj2.client;

import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamDecoderState;

/**
 * Configuration options that can be passed to the {@link StreamDelivery} APIs for
 * reading messages and delivery annotations to give access to certain options
 * that can influence certain behaviors of the AMQP decoder that reads the bytes
 * associated with the incoming delivery.
 */
public final class StreamDecodeOptions {

    private int maxDepthLimit = ProtonStreamDecoderState.DEFAULT_MAX_DECODE_DEPTH;
    private int maxZeroWidthArrayElements = ProtonStreamDecoderState.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS;
    private int maxStringLength = ProtonStreamDecoderState.DEFAULT_MAX_ALLOCATION_LIMIT;
    private int maxArrayLength = ProtonStreamDecoderState.DEFAULT_MAX_ALLOCATION_LIMIT;
    private int maxBinaryLength = ProtonStreamDecoderState.DEFAULT_MAX_ALLOCATION_LIMIT;
    private int maxSymbolLength = ProtonStreamDecoderState.DEFAULT_MAX_ALLOCATION_LIMIT;
    private int maxListSize = ProtonStreamDecoderState.DEFAULT_MAX_ALLOCATION_LIMIT;
    private int maxMapSize = ProtonStreamDecoderState.DEFAULT_MAX_ALLOCATION_LIMIT;

    /**
     * Creates a default {@link StreamDecodeOptions} instance configured with the client
     * defied defaults for all values.
     *
     * @return a new {@link StreamDecodeOptions} instance configured with defaults.
     */
    public static StreamDecodeOptions defaultOptions() {
        return new StreamDecodeOptions();
    }

    /**
     * Gets the currently configured decode depth limit that will be enforced when
     * decoding AMQP types, if the encoding nests deeper than the configured limit
     * an exception will be thrown.
     *
     * @return the currently configured decode depth limit
     */
    public int depthLimit() {
        return maxDepthLimit;
    }

    /**
     * Sets the configured maximum depth that nested types such as Lists, Maps and Arrays
     * can have before a {@link DecodeException} is thrown to allow the decoder to error
     * in cases where the depth of encoding exceeds what the environment is thought to be
     * able to support.
     *
     * @param depthLimit
     * 		The configured limit on decoding types with nested element structures.
     *
     * @throws DecodeException if a set decoding depth limit is exceeded.
     */
    public StreamDecodeOptions depthLimit(int depthLimit) {
        maxDepthLimit = Math.max(0, depthLimit);
        return this;
    }

    /**
     * Gets the configured maximum number of elements that can be decoded from an array
     * encoded with the zero width AMQP types (Null, UInt0. ULong0, List0, Boolean_False
     * and Boolean_True). A return value of zero indicates that the decoder should throw
     * if any array with zero width encodings is encountered.
     *
     * @return the configured max number of zero width array encoding elements.
     */
    public int maxZeroWidthArrayElements() {
        return maxZeroWidthArrayElements;
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
     * @return this {@link StreamDecodeOptions} instance for chaining.
     */
    public StreamDecodeOptions maxZeroWidthArrayElements(int maxElements) {
        maxZeroWidthArrayElements = Math.max(0, maxElements);
        return this;
    }

    /**
     * Gets the configured maximum size of an encoded string this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded string size supported.
     */
    public int maxStringSize() {
        return maxStringLength;
    }

    /**
     * Sets the configured maximum size of an encoded string this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecodeOptions} instance for chaining.
     */
    public StreamDecodeOptions maxStringSize(int maximumSize) {
        this.maxStringLength = maximumSize;
        return this;
    }

    /**
     * Gets the configured maximum size of an encoded array this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded array size supported.
     */
    public int maxArraySize() {
        return maxArrayLength;
    }

    /**
     * Sets the configured maximum size of an encoded array this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecodeOptions} instance for chaining.
     */
    public StreamDecodeOptions maxArraySize(int maximumSize) {
        maxArrayLength = maximumSize;
        return this;
    }

    /**
     * Gets the configured maximum indicated encoded size of a List this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded list elements supported.
     */
    public int maxListSize() {
        return maxListSize;
    }

    /**
     * Sets the configured maximum indicated encoded size of a List this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecodeOptions} instance for chaining.
     */
    public StreamDecodeOptions maxListSize(int maximumSize) {
        maxListSize = maximumSize;
        return this;
    }

    /**
     * Gets the configured maximum indicated encoded size of a List this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded list size supported.
     */
    public int maxMapSize() {
        return maxMapSize;
    }

    /**
     * Sets the configured maximum indicated encoded size of a Map this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecodeOptions} instance for chaining.
     */
    public StreamDecodeOptions maxMapSize(int maximumSize) {
        maxMapSize = maximumSize;
        return this;
    }

    /**
     * Gets the configured maximum length of an encoded Binary this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded binary length supported.
     */
    public int maxBinarySize() {
        return maxBinaryLength;
    }

    /**
     * Gets the configured maximum size of an encoded Binary this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecodeOptions} instance for chaining.
     */
    public StreamDecodeOptions maxBinarySize(int maximumSize) {
        maxBinaryLength = maximumSize;
        return this;
    }

    /**
     * Gets the configured maximum size of an encoded Symbol this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @return the configured max encoded Symbol length supported.
     */
    public int maxSymbolSize() {
        return maxSymbolLength;
    }

    /**
     * Gets the configured maximum length of an encoded Symbol this decoder will allow before
     * it throws an {@link DecodeException} to indicate that a rule violation has occurred.
     *
     * @param maximumSize
     * 		The maximum allowed encoded size value to allow before an decoding exception is thrown.
     *
     * @return this {@link StreamDecodeOptions} instance for chaining.
     */
    public StreamDecodeOptions maxSymbolSize(int maximumSize) {
        maxSymbolLength = maximumSize;
        return this;
    }
}
