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
import org.apache.qpid.protonj2.codec.decoders.ProtonDecoderState;

/**
 * Configuration options that can be passed to the {@link Delivery} APIs for
 * reading messages and delivery annotations to give access to certain options
 * that can influence certain behaviors of the AMQP decoder that reads the bytes
 * associated with the incoming delivery.
 */
public final class DecodeOptions {

    private int maxDepthLimit = ProtonDecoderState.DEFAULT_MAX_DECODE_DEPTH;
    private int maxZeroWidthArrayElements = ProtonDecoderState.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS;

    /**
     * Creates and returns a default decode options instance.
     *
     * @return a new {@link DecodeOptions} instance configured with defaults.
     */
    public static DecodeOptions defaultOptions() {
        return new DecodeOptions();
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
    public DecodeOptions depthLimit(int depthLimit) {
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
     * @return this {@link DecodeOptions} instance for chaining.
     */
    public DecodeOptions maxZeroWidthArrayElements(int maxElements) {
        maxZeroWidthArrayElements = Math.max(0, maxElements);
        return this;
    }
}
