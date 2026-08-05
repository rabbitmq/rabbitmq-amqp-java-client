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
package org.apache.qpid.protonj2.codec.decoders;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;

/**
 * State object used by the Built in Decoder implementation.
 */
public final class ProtonDecoderState implements DecoderState {

    private static final int MAX_CHAR_BUFFER_CACHE_SIZE = 100;

    /**
     * The default maximum depth the decoder will allow before triggering an {@link DecodeException}
     * when the state depth value is increased during a decode process of complex types that can next
     * objects.
     */
    public static final int DEFAULT_MAX_DECODE_DEPTH = 32;

    /**
     * The default maximum number of zero width array elements that controls the size of an zero width
     * array type that can be decoded without throwing an DecodeException.
     */
    public static final int DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS = 0;

    private final ProtonDecoder decoder;
    private final byte[] decodeCache = new byte[MAX_CHAR_BUFFER_CACHE_SIZE];

    private int maxDecodeDepth = DEFAULT_MAX_DECODE_DEPTH;
    private UTF8Decoder stringDecoder;
    private int maxZeroWidthArrayElemets = DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS;

    private int decodeDepth;

    /**
     * Create a new {@link DecoderState} instance that is joined forever to the given {@link Decoder}.
     *
     * @param decoder
     * 		The {@link Decoder} that this state instance is assigned to.
     */
    public ProtonDecoderState(ProtonDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public ProtonDecoder getDecoder() {
        return decoder;
    }

    @Override
    public ProtonDecoderState reset() {
        decodeDepth = 0;
        return this;
    }

    /**
     * @return the currently set custom UTF-8 {@link String} decoder or null if non set.
     */
    public UTF8Decoder getStringDecoder() {
        return stringDecoder;
    }

    /**
     * Sets a custom UTF-8 {@link String} decoder that will be used for all {@link String} decoding done
     * from the encoder associated with this {@link DecoderState} instance.  If no decoder is registered
     * then the implementation uses its own decoding algorithm.
     *
     * @param stringDecoder
     * 		a custom {@link UTF8Decoder} that will be used for all {@link String} decoding.
     *
     * @return this {@link Decoder} instance.
     */
    public ProtonDecoderState setStringDecoder(UTF8Decoder stringDecoder) {
        this.stringDecoder = stringDecoder;
        return this;
    }

    @Override
    public int getMaxZeroWidthArrayElements() {
        return maxZeroWidthArrayElemets;
    }

    @Override
    public ProtonDecoderState setMaxZeroWidthArrayElements(int maxElements) {
        this.maxZeroWidthArrayElemets = Math.max(0, maxElements);
        return this;
    }

    @Override
    public ProtonDecoderState setDepthLimit(int limit) {
        this.maxDecodeDepth = Math.max(0, limit);
        return this;
    }

    @Override
    public int getDepthLimit() {
        return maxDecodeDepth;
    }

    @Override
    public ProtonDecoderState increaseDepth() throws DecodeException {
        if (++decodeDepth > maxDecodeDepth) {
            --decodeDepth; // Unwind decrement to ensure the depth returns to zero.
            throw new DecodeException(
                "The nesting of types in the object being decoded exceeded the configured limit: " + maxDecodeDepth);
        }

        return this;
    }

    @Override
    public ProtonDecoderState decreaseDepth() {
        decodeDepth = Math.max(0, decodeDepth - 1);
        return this;
    }

    @Override
    public String decodeUTF8(ProtonBuffer buffer, int length) throws DecodeException {
        if (length < 0) {
            throw new DecodeException("Specified UTF length:" + length + " cannot be negative.");
        }

        if (length > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                "String encoded size %d is specified to be greater than the amount " +
                "of data available (%d)", length, buffer.getReadableBytes()));
        }

        if (stringDecoder == null) {
            if (buffer.readableComponentCount() == 1) {
                try (ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
                    final ProtonBufferComponent component = accessor.first();

                    // Optimal fast path no copy for buffers that are array backed.
                    if (component.hasReadbleArray()) {
                        final String result = new String(component.getReadableArray(),
                                                         component.getReadableArrayOffset(),
                                                         length, StandardCharsets.UTF_8);

                        buffer.advanceReadOffset(length);

                        return result;
                    }
                }
            }

            final byte[] target = length > MAX_CHAR_BUFFER_CACHE_SIZE ? new byte[length] : decodeCache;

            buffer.readBytes(target, 0, length);

            return new String(target, 0, length, StandardCharsets.UTF_8);
        } else {
            final int originalPosition = buffer.getReadOffset();

            try {
                return stringDecoder.decodeUTF8(buffer, length);
            } catch (Exception ex) {
                throw new DecodeException("Cannot parse encoded UTF8 String", ex);
            } finally {
                buffer.setReadOffset(originalPosition + length);
            }
        }
    }
}
