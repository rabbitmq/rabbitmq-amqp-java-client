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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;

/**
 * State object used by the Built in Decoder implementation.
 */
public final class ProtonStreamDecoderState implements StreamDecoderState {

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

    private final CharsetDecoder STRING_DECODER = StandardCharsets.UTF_8.newDecoder();
    private final ProtonStreamDecoder decoder;
    private final byte[] decodeCache = new byte[MAX_CHAR_BUFFER_CACHE_SIZE];
    private final CharBuffer charDecodeChache = CharBuffer.allocate(MAX_CHAR_BUFFER_CACHE_SIZE);

    private int maxDecodeDepth = DEFAULT_MAX_DECODE_DEPTH;
    private UTF8StreamDecoder stringDecoder;
    private int maxZeroWidthArrayElemets = DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS;
    private int maxStringLength = DEFAULT_MAX_ALLOCATION_LIMIT;
    private int maxArrayLength = DEFAULT_MAX_ALLOCATION_LIMIT;
    private int maxBinaryLength = DEFAULT_MAX_ALLOCATION_LIMIT;
    private int maxSymbolLength = DEFAULT_MAX_ALLOCATION_LIMIT;
    private int maxListSize = DEFAULT_MAX_ALLOCATION_LIMIT;
    private int maxMapSize = DEFAULT_MAX_ALLOCATION_LIMIT;

    private int decodeDepth;

    /**
     * Create a new {@link StreamDecoderState} instance that is joined forever to the given {@link Decoder}.
     *
     * @param decoder
     * 		The {@link StreamDecoder} that this state instance is assigned to.
     */
    public ProtonStreamDecoderState(ProtonStreamDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public ProtonStreamDecoder getDecoder() {
        return decoder;
    }

    @Override
    public ProtonStreamDecoderState reset() {
        decodeDepth = 0;
        return this;
    }

    /**
     * @return the currently set custom UTF-8 {@link String} decoder or null if non set.
     */
    public UTF8StreamDecoder getStringDecoder() {
        return stringDecoder;
    }

    /**
     * Sets a custom UTF-8 {@link String} decoder that will be used for all {@link String} decoding done
     * from the encoder associated with this {@link StreamDecoderState} instance.  If no decoder is registered
     * then the implementation uses its own decoding algorithm.
     *
     * @param stringDecoder
     * 		a custom {@link UTF8Decoder} that will be used for all {@link String} decoding.
     */
    public void setStringDecoder(UTF8StreamDecoder stringDecoder) {
        this.stringDecoder = stringDecoder;
    }

    @Override
    public int getMaxZeroWidthArrayElements() {
        return maxZeroWidthArrayElemets;
    }

    @Override
    public ProtonStreamDecoderState setMaxZeroWidthArrayElements(int maxElements) {
        this.maxZeroWidthArrayElemets = maxElements;
        return this;
    }

    @Override
    public int getMaxStringSize() {
        return maxStringLength;
    }

    @Override
    public ProtonStreamDecoderState setMaxStringSize(int maxStringLength) {
        this.maxStringLength = maxStringLength;
        return this;
    }

    @Override
    public int getMaxArraySize() {
        return maxArrayLength;
    }

    @Override
    public ProtonStreamDecoderState setMaxArraySize(int maxArrayLength) {
        this.maxArrayLength = maxArrayLength;
        return this;
    }

    @Override
    public int getMaxListSize() {
        return maxListSize;
    }

    @Override
    public ProtonStreamDecoderState setMaxListSize(int maxListSize) {
        this.maxListSize = maxListSize;
        return this;
    }

    @Override
    public int getMaxMapSize() {
        return maxMapSize;
    }

    @Override
    public ProtonStreamDecoderState setMaxMapSize(int maxMapSize) {
        this.maxMapSize = maxMapSize;
        return this;
    }

    @Override
    public int getMaxBinarySize() {
        return maxBinaryLength;
    }

    @Override
    public ProtonStreamDecoderState setMaxBinarySize(int maxBinaryLength) {
        this.maxBinaryLength = maxBinaryLength;
        return this;
    }

    @Override
    public int getMaxSymbolSize() {
        return maxSymbolLength;
    }

    @Override
    public ProtonStreamDecoderState setMaxSymbolSize(int maxSymbolLength) {
        this.maxSymbolLength = maxSymbolLength;
        return this;
    }

    @Override
    public ProtonStreamDecoderState setDepthLimit(int limit) {
        this.maxDecodeDepth = Math.max(0, limit);
        return this;
    }

    @Override
    public int getDepthLimit() {
        return maxDecodeDepth;
    }

    @Override
    public ProtonStreamDecoderState increaseDepth() throws DecodeException {
        if (++decodeDepth > maxDecodeDepth) {
            --decodeDepth; // Unwind decrement to ensure the depth returns to zero.
            throw new DecodeException(
                "The nesting of types in the object being decoded exceeded the configured limit: " + maxDecodeDepth);
        }

        return this;
    }

    @Override
    public ProtonStreamDecoderState decreaseDepth() {
        decodeDepth = Math.max(0, decodeDepth - 1);
        return this;
    }

    @Override
    public String decodeUTF8(InputStream stream, int length) throws DecodeException {
        if (length < 0) {
            throw new DecodeException("Specified UTF length:" + length + " cannot be negative.");
        }

        if (length > getMaxStringSize()) {
            throw new DecodeException(String.format(
                    "String encoded size %d is specified to be greater than the configured " +
                    "max string size (%d)", length, getMaxStringSize()));
        }

        try {
            if (stringDecoder == null) {
                return internalDecode(stream, length, STRING_DECODER, length > MAX_CHAR_BUFFER_CACHE_SIZE ? new byte[length] : decodeCache);
            } else {
                return stringDecoder.decodeUTF8(stream);
            }
        } catch (Exception ex) {
            throw new DecodeException("Cannot parse encoded UTF8 String", ex);
        }
    }

    private String internalDecode(InputStream stream, final int length, CharsetDecoder decoder, byte[] scratch) throws IOException {
        int offset = 0;

        if (stream.read(scratch, 0, length) != length) {
            throw new DecodeException("Failed to read all string bytes from provided stream");
        }

        for (; offset < length; offset++) {
            // Check for non-ASCII chars and break if any which will trigger fallback decode
            if (scratch[offset] < 0) {
                break;
            }
        }

        if (offset == length) {
            return new String(scratch, 0, length, StandardCharsets.US_ASCII);
        } else {
            return internalDecodeUTF8(decoder, scratch, length, offset);
        }
    }

    private String internalDecodeUTF8(CharsetDecoder decoder, final byte[] contents, final int length, final int offset) throws IOException {
        final int remaining = length - offset; // Largest possible outcome if all remaining are single byte values

        if (offset < 0) {
            throw new IllegalArgumentException("Specified offset:" + offset + " cannot be negative.");
        }

        if (remaining < 0) {
            throw new IllegalArgumentException("Remaining UTF8 Bytes size cannot be negative, was " + remaining);
        }

        final ByteBuffer byteBuffer = ByteBuffer.wrap(contents, offset, remaining);
        final CharBuffer out = length > MAX_CHAR_BUFFER_CACHE_SIZE ? CharBuffer.allocate(length) : charDecodeChache.clear();

        // Pre-populate the ASCII portion we already scanned into the output character buffer
        for (int i = 0; i < offset; i++) {
            out.put((char) contents[i]);
        }

        try {
            CoderResult cr = null;

            for (;;) {
                cr = byteBuffer.hasRemaining() ? decoder.decode(byteBuffer, out, true) : CoderResult.UNDERFLOW;

                if (cr.isUnderflow()) {
                    cr = decoder.flush(out);
                }
                if (cr.isUnderflow()) {
                    break;
                }

                // The char buffer should have been sufficient here but wasn't so we know
                // that there was some encoding issue on the other end.
                cr.throwException();
            }

            return out.flip().toString();
        } catch (CharacterCodingException e) {
            throw new DecodeException("Cannot parse encoded UTF8 String", e);
        } finally {
            decoder.reset();
        }
    }
}
