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
package org.apache.qpid.protonj2.codec.decoders.primitives;

import java.io.IOException;
import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.impl.ProtonByteArrayBufferAllocator;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Base class for the Symbol decoders used on AMQP Symbol types.
 */
public abstract class AbstractSymbolTypeDecoder extends AbstractPrimitiveTypeDecoder<Symbol> implements SymbolTypeDecoder {

    @Override
    public Symbol readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final int length = readSize(buffer, state);

        if (length == 0) {
            return Symbol.getSymbol("");
        }

        if (Integer.compareUnsigned(length, buffer.getReadableBytes()) > 0) {
            throw new DecodeException(String.format(
                    "Symbol encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", Integer.toUnsignedLong(length), buffer.getReadableBytes()));
        }

        try (ProtonBuffer symbolBuffer = buffer.copy(buffer.getReadOffset(), length, true)) {
            buffer.advanceReadOffset(length);
            return getSymbol(symbolBuffer, true);
        }
    }

    /**
     * Reads a String view of an encoded Symbol value from the given buffer.
     * <p>
     * This method has the same result as calling the Symbol reading variant
     * {@link #readValue(ProtonBuffer, DecoderState)} and then invoking the toString
     * method on the resulting Symbol.
     *
     * @param buffer
     *      The buffer to read the encoded symbol from.
     * @param state
     *      The encoder state that applied to this decode operation.
     *
     * @return a String view of the encoded Symbol value.
     *
     * @throws DecodeException if an error occurs decoding the Symbol from the given buffer.
     */
    public String readString(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return readValue(buffer, state).toString();
    }

    @Override
    public Symbol readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final int length = readSize(stream, state);

        if (length == 0) {
            return Symbol.getSymbol("");
        }

        if (Integer.compareUnsigned(length, state.getMaxSymbolSize()) > 0) {
            throw new DecodeException(String.format(
                "Binary encoded length is specified to be greater than maximum allowed " +
                "l:(%d) m:(%d)", Integer.toUnsignedLong(length), state.getMaxSymbolSize()));
        }

        final byte[] symbolBytes = new byte[length];

        try {
            stream.read(symbolBytes);
        } catch (IOException ex) {
            throw new DecodeException("Error while reading Symbol payload bytes", ex);
        }

        return getSymbol(ProtonByteArrayBufferAllocator.wrapped(symbolBytes).convertToReadOnly(), false);
    }

    /**
     * Reads a String view of an encoded Symbol value from the given buffer.
     * <p>
     * This method has the same result as calling the Symbol reading variant
     * {@link #readValue(ProtonBuffer, DecoderState)} and then invoking the toString
     * method on the resulting Symbol.
     *
     * @param stream
     *      The InputStream to read the encoded symbol from.
     * @param state
     *      The encoder state that applied to this decode operation.
     *
     * @return a String view of the encoded Symbol value.
     *
     * @throws DecodeException if an error occurs decoding the Symbol from the given buffer.
     */
    public String readString(InputStream stream, StreamDecoderState state) throws DecodeException {
        return readValue(stream, state).toString();
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final int length = readSize(buffer, state);

        if (Integer.compareUnsigned(length, buffer.getReadableBytes()) > 0) {
            throw new DecodeException(String.format(
                "Symbol encoded size %d is specified to be greater than the amount " +
                "of data available (%d)", Integer.toUnsignedLong(length), buffer.getReadableBytes()));
        }

        buffer.advanceReadOffset(length);
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final int length = readSize(stream, state);

        if (Integer.compareUnsigned(length, state.getMaxSymbolSize()) > 0) {
            throw new DecodeException(String.format(
                "Binary encoded length is specified to be greater than maximum allowed " +
                "l:(%d) m:(%d)", Integer.toUnsignedLong(length), state.getMaxSymbolSize()));
        }

        ProtonStreamUtils.skipBytes(stream, length);
    }

    /**
     * Gets a singleton {@link Symbol} instance that matches the given {@link ProtonBuffer}
     * byte view of the {@link Symbol}. A subclass can override this to produce the Symbol
     * singleton from a source other than the default which is the general symbol cache.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} version of the {@link Symbol} value.
     * @param copyOnCreate
     * 		Should the provided buffer be copied during creation of a new {@link Symbol}.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    protected Symbol getSymbol(ProtonBuffer buffer, boolean copyOnCreate) {
        return Symbol.getSymbol(buffer, copyOnCreate);
    }
}
