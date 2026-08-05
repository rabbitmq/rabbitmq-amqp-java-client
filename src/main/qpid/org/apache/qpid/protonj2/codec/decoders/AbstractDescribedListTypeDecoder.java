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

import static org.apache.qpid.protonj2.codec.decoders.PrimitiveArrayTypeDecoder.validateArrayConstraints;

import java.io.InputStream;
import java.lang.reflect.Array;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;

/**
 * Abstract base for all List based Described Type decoders which implements the generic methods
 * common to all the implementations.
 *
 * @param <V> The type that this decoder handles.
 */
public abstract class AbstractDescribedListTypeDecoder<V> extends AbstractDescribedTypeDecoder<V> {

    @Override
    public final V readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        state.increaseDepth();

        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        try {
            return readSingle(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public final V readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        state.increaseDepth();

        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        try {
            return readSingle(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public final void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        checkIsExpectedType(ListTypeDecoder.class, state.getDecoder().readNextTypeDecoder(buffer, state)).skipValue(buffer, state);
    }

    @Override
    public final void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        checkIsExpectedType(ListTypeDecoder.class, state.getDecoder().readNextTypeDecoder(stream, state)).skipValue(stream, state);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final V[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);
        final ListTypeDecoder listDecoder = checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder);

        validateArrayConstraints(count, buffer, state, listDecoder);

        final V[] result = (V[]) Array.newInstance(getTypeClass(), count);
        for (int i = 0; i < count; ++i) {
            result[i] = readSingle(buffer, state, listDecoder);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final V[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);
        final ListTypeDecoder listDecoder = checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder);

        validateArrayConstraints(count, stream, state, listDecoder);

        final V[] result = (V[]) Array.newInstance(getTypeClass(), count);
        for (int i = 0; i < count; ++i) {
            result[i] = readSingle(stream, state, listDecoder);
        }

        return result;
    }

    /**
     * (@return the minimum number of elements that must appear in this list type}
     */
    protected abstract int getMinListElements();

    /**
     * {@return the maximum number of elements that may appear in this list type}
     */
    protected abstract int getMaxListElements();

    /**
     * Reads a single instance of the described list type using the provided decoder. The default implementation
     * performs a series of validation checks on the incoming encoded data and then calls the abstract readListType
     * method which can assume the basic contracts of the encoding are correct and simply read the elements into the
     * final described type object.
     *
     * @param buffer
     * 		The source of the encoded bytes to read the value from.
     * @param state
     * 		The decoder state that was passed at the start of decoding.
     * @param listDecoder
     * 		The list decoder that indicates the structure of the encoded lists of elements.
     *
     * @return a single value that is the described type this decoder reads using the given list decoder.
     *
     * @throws DecodeException if an error occurs while performing the decode.
     */
    protected V readSingle(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final int size = listDecoder.readSize(buffer, state);
        final int expectedEndPos = buffer.getReadOffset() + size;

        if (Integer.compareUnsigned(size, buffer.getReadableBytes()) > 0) {
            throw new DecodeException(String.format(
                "List encoded size is specified to be greater than the amount " +
                "of data available s:(%d) r:(%d)", Integer.toUnsignedLong(size), buffer.getReadableBytes()));
        }

        final int count = listDecoder.readCount(buffer, state);

        if (Integer.compareUnsigned(count, size) > 0) {
            throw new DecodeException(String.format(
                "List encoded count is specified to be greater than the reported encoded size " +
                "s:(%d) c:(%d)", Integer.toUnsignedLong(size), Integer.toUnsignedLong(count)));
        }

        if (Integer.compareUnsigned(count, getMinListElements()) < 0) {
            throw new DecodeException(String.format(
                "Not enough list elements indicated in the encoded count, expected %d but got %d",
                getMinListElements(), Integer.toUnsignedLong(count)));
        }

        if (Integer.compareUnsigned(count, getMaxListElements()) > 0) {
            throw new DecodeException(String.format(
                "To many elements indicated in the encoded count, maximum %d but got %d",
                getMaxListElements(), Integer.toUnsignedLong(count)));
        }

        final V type = readType(count, buffer, state.getDecoder(), state);

        if (buffer.getReadOffset() != expectedEndPos) {
            throw new DecodeException("List decoding did not read the expected amount of bytes: " + size);
        }

        return type;
    }

    /**
     * Reads a single instance of the described list type using the provided decoder. The default implementation
     * performs a series of validation checks on the incoming encoded data and then calls the abstract readListType
     * method which can assume the basic contracts of the encoding are correct and simply read the elements into the
     * final described type object.
     *
     * @param stream
     * 		The source of the encoded bytes to read the value from.
     * @param state
     * 		The decoder state that was passed at the start of decoding.
     * @param listDecoder
     * 		The list decoder that indicates the structure of the encoded lists of elements.
     *
     * @return a single value that is the described type this decoder reads using the given list decoder.
     *
     * @throws DecodeException if an error occurs while performing the decode.
     */
    protected V readSingle(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final int size = listDecoder.readSize(stream, state);

        if (Integer.compareUnsigned(size, state.getMaxListSize()) > 0) {
            throw new DecodeException(String.format(
                "List encoded size is specified to be greater than the configured maximum " +
                "List size allowed s:(%d) c:(%d)", Integer.toUnsignedLong(size), state.getMaxListSize()));
        }

        final int count = listDecoder.readCount(stream, state);

        if (Integer.compareUnsigned(count, size) > 0) {
            throw new DecodeException(String.format(
                "List encoded count is specified to be greater than the reported encoded size " +
                "s:(%d) c:(%d)", Integer.toUnsignedLong(size), Integer.toUnsignedLong(count)));
        }

        if (Integer.compareUnsigned(count, getMinListElements()) < 0) {
            throw new DecodeException(String.format(
                "Not enough list elements indicated in the encoded count, expected %d but got %d",
                getMinListElements(), Integer.toUnsignedLong(count)));
        }

        if (Integer.compareUnsigned(count, getMaxListElements()) > 0) {
            throw new DecodeException(String.format(
                "To many elements indicated in the encoded count, maximum %d but got %d",
                getMaxListElements(), Integer.toUnsignedLong(count)));
        }

        return readType(count, stream, state.getDecoder(), state);
    }

    /**
     * Reads the actual type from the byte stream with the given number of encoded list elements populated
     *
     * @param count
     * 		The number of entries encoded into the list body.
     * @param buffer
     * 		The source of the encoded bytes to read the value from.
     * @param decoder
     * 		The list decoder that indicates the structure of the encoded lists of elements.
     * @param state
     * 		The decoder state that was passed at the start of decoding.
     *
     * @return a single value that is the described type this decoder reads using the given list decoder.
     *
     * @throws DecodeException if an error occurs while performing the decode.
     */
    protected abstract V readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException;

    /**
     * Reads the actual type from the byte stream with the given number of encoded list elements populated
     *
     * @param count
     * 		The number of entries encoded into the list body.
     * @param stream
     * 		The source of the encoded bytes to read the value from.
     * @param decoder
     * 		The list decoder that indicates the structure of the encoded lists of elements.
     * @param state
     * 		The decoder state that was passed at the start of decoding.
     *
     * @return a single value that is the described type this decoder reads using the given list decoder.
     *
     * @throws DecodeException if an error occurs while performing the decode.
     */
    protected abstract V readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException;

}
