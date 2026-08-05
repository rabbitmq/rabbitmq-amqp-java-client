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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.MapTypeDecoder;

/**
 * Abstract base for all List based Described Type decoders which implements the generic methods
 * common to all the implementations.
 *
 * @param <D> The described type that this map type implements
 * @param <K> The key type for the elements in the described type map
 */
public abstract class AbstractDescribedMapTypeDecoder<D, K> extends AbstractDescribedTypeDecoder<D> {

    private static final int MAX_MAP_PREALLOCATION = 256;

    @Override
    public final D readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        state.increaseDepth();

        try {
            final MapTypeDecoder mapTypeDecoder =
                checkIsExpectedTypeAndCast(MapTypeDecoder.class, state.getDecoder().readNextTypeDecoder(buffer, state));

            return createDescribed(readMap(buffer, state, mapTypeDecoder));
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public final D readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        state.increaseDepth();

        try {
            final MapTypeDecoder mapTypeDecoder =
                checkIsExpectedTypeAndCast(MapTypeDecoder.class, state.getDecoder().readNextTypeDecoder(stream, state));

            return createDescribed(readMap(stream, state, mapTypeDecoder));
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public final void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(MapTypeDecoder.class, decoder).skipValue(buffer, state);
    }

    @Override
    public final void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(MapTypeDecoder.class, decoder).skipValue(stream, state);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final D[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);
        final MapTypeDecoder mapDecoder = checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder);

        validateArrayConstraints(count, buffer, state, mapDecoder);

        final D[] result = (D[]) Array.newInstance(getTypeClass(), count);

        for (int i = 0; i < count; ++i) {
            result[i] = createDescribed(readMap(buffer, state, mapDecoder));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final D[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);
        final MapTypeDecoder mapDecoder = checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder);

        validateArrayConstraints(count, stream, state, mapDecoder);

        final D[] result = (D[]) Array.newInstance(getTypeClass(), count);

        for (int i = 0; i < count; ++i) {
            result[i] = createDescribed(readMap(stream, state, mapDecoder));
        }

        return result;
    }

    /**
     * Called from read method to wrap the decoded {@link Map} in the described type wrapper that
     * this {@link Map} type manages.
     *
     * @param map
     * 		The decoded map that this type handles.
     *
     * @return the described type wrapper around the decoded {@link Map}.
     */
    protected abstract D createDescribed(Map<K, Object> map);

    /**
     * Called during decoding of the described {@link Map} body to allow the implementation to read
     * the map key using the optimal decoder API for the type of key stored in the map.
     *
     * @param buffer
     * 		The buffer where the encoded {@link Map} body lives.
     * @param decoder
     * 		The {@link Decoder} to use for reading the key.
     * @param state
     * 		The {@link DecoderState} to use while reading the key.
     *
     * @return the newly read {@link Map} key.
     *
     * @throws DecodeException if an error occurs while reading the map key.
     */
    protected abstract K readKey(ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException;

    /**
     * Called during decoding of the described {@link Map} body to allow the implementation to read
     * the map key using the optimal decoder API for the type of key stored in the map.
     *
     * @param stream
     * 		The bytes stream where the encoded {@link Map} body lives.
     * @param decoder
     * 		The {@link Decoder} to use for reading the key.
     * @param state
     * 		The {@link DecoderState} to use while reading the key.
     *
     * @return the newly read {@link Map} key.
     *
     * @throws DecodeException if an error occurs while reading the map key.
     */
    protected abstract K readKey(InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException;

    private Map<K, Object> readMap(ProtonBuffer buffer, DecoderState state, MapTypeDecoder mapDecoder) throws DecodeException {
        final int size = mapDecoder.readSize(buffer, state);
        final int expectedEndPos = buffer.getReadOffset() + size;
        final int count = validateAndGetCount(size, buffer, state, mapDecoder);
        final Decoder decoder = state.getDecoder();
        final int entries = count / 2;

        // Count include both key and value so we must include that in the loop
        final Map<K, Object> map = new LinkedHashMap<>(Math.min(MAX_MAP_PREALLOCATION, entries));

        for (int i = 0; i < entries; i++) {
            map.put(readKey(buffer, decoder, state), decoder.readObject(buffer, state));
        }

        if (buffer.getReadOffset() != expectedEndPos) {
            throw new DecodeException("Map decoding did not read the expected amount of bytes: " + size);
        }

        return map;
    }

    private Map<K, Object> readMap(InputStream stream, StreamDecoderState state, MapTypeDecoder mapDecoder) throws DecodeException {
        final int size = mapDecoder.readSize(stream, state);
        final int count = validateAndGetCount(size, stream, state, mapDecoder);
        final StreamDecoder decoder = state.getDecoder();
        final int entries = count / 2;

        // Count include both key and value so we must include that in the loop
        final Map<K, Object> map = new LinkedHashMap<>(Math.min(MAX_MAP_PREALLOCATION, entries));

        for (int i = 0; i < entries; i++) {
            map.put(readKey(stream, decoder, state), decoder.readObject(stream, state));
        }

        return map;
    }

    protected void scanMapEntries(ProtonBuffer buffer, DecoderState state, ScanningContext<K> context, BiConsumer<K, Object> matchConsumer) throws DecodeException {
        final TypeDecoder<?> typeDecoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (typeDecoder.isNull()) {
            return;
        }

        final MapTypeDecoder mapDecoder = checkIsExpectedTypeAndCast(MapTypeDecoder.class, typeDecoder);

        try {
            mapDecoder.scanKeys(buffer, state, context, matchConsumer);
        } finally {
            context.reset();
        }
    }

    protected void scanMapEntries(InputStream stream, StreamDecoderState state, StreamScanningContext<K> context, BiConsumer<K, Object> matchConsumer) throws DecodeException {
        final StreamTypeDecoder<?> typeDecoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (typeDecoder.isNull()) {
            return;
        }

        final MapTypeDecoder mapDecoder = checkIsExpectedTypeAndCast(MapTypeDecoder.class, typeDecoder);

        try {
            mapDecoder.scanKeys(stream, state, context, matchConsumer);
        } finally {
            context.reset();
        }
    }

    protected static final int validateAndGetCount(int size, ProtonBuffer buffer, DecoderState state, MapTypeDecoder decoder) throws DecodeException {
        if (Integer.compareUnsigned(size, buffer.getReadableBytes()) > 0) {
            throw new DecodeException(String.format(
                "Map encoded size is specified to be greater than the amount " +
                "of data available s:(%d) r:(%d)", Integer.toUnsignedLong(size), buffer.getReadableBytes()));
        }

        final int count = decoder.readCount(buffer, state);

        if (Integer.compareUnsigned(count, size) >= 0) {
            throw new DecodeException(String.format(
                "Map encoded count is specified to be greater than the reported encoded size " +
                "s:(%d) c:(%d)", Integer.toUnsignedLong(size), Integer.toUnsignedLong(count)));
        }

        if (count % 2 != 0) {
            throw new DecodeException(String.format(
                "Map encoded number of elements %d is not an even number.", count));
        }

        return count;
    }

    protected static final int validateAndGetCount(int size, InputStream stream, StreamDecoderState state, MapTypeDecoder decoder) throws DecodeException {
        if (Integer.compareUnsigned(size, state.getMaxMapSize()) > 0) {
            throw new DecodeException(String.format(
                "Map encoded size is specified to be greater than the configured maximum " +
                "map size allowed s:(%d) c:(%d)", Integer.toUnsignedLong(size), state.getMaxMapSize()));
        }

        final int count = decoder.readCount(stream, state);

        if (Integer.compareUnsigned(count, size) >= 0) {
            throw new DecodeException(String.format(
                "Map encoded count is specified to be greater than the reported encoded size " +
                "s:(%d) c:(%d)", Integer.toUnsignedLong(size), Integer.toUnsignedLong(count)));
        }

        if (count % 2 != 0) {
            throw new DecodeException(String.format(
                "Map encoded number of elements %d is not an even number.", count));
        }

        return count;
    }
}
