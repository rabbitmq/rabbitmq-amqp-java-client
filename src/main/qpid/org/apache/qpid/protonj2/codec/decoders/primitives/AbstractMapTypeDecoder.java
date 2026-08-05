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

import java.io.InputStream;
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
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.codec.decoders.ScanningContext;
import org.apache.qpid.protonj2.codec.decoders.StreamScanningContext;

/**
 * Base for the various Map type decoders used to read AMQP Map values.
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractMapTypeDecoder extends AbstractPrimitiveTypeDecoder<Map> implements MapTypeDecoder {

    private static final int MAX_MAP_PREALLOCATION = 256;

    @Override
    public Map<Object, Object> readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        state.increaseDepth();

        try {
            final int size = readSize(buffer, state);
            final int expectedEndPos = buffer.getReadOffset() + size;

            if (Integer.compareUnsigned(size, buffer.getReadableBytes()) > 0) {
                throw new DecodeException(String.format(
                        "Map encoded size %d is specified to be greater than the amount " +
                        "of data available (%d)", Integer.toUnsignedLong(size), buffer.getReadableBytes()));
            }

            final int count = readCount(buffer, state);

            if (Integer.compareUnsigned(count, size) > 0) {
                throw new DecodeException(String.format(
                        "Map encoded Map entries count %d is specified to be greater than the encoded " +
                        "size of the map (%d)", count, Integer.toUnsignedLong(size)));
            }

            if (count % 2 != 0) {
                throw new DecodeException(String.format(
                    "Map encoded number of elements %d is not an even number.", count));
            }

            final Decoder decoder = state.getDecoder();
            final int elementCount = count / 2;

            // Count include both key and value so we must include that in the loop
            final Map<Object, Object> map = new LinkedHashMap<>(Math.min(MAX_MAP_PREALLOCATION, elementCount));

            for (int i = 0; i < elementCount; i++) {
                map.put(decoder.readObject(buffer, state), decoder.readObject(buffer, state));
            }

            if (buffer.getReadOffset() != expectedEndPos) {
                throw new DecodeException("Map decoding did not read the expected amount of bytes: " + size);
            }

            return map;
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final int size = readSize(buffer, state);

        if (Integer.compareUnsigned(size, buffer.getReadableBytes()) > 0) {
            throw new DecodeException(String.format(
                    "Map encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", Integer.toUnsignedLong(size), buffer.getReadableBytes()));
        }

        state.increaseDepth();

        try {
            buffer.advanceReadOffset(size);
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public <KeyType> void scanKeys(ProtonBuffer buffer, DecoderState state, ScanningContext<KeyType> context, BiConsumer<KeyType, Object> matchConsumer) throws DecodeException {
        final Decoder decoder = state.getDecoder();
        final int size = readSize(buffer, state);

        if (Integer.compareUnsigned(size, buffer.getReadableBytes()) > 0) {
            throw new DecodeException(String.format(
                "Map encoded size %d is specified to be greater than the amount " +
                "of data available (%d)", Integer.toUnsignedLong(size), buffer.getReadableBytes()));
        }

        final int completionOffset = buffer.getReadOffset() + size;
        final int count = readCount(buffer, state);

        if (Integer.compareUnsigned(count, size) > 0) {
            throw new DecodeException(String.format(
                    "Map encoded Map entries count %d is specified to be greater than the encoded " +
                    "size of the map (%d)", count, Integer.toUnsignedLong(size)));
        }

        if (count % 2 != 0) {
            throw new DecodeException(String.format(
                "Map encoded number of elements %d is not an even number.", count));
        }

        final int elementCount = count / 2;

        try {
            for (int i = 0; i < elementCount && !context.isComplete(); ++i) {
                final TypeDecoder<?> keyDecoder = state.getDecoder().readNextTypeDecoder(buffer, state);
                final int keySize = keyDecoder.readSize(buffer, state);

                final boolean matched = context.matches(keyDecoder, buffer, keySize, (key) -> {
                    // Consume the key without decoding.
                    buffer.advanceReadOffset(keySize);
                    // Signal the callback with the decoded value and the key that was provided
                    // which avoid a decode of the key here.
                    matchConsumer.accept(key, decoder.readObject(buffer, state));
                });

                if (!matched) {
                    // Consume the key without decoding since it didn't match the search criteria
                    buffer.advanceReadOffset(keySize);
                    // No match on the key means we need to skip the value so we are positioned
                    // on the next key or the buffer is consumed.
                    decoder.readNextTypeDecoder(buffer, state).skipValue(buffer, state);
                }
            }

            // If matching stooped early ensure that the encoded contents are fully skipped.
            buffer.setReadOffset(completionOffset);
        } finally {
            context.reset();
        }
    }

    @Override
    public Map<Object, Object> readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        state.increaseDepth();

        try {
            final int size = readSize(stream, state);

            if (Integer.compareUnsigned(size, state.getMaxMapSize()) > 0) {
                throw new DecodeException(String.format(
                        "Map encoded size is specified to be greater than maximum allowed " +
                        "s:(%d) m:(%d)", Integer.toUnsignedLong(size), state.getMaxMapSize()));
            }

            final int count = readCount(stream, state);

            if (Integer.compareUnsigned(count, size) > 0) {
                throw new DecodeException(String.format(
                        "Map encoded Map entries count %d is specified to be greater than the encoded " +
                        "size of the map (%d)", Integer.toUnsignedLong(count), Integer.toUnsignedLong(size)));
            }

            if (count % 2 != 0) {
                throw new DecodeException(String.format(
                    "Map encoded number of elements %d is not an even number.", Integer.toUnsignedLong(count)));
            }

            final StreamDecoder decoder = state.getDecoder();
            final int elementCount = count / 2;

            // Count include both key and value so we must include that in the loop
            final Map<Object, Object> map = new LinkedHashMap<>(Math.min(MAX_MAP_PREALLOCATION, elementCount));

            for (int i = 0; i < elementCount; i++) {
                map.put(decoder.readObject(stream, state), decoder.readObject(stream, state));
            }

            return map;
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final int size = readSize(stream, state);

        if (Integer.compareUnsigned(size, state.getMaxMapSize()) > 0) {
            throw new DecodeException(String.format(
                    "Map encoded size is specified to be greater than maximum allowed " +
                    "s:(%d) m:(%d)", Integer.toUnsignedLong(size), state.getMaxMapSize()));
        }

        state.increaseDepth();

        try {
            ProtonStreamUtils.skipBytes(stream, size);
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public <KeyType> void scanKeys(InputStream stream, StreamDecoderState state, StreamScanningContext<KeyType> context, BiConsumer<KeyType, Object> matchConsumer) throws DecodeException {
        final StreamDecoder decoder = state.getDecoder();

        final int size = readSize(stream, state);

        if (Integer.compareUnsigned(size, state.getMaxMapSize()) > 0) {
            throw new DecodeException(String.format(
                    "Map encoded size is specified to be greater than maximum allowed " +
                    "s:(%d) m:(%d)", Integer.toUnsignedLong(size), state.getMaxMapSize()));
        }

        final int count = readCount(stream, state);

        if (Integer.compareUnsigned(count, size) > 0) {
            throw new DecodeException(String.format(
                    "Map encoded Map entries count %d is specified to be greater than the encoded " +
                    "size of the map (%d)", Integer.toUnsignedLong(count), Integer.toUnsignedLong(size)));
        }

        if (count % 2 != 0) {
            throw new DecodeException(String.format(
                "Map encoded number of elements %d is not an even number.", Integer.toUnsignedLong(count)));
        }

        final int elementCount = count / 2;

        try {
            for (int i = 0; i < elementCount && !context.isComplete(); ++i) {
                final StreamTypeDecoder<?> keyDecoder = state.getDecoder().readNextTypeDecoder(stream, state);
                final int keySize = keyDecoder.readSize(stream, state);

                final boolean matched = context.matches(keyDecoder, stream, keySize, (key) -> {
                    // Consume the key without decoding.
                    ProtonStreamUtils.skipBytes(stream, keySize);
                    // Signal the callback with the decoded value and the key that was provided
                    // which avoid a decode of the key here.
                    matchConsumer.accept(key, decoder.readObject(stream, state));
                });

                if (!matched) {
                    // Consume the key without decoding.
                    ProtonStreamUtils.skipBytes(stream, keySize);
                    // No match on the key means we need to skip the value so we are positioned
                    // on the next key or the buffer is consumed.
                    decoder.readNextTypeDecoder(stream, state).skipValue(stream, state);
                }
            }
        } finally {
            context.reset();
        }
    }
}
