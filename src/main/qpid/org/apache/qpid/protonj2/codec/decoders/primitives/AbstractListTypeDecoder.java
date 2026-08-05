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
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;

/**
 * Base for the various List type decoders needed to read AMQP List values.
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractListTypeDecoder extends AbstractPrimitiveTypeDecoder<List> implements ListTypeDecoder {

    private static final int MAX_LIST_PREALLOCATION = 256;

    @Override
    public List<Object> readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        state.increaseDepth();

        try {
            final int size = readSize(buffer, state);
            final int expectedEndPos = buffer.getReadOffset() + size;

            // Ensure we do not allocate an array of size greater then the available data, otherwise there is a risk for an OOM error
            if (Integer.compareUnsigned(size, buffer.getReadableBytes()) > 0) {
                throw new DecodeException(String.format(
                        "List encoded size %d is specified to be greater than the amount " +
                        "of data available (%d)", Integer.toUnsignedLong(size), buffer.getReadableBytes()));
            }

            final int count = readCount(buffer, state);

            if (Integer.compareUnsigned(count, size) > 0) {
                throw new DecodeException(String.format(
                        "List encoded element count is specified to be greater than the encoded size " +
                        "s:(%d) c:(%d)", Integer.toUnsignedLong(size), Integer.toUnsignedLong(count)));
            }

            final List<Object> list = new ArrayList<>(Math.min(MAX_LIST_PREALLOCATION, count));
            final Decoder decoder = state.getDecoder();
            for (int i = 0; i < count; i++) {
                list.add(decoder.readObject(buffer, state));
            }

            if (buffer.getReadOffset() != expectedEndPos) {
                throw new DecodeException("List decoding did not read the expected amount of bytes: " + size);
            }

            return list;
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final int size = readSize(buffer, state);

        if (Integer.compareUnsigned(size, buffer.getReadableBytes()) > 0) {
            throw new DecodeException(String.format(
                "List encoded size %d is specified to be greater than the amount " +
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
    public List<Object> readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        state.increaseDepth();

        try {
            final int size = readSize(stream, state);

            if (Integer.compareUnsigned(size, state.getMaxListSize()) > 0) {
                throw new DecodeException(String.format(
                        "List encoded suze is specified to be greater than maximum allowed " +
                        "s:(%d) m:(%d)", Integer.toUnsignedLong(size), state.getMaxListSize()));
            }

            final int count = readCount(stream, state);

            if (Integer.compareUnsigned(count, size) > 0) {
                throw new DecodeException(String.format(
                        "List encoded element count is specified to be greater than the encoded size " +
                        "s:(%d) c:(%d)", Integer.toUnsignedLong(size), Integer.toUnsignedLong(count)));
            }

            final List<Object> list = new ArrayList<>(Math.min(MAX_LIST_PREALLOCATION, count));
            final StreamDecoder decoder = state.getDecoder();
            for (int i = 0; i < count; i++) {
                list.add(decoder.readObject(stream, state));
            }

            return list;
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final int size = readSize(stream, state);

        if (Integer.compareUnsigned(size, state.getMaxListSize()) > 0) {
            throw new DecodeException(String.format(
                    "List encoded suze is specified to be greater than maximum allowed " +
                    "s:(%d) m:(%d)", Integer.toUnsignedLong(size), state.getMaxListSize()));
        }

        state.increaseDepth();

        try {
            ProtonStreamUtils.skipBytes(stream, size);
        } finally {
            state.decreaseDepth();
        }
    }
}
