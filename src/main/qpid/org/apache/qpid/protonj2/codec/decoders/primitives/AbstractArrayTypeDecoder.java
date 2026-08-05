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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveArrayTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;

/**
 * Base for the decoders of AMQP Array types that defaults to returning opaque Object
 * values to match what the other decoders do.  External decoding tools will need to use
 * the {@link PrimitiveArrayTypeDecoder#isArrayType()} checks to determine how they want
 * to read and return array types.
 */
public abstract class AbstractArrayTypeDecoder extends AbstractPrimitiveTypeDecoder<Object> implements PrimitiveArrayTypeDecoder {

    @Override
    public Class<Object> getTypeClass() {
        return Object.class;
    }

    @Override
    public boolean isArrayType() {
        return true;
    }

    @Override
    public Object readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return readValue(buffer, state, Object.class);
    }

    @Override
    public Object readValue(ProtonBuffer buffer, DecoderState state, Class<?> ofType) throws DecodeException {
        state.increaseDepth();

        try {
            final int size = readSize(buffer, state);

            if (Integer.compareUnsigned(size, buffer.getReadableBytes()) > 0) {
                throw new DecodeException(String.format(
                    "Array size indicated %d is greater than the amount of data available to decode (%d)",
                    Integer.toUnsignedLong(size), buffer.getReadableBytes()));
            }

            return decodeArray(buffer, state, size, ofType);
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public Object readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return readValue(stream, state, Object.class);
    }

    @Override
    public Object readValue(InputStream stream, StreamDecoderState state, Class<?> ofType) throws DecodeException {
        state.increaseDepth();

        try {
            final int size = readSize(stream, state);

            if (Integer.compareUnsigned(size, state.getMaxArraySize()) > 0) {
                throw new DecodeException(String.format(
                        "Array encoded size %d is specified to be greater than the amount " +
                        "of the configured max array size (%d)", Integer.toUnsignedLong(size), state.getMaxArraySize()));
            }

            return decodeArray(stream, state, size, readCount(stream, state), ofType);
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final int size = readSize(buffer, state);

        if (Integer.compareUnsigned(size, buffer.getReadableBytes()) > 0) {
            throw new DecodeException(String.format(
                "Array size indicated %d is greater than the amount of data available to decode (%d)",
                Integer.toUnsignedLong(size), buffer.getReadableBytes()));
        }

        buffer.advanceReadOffset(size);
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final int size = readSize(stream, state);

        if (Integer.compareUnsigned(size, state.getMaxArraySize()) > 0) {
            throw new DecodeException(String.format(
                    "Array encoded size %d is specified to be greater than the amount " +
                    "of the configured max array size (%d)", Integer.toUnsignedLong(size), state.getMaxArraySize()));
        }

        ProtonStreamUtils.skipBytes(stream, size);
    }

    private Object decodeArray(ProtonBuffer buffer, DecoderState state, int size, Class<?> ofType) throws DecodeException {
        final int startOffset = buffer.getReadOffset();
        final int count = readCount(buffer, state);
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);
        final Object result;

        if (!ofType.isAssignableFrom(decoder.getTypeClass())) {
            throw new DecodeException("Unexpected type " + decoder.getTypeClass() + ". " +
                                      "Expected a type assignable to " + ofType.getName() + ".");
        }

        if (decoder instanceof PrimitiveTypeDecoder) {
            final PrimitiveTypeDecoder<?> primitiveTypeDecoder = (PrimitiveTypeDecoder<?>) decoder;

            if (primitiveTypeDecoder.isJavaPrimitive()) {
                result = primitiveTypeDecoder.readPrimitiveArray(buffer, state, count);
            } else {
                result = decodeNonJavaPrimitiveArray(decoder, buffer, state, count);
            }
        } else {
            result = decodeNonJavaPrimitiveArray(decoder, buffer, state, count);
        }

        if (buffer.getReadOffset() - startOffset != size) {
            throw new DecodeException(String.format(
                "Encoded size indicates the array encoding should have been %d bytes but the actual bytes read was %d",
                size, buffer.getReadOffset() - startOffset));
        }

        return result;
    }

    private static Object decodeNonJavaPrimitiveArray(TypeDecoder<?> decoder, ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        if (decoder.isArrayType()) {
            final PrimitiveArrayTypeDecoder arrayDecoder = (PrimitiveArrayTypeDecoder) decoder;

            if (Integer.compareUnsigned(count, buffer.getReadableBytes()) > 0) {
                throw new DecodeException(String.format(
                    "Array encoded element count %d is specified to be greater than the amount " +
                    "of data available (%d)", Integer.toUnsignedLong(count), buffer.getReadableBytes()));
            }

            final Object[] array = new Object[count];
            for (int i = 0; i < count; i++) {
                array[i] = arrayDecoder.readValue(buffer, state);
            }

            return array;
        } else {
            return decoder.readArrayElements(buffer, state, count);
        }
    }

    //----- InputStream based array decoding

    private static Object decodeArray(InputStream stream, StreamDecoderState state, int size, int count, Class<?> ofType) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (!ofType.isAssignableFrom(decoder.getTypeClass())) {
            throw new DecodeException("Unexpected type " + decoder.getTypeClass() + ". " +
                                      "Expected a type assignable to " + ofType.getName() + ".");
        }

        if (decoder instanceof PrimitiveTypeDecoder) {
            final PrimitiveTypeDecoder<?> primitiveTypeDecoder = (PrimitiveTypeDecoder<?>) decoder;

            if (primitiveTypeDecoder.isJavaPrimitive()) {
                return primitiveTypeDecoder.readPrimitiveArray(stream, state, count);
            } else {
                return decodeNonJavaPrimitiveArray(decoder, stream, state, count);
            }
        } else {
            return decodeNonJavaPrimitiveArray(decoder, stream, state, count);
        }
    }

    private static Object[] decodeNonJavaPrimitiveArray(StreamTypeDecoder<?> decoder, InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        if (decoder.isArrayType()) {
            final PrimitiveArrayTypeDecoder arrayDecoder = (PrimitiveArrayTypeDecoder) decoder;

            if (Integer.compareUnsigned(count, state.getMaxArraySize()) > 0) {
                throw new DecodeException(String.format(
                    "Array encoded element count %d is specified to be greater than the amount " +
                    "of data available (%d)", Integer.toUnsignedLong(count), state.getMaxArraySize()));
            }

            final Object[] array = new Object[count];
            for (int i = 0; i < count; i++) {
                array[i] = arrayDecoder.readValue(stream, state);
            }

            return array;
        } else {
            return decoder.readArrayElements(stream, state, count);
        }
    }
}
