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

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;

/**
 * Interface for a TypeDecoder that manages decoding of AMQP primitive types.
 *
 * @param <V> the Type Class that this decoder manages.
 */
public interface PrimitiveTypeDecoder<V> extends TypeDecoder<V>, StreamTypeDecoder<V> {

    @Override
    default boolean isPrimitive() {
        return true;
    }

    /**
     * {@return true if the underlying type is a zero width type that has no encoding beyond the encoding code}
     */
    default boolean isZeroWidth() {
        return false;
    }

    /**
     * {@return true if the type managed by this decoder is assignable to a Java primitive type}
     */
    boolean isJavaPrimitive();

    /**
     * {@return the AMQP Encoding Code that this primitive type decoder can read}
     */
    int getTypeCode();

    /**
     * Read and return a Java primitive type array from the given stream of bytes up to
     * the given count of elements.
     *
     * @param buffer
     * 		The source of bytes to read the primitive type from.
     * @param state
     * 		The decoder state used when performing the decode operation.
     * @param count
     * 		The number of elements that comprise the array to read.
     *
     * @return an array made up of Java primitive values stored in the primitive typed array.
     *
     * @throws DecodeException if an error occurs or this type is not a Java primitive type decoder.
     */
    default Object readPrimitiveArray(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        throw new UnsupportedOperationException("This type is not a Java primitive type");
    }

    /**
     * Read and return a Java primitive type array from the given stream of bytes up to
     * the given count of elements.
     *
     * @param stream
     * 		The source of bytes to read the primitive type from.
     * @param state
     * 		The decoder state used when performing the decode operation.
     * @param count
     * 		The number of elements that comprise the array to read.
     *
     * @return an array made up of Java primitive values stored in the primitive typed array.
     *
     * @throws DecodeException if an error occurs or this type is not a Java primitive type decoder.
     */
    default Object readPrimitiveArray(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        throw new UnsupportedOperationException("This type is not a Java primitive type");
    }
}
