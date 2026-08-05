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
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.apache.qpid.protonj2.types.Binary;

/**
 * Base for all Binary type value decoders.
 */
public interface BinaryTypeDecoder extends PrimitiveTypeDecoder<Binary> {

    @Override
    default Class<Binary> getTypeClass() {
        return Binary.class;
    }

    /**
     * Read and decode an AMQP {@link Binary} into a {@link ProtonBuffer} instance and return it.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the {@link Binary} encoding should be read from.
     * @param state
     * 		The {@link EncoderState} that can be used when reading the encoded value.
     *
     * @return the bytes that comprise the encoded Binary wrapped in a {@link ProtonBuffer} instance.
     *
     * @throws DecodeException if an error occurs while reading the Binary value.
     */
    public ProtonBuffer readValueAsBuffer(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Read and decode an AMQP {@link Binary} into a byte array instance and return it.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the {@link Binary} encoding should be read from.
     * @param state
     * 		The {@link EncoderState} that can be used when reading the encoded value.
     *
     * @return the bytes that comprise the encoded Binary copied into a byte array instance.
     *
     * @throws DecodeException if an error occurs while reading the Binary value.
     */
    public byte[] readValueAsArray(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Read and decode an AMQP {@link Binary} into a {@link ProtonBuffer} instance and return it.
     *
     * @param stream
     * 		The {@link InputStream} where the {@link Binary} encoding should be read from.
     * @param state
     * 		The {@link EncoderState} that can be used when reading the encoded value.
     *
     * @return the bytes that comprise the encoded Binary wrapped in a {@link ProtonBuffer} instance.
     *
     * @throws DecodeException if an error occurs while reading the Binary value.
     */
    public ProtonBuffer readValueAsBuffer(InputStream stream, StreamDecoderState state) throws DecodeException;

    /**
     * Read and decode an AMQP {@link Binary} into a byte array instance and return it.
     *
     * @param stream
     * 		The {@link InputStream} where the {@link Binary} encoding should be read from.
     * @param state
     * 		The {@link EncoderState} that can be used when reading the encoded value.
     *
     * @return the bytes that comprise the encoded Binary copied into a byte array instance.
     *
     * @throws DecodeException if an error occurs while reading the Binary value.
     */
    public byte[] readValueAsArray(InputStream stream, StreamDecoderState state) throws DecodeException;

}