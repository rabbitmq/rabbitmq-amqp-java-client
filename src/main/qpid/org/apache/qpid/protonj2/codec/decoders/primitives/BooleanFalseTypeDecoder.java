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

import static org.apache.qpid.protonj2.codec.decoders.PrimitiveArrayTypeDecoder.validateArrayConstraints;

import java.io.InputStream;
import java.util.Arrays;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;

/**
 * Decoder of AMQP Boolean False values from a byte stream.
 */
public final class BooleanFalseTypeDecoder extends BooleanTypeDecoder {

    public static final BooleanFalseTypeDecoder INSTANCE = new BooleanFalseTypeDecoder();

    @Override
    public Boolean readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return Boolean.FALSE;
    }

    @Override
    public Boolean readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return Boolean.FALSE;
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.BOOLEAN_FALSE & 0xff;
    }

    @Override
    public boolean isZeroWidth() {
        return true;
    }

    @Override
    public boolean readPrimitiveValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return false;
    }

    @Override
    public boolean readPrimitiveValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return false;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
    }

    @Override
    public int readSize(ProtonBuffer buffer, DecoderState state) {
        return 0;
    }

    @Override
    public int readSize(InputStream stream, StreamDecoderState state) {
        return 0;
    }

    @Override
    public Boolean[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        validateArrayConstraints(count, buffer, state, this);

        final Boolean[] array = new Boolean[count];

        Arrays.fill(array, Boolean.FALSE);

        return array;
    }

    @Override
    public Boolean[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        validateArrayConstraints(count, stream, state, this);

        final Boolean[] array = new Boolean[count];

        Arrays.fill(array, Boolean.FALSE);

        return array;
    }

    @Override
    public boolean[] readPrimitiveArray(ProtonBuffer buffer, DecoderState state, int count) {
        validateArrayConstraints(count, buffer, state, this);

        final boolean[] array = new boolean[count];

        Arrays.fill(array, Boolean.FALSE.booleanValue());

        return array;
    }

    @Override
    public boolean[] readPrimitiveArray(InputStream stream, StreamDecoderState state, int count) {
        validateArrayConstraints(count, stream, state, this);

        final boolean[] array = new boolean[count];

        Arrays.fill(array, Boolean.FALSE.booleanValue());

        return array;
    }
}
