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
package org.apache.qpid.protonj2.codec.decoders.messaging;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Decoder of AMQP Rejected type values from a byte stream.
 */
public final class RejectedTypeDecoder extends AbstractDescribedListTypeDecoder<Rejected> {

    private static final int MIN_REJECTED_LIST_ENTRIES = 0;
    private static final int MAX_REJECTED_LIST_ENTRIES = 1;

    @Override
    public Class<Rejected> getTypeClass() {
        return Rejected.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Rejected.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Rejected.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Rejected readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readRejected(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Rejected[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Rejected[] result = new Rejected[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readRejected(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Rejected readRejected(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Rejected rejected = new Rejected();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer, state);
        final int count = listDecoder.readCount(buffer, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_REJECTED_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Rejected list encoding: " + count);
        }

        if (count > MAX_REJECTED_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Rejected list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    rejected.setError(state.getDecoder().readObject(buffer, state, ErrorCondition.class));
                    break;
            }
        }

        return rejected;
    }

    @Override
    public Rejected readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readRejected(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Rejected[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Rejected[] result = new Rejected[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readRejected(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Rejected readRejected(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Rejected rejected = new Rejected();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream, state);
        final int count = listDecoder.readCount(stream, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_REJECTED_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Rejected list encoding: " + count);
        }

        if (count > MAX_REJECTED_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Rejected list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    rejected.setError(state.getDecoder().readObject(stream, state, ErrorCondition.class));
                    break;
            }
        }

        return rejected;
    }
}
