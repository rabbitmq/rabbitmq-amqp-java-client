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
package org.apache.qpid.protonj2.codec.decoders.transactions;

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
import org.apache.qpid.protonj2.types.transactions.Declare;
import org.apache.qpid.protonj2.types.transactions.GlobalTxId;

/**
 * Decoder of AMQP Declare type values from a byte stream
 */
public final class DeclareTypeDecoder extends AbstractDescribedListTypeDecoder<Declare> {

    private static final int MIN_DECLARE_LIST_ENTRIES = 0;
    private static final int MAX_DECLARE_LIST_ENTRIES = 1;

    @Override
    public Class<Declare> getTypeClass() {
        return Declare.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Declare.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Declare.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Declare readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readDeclare(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Declare[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Declare[] result = new Declare[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDeclare(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Declare readDeclare(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Declare declare = new Declare();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer, state);
        final int count = listDecoder.readCount(buffer, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_DECLARE_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Declare list encoding: " + count);
        } else if (count > MAX_DECLARE_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Declare list encoding: " + count);
        } else if (count == 1) {
            declare.setGlobalId(state.getDecoder().readObject(buffer, state, GlobalTxId.class));
        }

        return declare;
    }

    @Override
    public Declare readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readDeclare(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Declare[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Declare[] result = new Declare[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDeclare(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Declare readDeclare(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Declare declare = new Declare();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream, state);
        final int count = listDecoder.readCount(stream, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_DECLARE_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Declare list encoding: " + count);
        } else if (count > MAX_DECLARE_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Declare list encoding: " + count);
        } else if (count == 1) {
            declare.setGlobalId(state.getDecoder().readObject(stream, state, GlobalTxId.class));
        }

        return declare;
    }
}
