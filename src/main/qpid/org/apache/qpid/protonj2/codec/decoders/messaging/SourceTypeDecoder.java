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
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Outcome;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.apache.qpid.protonj2.types.messaging.TerminusExpiryPolicy;

/**
 * Decoder of AMQP Source type values from a byte stream.
 */
public final class SourceTypeDecoder extends AbstractDescribedListTypeDecoder<Source> {

    private static final int MIN_SOURCE_LIST_ENTRIES = 0;
    private static final int MAX_SOURCE_LIST_ENTRIES = 11;

    @Override
    public Class<Source> getTypeClass() {
        return Source.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Source.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Source.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Source readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readSource(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Source[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Source[] result = new Source[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readSource(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Source readSource(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Source source = new Source();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer, state);
        final int count = listDecoder.readCount(buffer, state);

        if (count < MIN_SOURCE_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Source list encoding: " + count);
        }

        if (count > MAX_SOURCE_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Source list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    source.setAddress(state.getDecoder().readString(buffer, state));
                    break;
                case 1:
                    final long durability = state.getDecoder().readUnsignedInteger(buffer, state, 0);
                    source.setDurable(TerminusDurability.valueOf(durability));
                    break;
                case 2:
                    final Symbol expiryPolicy = state.getDecoder().readSymbol(buffer, state);
                    source.setExpiryPolicy(expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : TerminusExpiryPolicy.valueOf(expiryPolicy));
                    break;
                case 3:
                    final UnsignedInteger timeout = state.getDecoder().readUnsignedInteger(buffer, state);
                    source.setTimeout(timeout == null ? UnsignedInteger.ZERO : timeout);
                    break;
                case 4:
                    source.setDynamic(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 5:
                    source.setDynamicNodeProperties(state.getDecoder().readMap(buffer, state));
                    break;
                case 6:
                    source.setDistributionMode(state.getDecoder().readSymbol(buffer, state));
                    break;
                case 7:
                    source.setFilter(state.getDecoder().readMap(buffer, state));
                    break;
                case 8:
                    source.setDefaultOutcome(state.getDecoder().readObject(buffer, state, Outcome.class));
                    break;
                case 9:
                    source.setOutcomes(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 10:
                    source.setCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
            }
        }

        return source;
    }

    @Override
    public Source readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readSource(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Source[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Source[] result = new Source[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readSource(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Source readSource(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Source source = new Source();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream, state);
        final int count = listDecoder.readCount(stream, state);

        if (count < MIN_SOURCE_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Source list encoding: " + count);
        }

        if (count > MAX_SOURCE_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Source list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    source.setAddress(state.getDecoder().readString(stream, state));
                    break;
                case 1:
                    final long durability = state.getDecoder().readUnsignedInteger(stream, state, 0);
                    source.setDurable(TerminusDurability.valueOf(durability));
                    break;
                case 2:
                    final Symbol expiryPolicy = state.getDecoder().readSymbol(stream, state);
                    source.setExpiryPolicy(expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : TerminusExpiryPolicy.valueOf(expiryPolicy));
                    break;
                case 3:
                    final UnsignedInteger timeout = state.getDecoder().readUnsignedInteger(stream, state);
                    source.setTimeout(timeout == null ? UnsignedInteger.ZERO : timeout);
                    break;
                case 4:
                    source.setDynamic(state.getDecoder().readBoolean(stream, state, false));
                    break;
                case 5:
                    source.setDynamicNodeProperties(state.getDecoder().readMap(stream, state));
                    break;
                case 6:
                    source.setDistributionMode(state.getDecoder().readSymbol(stream, state));
                    break;
                case 7:
                    source.setFilter(state.getDecoder().readMap(stream, state));
                    break;
                case 8:
                    source.setDefaultOutcome(state.getDecoder().readObject(stream, state, Outcome.class));
                    break;
                case 9:
                    source.setOutcomes(state.getDecoder().readMultiple(stream, state, Symbol.class));
                    break;
                case 10:
                    source.setCapabilities(state.getDecoder().readMultiple(stream, state, Symbol.class));
                    break;
            }
        }

        return source;
    }
}
