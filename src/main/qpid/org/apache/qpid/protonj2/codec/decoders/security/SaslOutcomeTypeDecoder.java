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
package org.apache.qpid.protonj2.codec.decoders.security;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.security.SaslCode;
import org.apache.qpid.protonj2.types.security.SaslOutcome;

/**
 * Decoder of AMQP SaslOutcome type values from a byte stream.
 */
public final class SaslOutcomeTypeDecoder extends AbstractDescribedListTypeDecoder<SaslOutcome> {

    public static final SaslOutcomeTypeDecoder INSTANCE = new SaslOutcomeTypeDecoder();

    private static final int MIN_SASL_OUTCOME_LIST_ENTRIES = 1;
    private static final int MAX_SASL_OUTCOME_LIST_ENTRIES = 2;

    @Override
    public UnsignedLong getDescriptorCode() {
        return SaslOutcome.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return SaslOutcome.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<SaslOutcome> getTypeClass() {
        return SaslOutcome.class;
    }

    @Override
    protected final int getMinListElements() {
        return MIN_SASL_OUTCOME_LIST_ENTRIES;
    }

    @Override
    protected final int getMaxListElements() {
        return MAX_SASL_OUTCOME_LIST_ENTRIES;
    }

    @Override
    protected SaslOutcome readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final SaslOutcome outcome = new SaslOutcome();
        final UnsignedByte code = state.getDecoder().readUnsignedByte(buffer, state);

        if (code == null) {
            throw new DecodeException("The code field cannot be omitted from the SaslOutcome");
        }

        outcome.setCode(SaslCode.valueOf(code));

        if (count == 2) {
            outcome.setAdditionalData(state.getDecoder().readBinaryAsBuffer(buffer, state));
        }

        return outcome;
    }

    @Override
    protected SaslOutcome readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final SaslOutcome outcome = new SaslOutcome();
        final UnsignedByte code = state.getDecoder().readUnsignedByte(stream, state);

        if (code == null) {
            throw new DecodeException("The code field cannot be omitted from the SaslOutcome");
        }

        outcome.setCode(SaslCode.valueOf(code));

        if (count == 2) {
            outcome.setAdditionalData(state.getDecoder().readBinaryAsBuffer(stream, state));
        }

        return outcome;
    }
}
