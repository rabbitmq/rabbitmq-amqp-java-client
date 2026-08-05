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
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.security.SaslChallenge;

/**
 * Decoder of AMQP SaslChallenge type values from a byte stream.
 */
public final class SaslChallengeTypeDecoder extends AbstractDescribedListTypeDecoder<SaslChallenge> {

    public static final SaslChallengeTypeDecoder INSTANCE = new SaslChallengeTypeDecoder();

    private static final int REQUIRED_LIST_ENTRIES = 1;

    @Override
    public UnsignedLong getDescriptorCode() {
        return SaslChallenge.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return SaslChallenge.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<SaslChallenge> getTypeClass() {
        return SaslChallenge.class;
    }

    @Override
    protected final int getMinListElements() {
        return REQUIRED_LIST_ENTRIES;
    }

    @Override
    protected final int getMaxListElements() {
        return REQUIRED_LIST_ENTRIES;
    }

    @Override
    protected SaslChallenge readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final SaslChallenge challenge = new SaslChallenge();
        final ProtonBuffer challenegeBuffer = state.getDecoder().readBinaryAsBuffer(buffer, state);

        if (challenegeBuffer == null) {
            throw new DecodeException("The challenge field cannot be omitted from the SaslChallenge");
        }

        return challenge.setChallenge(challenegeBuffer);
    }

    @Override
    protected SaslChallenge readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final SaslChallenge challenge = new SaslChallenge();
        final ProtonBuffer challenegeBuffer = state.getDecoder().readBinaryAsBuffer(stream, state);

        if (challenegeBuffer == null) {
            throw new DecodeException("The challenge field cannot be omitted from the SaslChallenge");
        }

        return challenge.setChallenge(challenegeBuffer);
    }
}
