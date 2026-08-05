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
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.security.SaslInit;

/**
 * Decoder of AMQP SaslInit type values from a byte stream.
 */
public final class SaslInitTypeDecoder extends AbstractDescribedListTypeDecoder<SaslInit> {

    public static final SaslInitTypeDecoder INSTANCE = new SaslInitTypeDecoder();

    private static final int MIN_SASL_INIT_LIST_ENTRIES = 1;
    private static final int MAX_SASL_INIT_LIST_ENTRIES = 3;

    @Override
    public UnsignedLong getDescriptorCode() {
        return SaslInit.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return SaslInit.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<SaslInit> getTypeClass() {
        return SaslInit.class;
    }

    @Override
    protected final int getMinListElements() {
        return MIN_SASL_INIT_LIST_ENTRIES;
    }

    @Override
    protected final int getMaxListElements() {
        return MAX_SASL_INIT_LIST_ENTRIES;
    }

    @Override
    protected SaslInit readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final SaslInit init = new SaslInit();

        for (int index = 0; index < count; ++index) {
            if (buffer.peekByte() == EncodingCodes.NULL) {
                if (index < MIN_SASL_INIT_LIST_ENTRIES) {
                    throw new DecodeException("The mechanism field cannot be omitted from the SaslInit");
                }

                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    init.setMechanism(state.getDecoder().readSymbol(buffer, state));
                    break;
                case 1:
                    init.setInitialResponse(state.getDecoder().readBinaryAsBuffer(buffer, state));
                    break;
                case 2:
                    init.setHostname(state.getDecoder().readString(buffer, state));
                    break;
            }
        }

        return init;
    }

    @Override
    protected SaslInit readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final SaslInit init = new SaslInit();

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                final boolean nullValue = ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL;
                if (nullValue) {
                    if (index < MIN_SASL_INIT_LIST_ENTRIES) {
                        throw new DecodeException("The mechanism field cannot be omitted from the SaslInit");
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    init.setMechanism(state.getDecoder().readSymbol(stream, state));
                    break;
                case 1:
                    init.setInitialResponse(state.getDecoder().readBinaryAsBuffer(stream, state));
                    break;
                case 2:
                    init.setHostname(state.getDecoder().readString(stream, state));
                    break;
            }
        }

        return init;
    }
}
