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
import org.apache.qpid.protonj2.types.security.SaslResponse;

/**
 * Decoder of AMQP SaslResponse type values from a byte stream.
 */
public final class SaslResponseTypeDecoder extends AbstractDescribedListTypeDecoder<SaslResponse> {

    public static final SaslResponseTypeDecoder INSTANCE = new SaslResponseTypeDecoder();

    private static final int REQUIRED_LIST_ENTRIES = 1;

    @Override
    public UnsignedLong getDescriptorCode() {
        return SaslResponse.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return SaslResponse.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<SaslResponse> getTypeClass() {
        return SaslResponse.class;
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
    protected SaslResponse readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final SaslResponse response = new SaslResponse();
        final ProtonBuffer responseBuffer = state.getDecoder().readBinaryAsBuffer(buffer, state);

        if (responseBuffer == null) {
            throw new DecodeException("The response field cannot be omitted from the SaslResponse");
        }

        return response.setResponse(responseBuffer);
    }

    @Override
    protected SaslResponse readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final SaslResponse response = new SaslResponse();
        final ProtonBuffer responseBuffer = state.getDecoder().readBinaryAsBuffer(stream, state);

        if (responseBuffer == null) {
            throw new DecodeException("The response field cannot be omitted from the SaslResponse");
        }

        return response.setResponse(responseBuffer);
    }
}
