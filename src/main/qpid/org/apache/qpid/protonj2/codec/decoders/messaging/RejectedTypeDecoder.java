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
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Decoder of AMQP Rejected type values from a byte stream.
 */
public final class RejectedTypeDecoder extends AbstractDescribedListTypeDecoder<Rejected> {

    public static final RejectedTypeDecoder INSTANCE = new RejectedTypeDecoder();

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
    protected int getMinListElements() {
        return MIN_REJECTED_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_REJECTED_LIST_ENTRIES;
    }

    @Override
    protected Rejected readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Rejected rejected = new Rejected();

        if (count == 1) {
            rejected.setError(state.getDecoder().readObject(buffer, state, ErrorCondition.class));
        }

        return rejected;
    }

    @Override
    protected Rejected readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Rejected rejected = new Rejected();

        if (count == 1) {
            rejected.setError(state.getDecoder().readObject(stream, state, ErrorCondition.class));
        }

        return rejected;
    }
}
