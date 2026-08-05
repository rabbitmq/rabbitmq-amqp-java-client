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
package org.apache.qpid.protonj2.codec.decoders.transport;

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
import org.apache.qpid.protonj2.types.transport.Close;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Decoder of AMQP Close type values from a byte stream
 */
public final class CloseTypeDecoder extends AbstractDescribedListTypeDecoder<Close> {

    public static final CloseTypeDecoder INSTANCE = new CloseTypeDecoder();

    private static final int MIN_CLOSE_LIST_ENTRIES = 0;
    private static final int MAX_CLOSE_LIST_ENTRIES = 1;

    @Override
    public Class<Close> getTypeClass() {
        return Close.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Close.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Close.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_CLOSE_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_CLOSE_LIST_ENTRIES;
    }

    @Override
    protected Close readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Close close = new Close();

        if (count == 1) {
            close.setError(state.getDecoder().readObject(buffer, state, ErrorCondition.class));
        }

        return close;
    }

    @Override
    protected Close readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Close close = new Close();

        if (count == 1) {
            close.setError(state.getDecoder().readObject(stream, state, ErrorCondition.class));
        }

        return close;
    }
}
