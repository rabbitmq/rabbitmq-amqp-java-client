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
import org.apache.qpid.protonj2.types.messaging.Received;

/**
 * Decoder of AMQP Received type value from a byte stream.
 */
public final class ReceivedTypeDecoder extends AbstractDescribedListTypeDecoder<Received> {

    public static final ReceivedTypeDecoder INSTANCE = new ReceivedTypeDecoder();

    private static final int REQUIRED_RECEIVED_LIST_ENTRIES = 2;

    @Override
    public Class<Received> getTypeClass() {
        return Received.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Received.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Received.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return REQUIRED_RECEIVED_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return REQUIRED_RECEIVED_LIST_ENTRIES;
    }

    @Override
    protected Received readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Received received = new Received();

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    received.setSectionNumber(decoder.readUnsignedInteger(buffer, state));
                    break;
                case 1:
                    received.setSectionOffset(decoder.readUnsignedLong(buffer, state));
                    break;
            }
        }

        return received;
    }

    @Override
    protected Received readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Received received = new Received();

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    received.setSectionNumber(decoder.readUnsignedInteger(stream, state));
                    break;
                case 1:
                    received.setSectionOffset(decoder.readUnsignedLong(stream, state));
                    break;
            }
        }

        return received;
    }
}
