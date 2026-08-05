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
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Header;

/**
 * Decoder of AMQP Header types from a byte stream
 */
public final class HeaderTypeDecoder extends AbstractDescribedListTypeDecoder<Header> {

    public static final HeaderTypeDecoder INSTANCE = new HeaderTypeDecoder();

    private static final int MIN_HEADER_LIST_ENTRIES = 0;
    private static final int MAX_HEADER_LIST_ENTRIES = 5;

    @Override
    public Class<Header> getTypeClass() {
        return Header.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Header.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Header.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_HEADER_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_HEADER_LIST_ENTRIES;
    }

    @Override
    protected Header readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Header header = new Header();

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            if (buffer.peekByte() == EncodingCodes.NULL) {
                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    header.setDurable(decoder.readBoolean(buffer, state, false));
                    break;
                case 1:
                    header.setPriority(decoder.readUnsignedByte(buffer, state, Header.DEFAULT_PRIORITY));
                    break;
                case 2:
                    header.setTimeToLive(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 3:
                    header.setFirstAcquirer(decoder.readBoolean(buffer, state, false));
                    break;
                case 4:
                    header.setDeliveryCount(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
            }
        }

        return header;
    }

    @Override
    protected Header readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Header header = new Header();

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                if (ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL) {
                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    header.setDurable(decoder.readBoolean(stream, state, false));
                    break;
                case 1:
                    header.setPriority(decoder.readUnsignedByte(stream, state, Header.DEFAULT_PRIORITY));
                    break;
                case 2:
                    header.setTimeToLive(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 3:
                    header.setFirstAcquirer(decoder.readBoolean(stream, state, false));
                    break;
                case 4:
                    header.setDeliveryCount(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
            }
        }

        return header;
    }
}
