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
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.Begin;

/**
 * Decoder of AMQP Begin type values from a byte stream
 */
public final class BeginTypeDecoder extends AbstractDescribedListTypeDecoder<Begin> {

    public static final BeginTypeDecoder INSTANCE = new BeginTypeDecoder();

    private static final int MIN_BEGIN_LIST_ENTRIES = 4;
    private static final int MAX_BEGIN_LIST_ENTRIES = 8;

    @Override
    public Class<Begin> getTypeClass() {
        return Begin.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Begin.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Begin.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_BEGIN_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_BEGIN_LIST_ENTRIES;
    }

    @Override
    protected Begin readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Begin begin = new Begin();

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            if (buffer.peekByte() == EncodingCodes.NULL) {
                // Ensure mandatory fields are set
                if (index > 0 && index < MIN_BEGIN_LIST_ENTRIES) {
                    throw new DecodeException(errorForMissingRequiredFields(index));
                }

                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    begin.setRemoteChannel(decoder.readUnsignedShort(buffer, state, 0));
                    break;
                case 1:
                    begin.setNextOutgoingId(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 2:
                    begin.setIncomingWindow(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 3:
                    begin.setOutgoingWindow(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 4:
                    begin.setHandleMax(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 5:
                    begin.setOfferedCapabilities(decoder.readMultiple(buffer, state, Symbol.class));
                    break;
                case 6:
                    begin.setDesiredCapabilities(decoder.readMultiple(buffer, state, Symbol.class));
                    break;
                case 7:
                    begin.setProperties(decoder.readMap(buffer, state));
                    break;
            }
        }

        return begin;
    }

    @Override
    protected Begin readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Begin begin = new Begin();

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                final boolean nullValue = ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL;
                if (nullValue) {
                    // Ensure mandatory fields are set
                    if (index > 0 && index < MIN_BEGIN_LIST_ENTRIES) {
                        throw new DecodeException(errorForMissingRequiredFields(index));
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    begin.setRemoteChannel(decoder.readUnsignedShort(stream, state, 0));
                    break;
                case 1:
                    begin.setNextOutgoingId(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 2:
                    begin.setIncomingWindow(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 3:
                    begin.setOutgoingWindow(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 4:
                    begin.setHandleMax(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 5:
                    begin.setOfferedCapabilities(decoder.readMultiple(stream, state, Symbol.class));
                    break;
                case 6:
                    begin.setDesiredCapabilities(decoder.readMultiple(stream, state, Symbol.class));
                    break;
                case 7:
                    begin.setProperties(decoder.readMap(stream, state));
                    break;
            }
        }

        return begin;
    }

    private String errorForMissingRequiredFields(int present) {
        switch (present) {
            case 3:
                return "The outgoing-window field cannot be omitted from the Begin";
            case 2:
                return "The incoming-window field cannot be omitted from the Begin";
            default:
                return "The next-outgoing-id field cannot be omitted from the Begin";
        }
    }
}
