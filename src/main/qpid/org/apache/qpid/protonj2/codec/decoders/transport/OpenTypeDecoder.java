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
import org.apache.qpid.protonj2.types.transport.Open;

/**
 * Decoder of AMQP Open type values from a byte stream.
 */
public final class OpenTypeDecoder extends AbstractDescribedListTypeDecoder<Open> {

    public static final OpenTypeDecoder INSTANCE = new OpenTypeDecoder();

    private static final int MIN_OPEN_LIST_ENTRIES = 1;
    private static final int MAX_OPEN_LIST_ENTRIES = 10;

    @Override
    public Class<Open> getTypeClass() {
        return Open.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Open.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Open.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_OPEN_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_OPEN_LIST_ENTRIES;
    }

    @Override
    protected Open readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Open open = new Open();

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            if (buffer.peekByte() == EncodingCodes.NULL) {
                if (index == 0) {
                    throw new DecodeException("The container-id field cannot be omitted from the Open");
                }

                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    open.setContainerId(decoder.readString(buffer, state));
                    break;
                case 1:
                    open.setHostname(decoder.readString(buffer, state));
                    break;
                case 2:
                    open.setMaxFrameSize(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 3:
                    open.setChannelMax(decoder.readUnsignedShort(buffer, state, 0));
                    break;
                case 4:
                    open.setIdleTimeout(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 5:
                    open.setOutgoingLocales(decoder.readMultiple(buffer, state, Symbol.class));
                    break;
                case 6:
                    open.setIncomingLocales(decoder.readMultiple(buffer, state, Symbol.class));
                    break;
                case 7:
                    open.setOfferedCapabilities(decoder.readMultiple(buffer, state, Symbol.class));
                    break;
                case 8:
                    open.setDesiredCapabilities(decoder.readMultiple(buffer, state, Symbol.class));
                    break;
                case 9:
                    open.setProperties(decoder.readMap(buffer, state));
                    break;
            }
        }

        return open;
    }

    @Override
    protected Open readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Open open = new Open();

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                final boolean nullValue = ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL;
                if (nullValue) {
                    if (index == 0) {
                        throw new DecodeException("The container-id field cannot be omitted from the Open");
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    open.setContainerId(decoder.readString(stream, state));
                    break;
                case 1:
                    open.setHostname(decoder.readString(stream, state));
                    break;
                case 2:
                    open.setMaxFrameSize(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 3:
                    open.setChannelMax(decoder.readUnsignedShort(stream, state, 0));
                    break;
                case 4:
                    open.setIdleTimeout(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 5:
                    open.setOutgoingLocales(decoder.readMultiple(stream, state, Symbol.class));
                    break;
                case 6:
                    open.setIncomingLocales(decoder.readMultiple(stream, state, Symbol.class));
                    break;
                case 7:
                    open.setOfferedCapabilities(decoder.readMultiple(stream, state, Symbol.class));
                    break;
                case 8:
                    open.setDesiredCapabilities(decoder.readMultiple(stream, state, Symbol.class));
                    break;
                case 9:
                    open.setProperties(decoder.readMap(stream, state));
                    break;
            }
        }

        return open;
    }
}
