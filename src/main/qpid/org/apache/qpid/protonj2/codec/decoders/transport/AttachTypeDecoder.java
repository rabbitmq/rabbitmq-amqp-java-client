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
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Terminus;
import org.apache.qpid.protonj2.types.transport.Attach;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;

/**
 * Decoder of AMQP Attach type values from a byte stream.
 */
public final class AttachTypeDecoder extends AbstractDescribedListTypeDecoder<Attach> {

    public static final AttachTypeDecoder INSTANCE = new AttachTypeDecoder();

    private static final int MIN_ATTACH_LIST_ENTRIES = 3;
    private static final int MAX_ATTACH_LIST_ENTRIES = 14;

    @Override
    public Class<Attach> getTypeClass() {
        return Attach.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Attach.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Attach.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_ATTACH_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_ATTACH_LIST_ENTRIES;
    }

    @Override
    protected Attach readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Attach attach = new Attach();

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            if (buffer.peekByte() == EncodingCodes.NULL) {
                // Ensure mandatory fields are set
                if (index < MIN_ATTACH_LIST_ENTRIES) {
                    throw new DecodeException(errorForMissingRequiredFields(index));
                }

                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    attach.setName(decoder.readString(buffer, state));
                    break;
                case 1:
                    attach.setHandle(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 2:
                    attach.setRole(decoder.readBoolean(buffer, state, false) ? Role.RECEIVER : Role.SENDER);
                    break;
                case 3:
                    attach.setSenderSettleMode(SenderSettleMode.valueOf(decoder.readUnsignedByte(buffer, state, (byte) 2)));
                    break;
                case 4:
                    attach.setReceiverSettleMode(ReceiverSettleMode.valueOf(decoder.readUnsignedByte(buffer, state, (byte) 0)));
                    break;
                case 5:
                    attach.setSource(decoder.readObject(buffer, state, Source.class));
                    break;
                case 6:
                    attach.setTarget(decoder.readObject(buffer, state, Terminus.class));
                    break;
                case 7:
                    attach.setUnsettled(decoder.readMap(buffer, state));
                    break;
                case 8:
                    attach.setIncompleteUnsettled(decoder.readBoolean(buffer, state, true));
                    break;
                case 9:
                    attach.setInitialDeliveryCount(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 10:
                    attach.setMaxMessageSize(decoder.readUnsignedLong(buffer, state));
                    break;
                case 11:
                    attach.setOfferedCapabilities(decoder.readMultiple(buffer, state, Symbol.class));
                    break;
                case 12:
                    attach.setDesiredCapabilities(decoder.readMultiple(buffer, state, Symbol.class));
                    break;
                case 13:
                    attach.setProperties(decoder.readMap(buffer, state));
                    break;
            }
        }

        return attach;
    }

    @Override
    protected Attach readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Attach attach = new Attach();

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                final boolean nullValue = ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL;
                if (nullValue) {
                    // Ensure mandatory fields are set
                    if (index < MIN_ATTACH_LIST_ENTRIES) {
                        throw new DecodeException(errorForMissingRequiredFields(index));
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    attach.setName(decoder.readString(stream, state));
                    break;
                case 1:
                    attach.setHandle(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 2:
                    attach.setRole(decoder.readBoolean(stream, state, false) ? Role.RECEIVER : Role.SENDER);
                    break;
                case 3:
                    attach.setSenderSettleMode(SenderSettleMode.valueOf(decoder.readUnsignedByte(stream, state, (byte) 2)));
                    break;
                case 4:
                    attach.setReceiverSettleMode(ReceiverSettleMode.valueOf(decoder.readUnsignedByte(stream, state, (byte) 0)));
                    break;
                case 5:
                    attach.setSource(decoder.readObject(stream, state, Source.class));
                    break;
                case 6:
                    attach.setTarget(decoder.readObject(stream, state, Terminus.class));
                    break;
                case 7:
                    attach.setUnsettled(decoder.readMap(stream, state));
                    break;
                case 8:
                    attach.setIncompleteUnsettled(decoder.readBoolean(stream, state, true));
                    break;
                case 9:
                    attach.setInitialDeliveryCount(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 10:
                    attach.setMaxMessageSize(decoder.readUnsignedLong(stream, state));
                    break;
                case 11:
                    attach.setOfferedCapabilities(decoder.readMultiple(stream, state, Symbol.class));
                    break;
                case 12:
                    attach.setDesiredCapabilities(decoder.readMultiple(stream, state, Symbol.class));
                    break;
                case 13:
                    attach.setProperties(decoder.readMap(stream, state));
                    break;
            }
        }

        return attach;
    }

    private String errorForMissingRequiredFields(int present) {
        switch (present) {
            case 2:
                return "The role field cannot be omitted from the Attach";
            case 1:
                return "The handle field cannot be omitted from the Attach";
            default:
                return "The name field cannot be omitted from the Attach";
        }
    }
}
