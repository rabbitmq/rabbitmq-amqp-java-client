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
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Decoder of AMQP Transfer type values from a byte stream
 */
public final class TransferTypeDecoder extends AbstractDescribedListTypeDecoder<Transfer> {

    public static final TransferTypeDecoder INSTANCE = new TransferTypeDecoder();

    private static final int MIN_TRANSFER_LIST_ENTRIES = 1;
    private static final int MAX_TRANSFER_LIST_ENTRIES = 11;

    @Override
    public Class<Transfer> getTypeClass() {
        return Transfer.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Transfer.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Transfer.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_TRANSFER_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_TRANSFER_LIST_ENTRIES;
    }

    @Override
    protected Transfer readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Transfer transfer = new Transfer();

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            if (buffer.peekByte() == EncodingCodes.NULL) {
                if (index == 0) {
                    throw new DecodeException("The handle field cannot be omitted from the Transfer");
                }

                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    transfer.setHandle(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 1:
                    transfer.setDeliveryId(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 2:
                    transfer.setDeliveryTag(decoder.readDeliveryTag(buffer, state));
                    break;
                case 3:
                    transfer.setMessageFormat(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 4:
                    transfer.setSettled(decoder.readBoolean(buffer, state, false));
                    break;
                case 5:
                    transfer.setMore(decoder.readBoolean(buffer, state, false));
                    break;
                case 6:
                    final UnsignedByte rcvSettleMode = decoder.readUnsignedByte(buffer, state);
                    transfer.setRcvSettleMode(rcvSettleMode == null ? null : ReceiverSettleMode.values()[rcvSettleMode.intValue()]);
                    break;
                case 7:
                    transfer.setState(decoder.readObject(buffer, state, DeliveryState.class));
                    break;
                case 8:
                    transfer.setResume(decoder.readBoolean(buffer, state, false));
                    break;
                case 9:
                    transfer.setAborted(decoder.readBoolean(buffer, state, false));
                    break;
                case 10:
                    transfer.setBatchable(decoder.readBoolean(buffer, state, false));
                    break;
            }
        }

        return transfer;
    }

    @Override
    protected Transfer readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Transfer transfer = new Transfer();

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                final boolean nullValue = ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL;
                if (nullValue) {
                    if (index == 0) {
                        throw new DecodeException("The handle field cannot be omitted from the Transfer");
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    transfer.setHandle(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 1:
                    transfer.setDeliveryId(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 2:
                    transfer.setDeliveryTag(decoder.readDeliveryTag(stream, state));
                    break;
                case 3:
                    transfer.setMessageFormat(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 4:
                    transfer.setSettled(decoder.readBoolean(stream, state, false));
                    break;
                case 5:
                    transfer.setMore(decoder.readBoolean(stream, state, false));
                    break;
                case 6:
                    final UnsignedByte rcvSettleMode = decoder.readUnsignedByte(stream, state);
                    transfer.setRcvSettleMode(rcvSettleMode == null ? null : ReceiverSettleMode.values()[rcvSettleMode.intValue()]);
                    break;
                case 7:
                    transfer.setState(decoder.readObject(stream, state, DeliveryState.class));
                    break;
                case 8:
                    transfer.setResume(decoder.readBoolean(stream, state, false));
                    break;
                case 9:
                    transfer.setAborted(decoder.readBoolean(stream, state, false));
                    break;
                case 10:
                    transfer.setBatchable(decoder.readBoolean(stream, state, false));
                    break;
            }
        }

        return transfer;
    }
}
