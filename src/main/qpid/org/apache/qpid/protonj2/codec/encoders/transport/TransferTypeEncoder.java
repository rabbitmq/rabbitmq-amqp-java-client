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
package org.apache.qpid.protonj2.codec.encoders.transport;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncodeException;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncodings;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Encoder of AMQP Transfer type values to a byte stream.
 */
public final class TransferTypeEncoder extends AbstractDescribedListTypeEncoder<Transfer> {

    public static final TransferTypeEncoder INSTANCE = new TransferTypeEncoder();

    @Override
    public UnsignedLong getDescriptorCode() {
        return Transfer.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Transfer.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Transfer> getTypeClass() {
        return Transfer.class;
    }

    @Override
    public int getElementCount(Transfer transfer) {
        return transfer.getElementCount();
    }

    @Override
    public int getMinElementCount() {
        return 1;
    }

    @Override
    public int getMaxElementCount() {
        return 11;
    }

    @Override
    public byte getListEncoding(Transfer value) {
        if (value.getState() != null) {
            return EncodingCodes.LIST32;
        } else if (value.getDeliveryTag() != null && value.getDeliveryTag().tagLength() > 200) {
            return EncodingCodes.LIST32;
        } else {
            return EncodingCodes.LIST8;
        }
    }

    @Override
    public void writeElements(Transfer transfer, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        if (transfer.hasHandle()) {
            ProtonEncodings.writeUnsignedInteger(buffer, transfer.getHandle());
        } else {
            throw new EncodeException("Cannot write an Transfer that does not have a handle assigned.");
        }

        if (count >= 2) {
            if (transfer.hasDeliveryId()) {
                ProtonEncodings.writeUnsignedInteger(buffer, transfer.getDeliveryId());
            } else {
                buffer.writeByte(EncodingCodes.NULL);
            }
        } else {
            return;
        }

        if (count >= 3) {
            if (transfer.hasDeliveryTag()) {
                ProtonEncodings.writeDeliveryTag(buffer, transfer.getDeliveryTag());
            } else {
                buffer.writeByte(EncodingCodes.NULL);
            }
        } else {
            return;
        }

        if (count >= 4) {
            if (transfer.hasMessageFormat()) {
                ProtonEncodings.writeUnsignedInteger(buffer, transfer.getMessageFormat());
            } else {
                buffer.writeByte(EncodingCodes.NULL);
            }
        } else {
            return;
        }

        for (int i = 4; i < count; ++i) {
            if (!transfer.hasElement(i)) {
                buffer.writeByte(EncodingCodes.NULL);
                continue;
            }

            switch (i) {
                case 4:
                    buffer.writeByte(transfer.getSettled() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 5:
                    buffer.writeByte(transfer.getMore() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 6:
                    buffer.writeByte(EncodingCodes.UBYTE);
                    buffer.writeByte(transfer.getRcvSettleMode().byteValue());
                    break;
                case 7:
                    encoder.writeObject(buffer, state, transfer.getState());
                    break;
                case 8:
                    buffer.writeByte(transfer.getResume() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 9:
                    buffer.writeByte(transfer.getAborted() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 10:
                    buffer.writeByte(transfer.getBatchable() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
            }
        }
    }
}
