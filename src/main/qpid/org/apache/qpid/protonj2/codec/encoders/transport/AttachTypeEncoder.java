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
import org.apache.qpid.protonj2.types.transport.Attach;

/**
 * Encoder of AMQP Attach type values to a byte stream.
 */
public final class AttachTypeEncoder extends AbstractDescribedListTypeEncoder<Attach> {

    public static final AttachTypeEncoder INSTANCE = new AttachTypeEncoder();

    @Override
    public UnsignedLong getDescriptorCode() {
        return Attach.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Attach.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Attach> getTypeClass() {
        return Attach.class;
    }

    @Override
    public byte getListEncoding(Attach value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Attach attach) {
        return attach.getElementCount();
    }

    @Override
    public int getMinElementCount() {
        return 3;
    }

    @Override
    public int getMaxElementCount() {
        return 14;
    }

    @Override
    public void writeElements(Attach attach, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        if (attach.hasName()) {
            ProtonEncodings.writeString(buffer, state, attach.getName());
        } else {
            throw new EncodeException("Cannot write an Attach that does not have a name assigned.");
        }

        if (attach.hasHandle()) {
            ProtonEncodings.writeUnsignedInteger(buffer, attach.getHandle());
        } else {
            throw new EncodeException("Cannot write an Attach that does not have a handle assigned.");
        }

        if (attach.hasRole()) {
            buffer.writeByte(attach.getRole().encodingCode());
        } else {
            throw new EncodeException("Cannot write an Attach that does not have a Role assigned.");
        }

        for (int i = 3; i < count; ++i) {
            if (!attach.hasElement(i)) {
                buffer.writeByte(EncodingCodes.NULL);
                continue;
            }

            switch (i) {
                case 3:
                    buffer.writeByte(EncodingCodes.UBYTE);
                    buffer.writeByte(attach.getSenderSettleMode().byteValue());
                    break;
                case 4:
                    buffer.writeByte(EncodingCodes.UBYTE);
                    buffer.writeByte(attach.getReceiverSettleMode().byteValue());
                    break;
                case 5:
                    encoder.writeObject(buffer, state, attach.getSource());
                    break;
                case 6:
                    encoder.writeObject(buffer, state, attach.getTarget());
                    break;
                case 7:
                    encoder.writeMap(buffer, state, attach.getUnsettled());
                    break;
                case 8:
                    buffer.writeByte(attach.getIncompleteUnsettled() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 9:
                    ProtonEncodings.writeUnsignedInteger(buffer, attach.getInitialDeliveryCount());
                    break;
                case 10:
                    ProtonEncodings.writeUnsignedLong(buffer, attach.getMaxMessageSize().longValue());
                    break;
                case 11:
                    encoder.writeArray(buffer, state, attach.getOfferedCapabilities());
                    break;
                case 12:
                    encoder.writeArray(buffer, state, attach.getDesiredCapabilities());
                    break;
                case 13:
                    encoder.writeMap(buffer, state, attach.getProperties());
                    break;
            }
        }
    }
}
