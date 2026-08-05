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
import org.apache.qpid.protonj2.types.transport.Begin;

/**
 * Encoder of AMQP Begin type values to a byte stream.
 */
public final class BeginTypeEncoder extends AbstractDescribedListTypeEncoder<Begin> {

    public static final BeginTypeEncoder INSTANCE = new BeginTypeEncoder();

    @Override
    public UnsignedLong getDescriptorCode() {
        return Begin.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Begin.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Begin> getTypeClass() {
        return Begin.class;
    }

    @Override
    public byte getListEncoding(Begin value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Begin begin) {
        return begin.getElementCount();
    }

    @Override
    public int getMinElementCount() {
        return 4;
    }

    @Override
    public int getMaxElementCount() {
        return 8;
    }

    @Override
    public void writeElements(Begin begin, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        if (begin.hasRemoteChannel()) {
            buffer.writeByte(EncodingCodes.USHORT);
            buffer.writeShort((short) begin.getRemoteChannel());
        } else {
            buffer.writeByte(EncodingCodes.NULL);
        }

        if (begin.hasNextOutgoingId()) {
            ProtonEncodings.writeUnsignedInteger(buffer, begin.getNextOutgoingId());
        } else {
            throw new EncodeException("Cannot write an Begin that does not have a next outgoing id assigned.");
        }

        if (begin.hasIncomingWindow()) {
            ProtonEncodings.writeUnsignedInteger(buffer, begin.getIncomingWindow());
        } else {
            throw new EncodeException("Cannot write an Begin that does not have a incoming window assigned.");
        }

        if (begin.hasOutgoingWindow()) {
            ProtonEncodings.writeUnsignedInteger(buffer, begin.getOutgoingWindow());
        } else {
            throw new EncodeException("Cannot write an Begin that does not have a outgoing window assigned.");
        }

        for (int i = 4; i < count; ++i) {
            if (!begin.hasElement(i)) {
                buffer.writeByte(EncodingCodes.NULL);
                continue;
            }

            switch (i) {
                case 4:
                    ProtonEncodings.writeUnsignedInteger(buffer, begin.getHandleMax());
                    break;
                case 5:
                    encoder.writeArray(buffer, state, begin.getOfferedCapabilities());
                    break;
                case 6:
                    encoder.writeArray(buffer, state, begin.getDesiredCapabilities());
                    break;
                case 7:
                    encoder.writeMap(buffer, state, begin.getProperties());
                    break;
            }
        }
    }
}
