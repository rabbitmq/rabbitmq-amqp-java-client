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
import org.apache.qpid.protonj2.types.transport.Flow;

/**
 * Encoder of AMQP Flow type values to a byte stream.
 */
public final class FlowTypeEncoder extends AbstractDescribedListTypeEncoder<Flow> {

    public static final FlowTypeEncoder INSTANCE = new FlowTypeEncoder();

    @Override
    public UnsignedLong getDescriptorCode() {
        return Flow.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Flow.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Flow> getTypeClass() {
        return Flow.class;
    }

    @Override
    public int getElementCount(Flow flow) {
        return flow.getElementCount();
    }

    @Override
    public int getMinElementCount() {
        return 4;
    }

    @Override
    public int getMaxElementCount() {
        return 11;
    }

    @Override
    public byte getListEncoding(Flow value) {
        if (value.getProperties() == null) {
            return EncodingCodes.LIST8;
        } else {
            return EncodingCodes.LIST32;
        }
    }

    @Override
    public void writeElements(Flow flow, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        if (flow.hasNextIncomingId()) {
            ProtonEncodings.writeUnsignedInteger(buffer, flow.getNextIncomingId());
        } else {
            buffer.writeByte(EncodingCodes.NULL);
        }

        if (flow.hasIncomingWindow()) {
            ProtonEncodings.writeUnsignedInteger(buffer, flow.getIncomingWindow());
        } else {
            throw new EncodeException("Cannot write an Flow that does not have a incoming window assigned.");
        }

        if (flow.hasNextOutgoingId()) {
            ProtonEncodings.writeUnsignedInteger(buffer, flow.getNextOutgoingId());
        } else {
            throw new EncodeException("Cannot write an Flow that does not have a next outgoing id assigned.");
        }

        if (flow.hasOutgoingWindow()) {
            ProtonEncodings.writeUnsignedInteger(buffer, flow.getOutgoingWindow());
        } else {
            throw new EncodeException("Cannot write an Flow that does not have a outgoing window assigned.");
        }

        for (int i = 4; i < count; ++i) {
            if (!flow.hasElement(i)) {
                buffer.writeByte(EncodingCodes.NULL);
                continue;
            }

            switch (i) {
                case 4:
                    ProtonEncodings.writeUnsignedInteger(buffer, flow.getHandle());
                    break;
                case 5:
                    ProtonEncodings.writeUnsignedInteger(buffer, flow.getDeliveryCount());
                    break;
                case 6:
                    ProtonEncodings.writeUnsignedInteger(buffer, flow.getLinkCredit());
                    break;
                case 7:
                    ProtonEncodings.writeUnsignedInteger(buffer, flow.getAvailable());
                    break;
                case 8:
                    buffer.writeByte(flow.getDrain() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 9:
                    buffer.writeByte(flow.getEcho() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 10:
                    encoder.writeMap(buffer, state, flow.getProperties());
                    break;
            }
        }
    }
}
