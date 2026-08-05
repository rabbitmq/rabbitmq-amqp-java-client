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
import org.apache.qpid.protonj2.types.transport.Open;

/**
 * Encoder of AMQP Open type values to a byte stream.
 */
public final class OpenTypeEncoder extends AbstractDescribedListTypeEncoder<Open> {

    public static final OpenTypeEncoder INSTANCE = new OpenTypeEncoder();

    @Override
    public UnsignedLong getDescriptorCode() {
        return Open.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Open.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Open> getTypeClass() {
        return Open.class;
    }

    @Override
    public byte getListEncoding(Open value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Open open) {
        return open.getElementCount();
    }

    @Override
    public int getMinElementCount() {
        return 1;
    }

    @Override
    public int getMaxElementCount() {
        return 10;
    }

    @Override
    public void writeElements(Open open, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        if (open.hasContainerId()) {
            ProtonEncodings.writeString(buffer, state, open.getContainerId());
        } else {
            throw new EncodeException("Cannot encode an Open that does not have an assigned container-id");
        }

        for (int i = 1; i < count; ++i) {
            if (!open.hasElement(i)) {
                buffer.writeByte(EncodingCodes.NULL);
                continue;
            }

            switch (i) {
                case 1:
                    ProtonEncodings.writeString(buffer, state, open.getHostname());
                    break;
                case 2:
                    ProtonEncodings.writeUnsignedInteger(buffer, open.getMaxFrameSize());
                    break;
                case 3:
                    buffer.writeByte(EncodingCodes.USHORT);
                    buffer.writeShort((short) open.getChannelMax());
                    break;
                case 4:
                    ProtonEncodings.writeUnsignedInteger(buffer, open.getIdleTimeout());
                    break;
                case 5:
                    encoder.writeArray(buffer, state, open.getOutgoingLocales());
                    break;
                case 6:
                    encoder.writeArray(buffer, state, open.getIncomingLocales());
                    break;
                case 7:
                    encoder.writeArray(buffer, state, open.getOfferedCapabilities());
                    break;
                case 8:
                    encoder.writeArray(buffer, state, open.getDesiredCapabilities());
                    break;
                case 9:
                    encoder.writeMap(buffer, state, open.getProperties());
                    break;
            }
        }
    }
}
