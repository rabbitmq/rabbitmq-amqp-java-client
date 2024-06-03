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
package org.apache.qpid.protonj2.codec.encoders.messaging;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Header;

/**
 * Encoder of AMQP Header type values to a byte stream
 */
public final class HeaderTypeEncoder extends AbstractDescribedListTypeEncoder<Header> {

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
    public byte getListEncoding(Header value) {
        return EncodingCodes.LIST8;
    }

    @Override
    public void writeElement(Header header, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        // When encoding ensure that values that were never set are omitted and a simple
        // NULL entry is written in the slot instead (don't write defaults).

        switch (index) {
            case 0:
                if (header.hasDurable()) {
                    buffer.writeByte(header.isDurable() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 1:
                if (header.hasPriority()) {
                    encoder.writeUnsignedByte(buffer, state, header.getPriority());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 2:
                if (header.hasTimeToLive()) {
                    encoder.writeUnsignedInteger(buffer, state, header.getTimeToLive());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 3:
                if (header.hasFirstAcquirer()) {
                    buffer.writeByte(header.isFirstAcquirer() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 4:
                if (header.hasDeliveryCount()) {
                    encoder.writeUnsignedInteger(buffer, state, header.getDeliveryCount());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown Header value index: " + index);
        }
    }

    @Override
    public int getElementCount(Header header) {
        return header.getElementCount();
    }
}
