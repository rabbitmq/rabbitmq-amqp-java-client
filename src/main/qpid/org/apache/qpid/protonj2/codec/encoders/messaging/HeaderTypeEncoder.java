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
import org.apache.qpid.protonj2.codec.encoders.ProtonEncodings;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Header;

/**
 * Encoder of AMQP Header type values to a byte stream
 */
public final class HeaderTypeEncoder extends AbstractDescribedListTypeEncoder<Header> {

    public static final HeaderTypeEncoder INSTANCE = new HeaderTypeEncoder();

    private static final int MAX_HEADER_ENTRIES = 5;

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
    public int getElementCount(Header header) {
        return header.getElementCount();
    }

    @Override
    public int getMaxElementCount() {
        return MAX_HEADER_ENTRIES;
    }

    @Override
    public void writeElements(Header header, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        if (header.hasDurable()) {
            buffer.writeByte(header.isDurable() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
        } else {
            buffer.writeByte(EncodingCodes.NULL);
        }

        if (count >= 2) {
            if (header.hasPriority()) {
                buffer.writeByte(EncodingCodes.UBYTE);
                buffer.writeByte(header.getPriority());
            } else {
                buffer.writeByte(EncodingCodes.NULL);
            }
        } else {
            return;
        }

        if (count >= 3) {
            if (header.hasTimeToLive()) {
                ProtonEncodings.writeUnsignedInteger(buffer, header.getTimeToLive());
            } else {
                buffer.writeByte(EncodingCodes.NULL);
            }
        } else {
            return;
        }

        if (count >= 4) {
            if (header.hasFirstAcquirer()) {
                buffer.writeByte(header.isFirstAcquirer() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
            } else {
                buffer.writeByte(EncodingCodes.NULL);
            }
        } else {
            return;
        }

        if (count == 5) {
            if (header.hasDeliveryCount()) {
                ProtonEncodings.writeUnsignedInteger(buffer, header.getDeliveryCount());
            } else {
                buffer.writeByte(EncodingCodes.NULL);
            }
        }
    }
}
