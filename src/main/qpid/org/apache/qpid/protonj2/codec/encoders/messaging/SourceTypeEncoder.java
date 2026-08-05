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
import org.apache.qpid.protonj2.types.messaging.Source;

/**
 * Encoder of AMQP Source type values to a byte stream.
 */
public final class SourceTypeEncoder extends AbstractDescribedListTypeEncoder<Source> {

    public static final SourceTypeEncoder INSTANCE = new SourceTypeEncoder();

    private static final int MAX_LIST_ELEMENTS = 11;

    @Override
    public UnsignedLong getDescriptorCode() {
        return Source.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Source.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Source> getTypeClass() {
        return Source.class;
    }

    @Override
    public int getElementCount(Source source) {
        return source.getElementCount();
    }

    @Override
    public byte getListEncoding(Source value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getMaxElementCount() {
        return MAX_LIST_ELEMENTS;
    }

    @Override
    public void writeElements(Source source, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        for (int index = 0; index < count; ++index) {
            if (!source.hasElement(index)) {
                buffer.writeByte(EncodingCodes.NULL);
                continue;
            }

            switch (index) {
                case 0:
                    ProtonEncodings.writeString(buffer, state, source.getAddress());
                    break;
                case 1:
                    ProtonEncodings.writeUnsignedInteger(buffer, source.getDurable().getValue().intValue());
                    break;
                case 2:
                    ProtonEncodings.writeSymbol(buffer, source.getExpiryPolicy().getPolicy());
                    break;
                case 3:
                    ProtonEncodings.writeUnsignedInteger(buffer, source.getTimeout().intValue());
                    break;
                case 4:
                    buffer.writeByte(source.isDynamic() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 5:
                    encoder.writeMap(buffer, state, source.getDynamicNodeProperties());
                    break;
                case 6:
                    ProtonEncodings.writeSymbol(buffer, source.getDistributionMode());
                    break;
                case 7:
                    encoder.writeMap(buffer, state, source.getFilter());
                    break;
                case 8:
                    encoder.writeObject(buffer, state, source.getDefaultOutcome());
                    break;
                case 9:
                    encoder.writeArray(buffer, state, source.getOutcomes());
                    break;
                case 10:
                    encoder.writeArray(buffer, state, source.getCapabilities());
                    break;
            }
        }
    }
}
