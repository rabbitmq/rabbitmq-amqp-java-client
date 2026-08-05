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
import org.apache.qpid.protonj2.types.messaging.Properties;

/**
 * Encoder of AMQP Properties type value to a byte stream.
 */
public final class PropertiesTypeEncoder extends AbstractDescribedListTypeEncoder<Properties> {

    public static final PropertiesTypeEncoder INSTANCE = new PropertiesTypeEncoder();

    private static final int MAX_LIST_ELEMENTS = 13;

    @Override
    public UnsignedLong getDescriptorCode() {
        return Properties.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Properties.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Properties> getTypeClass() {
        return Properties.class;
    }

    @Override
    public byte getListEncoding(Properties value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Properties properties) {
        return properties.getElementCount();
    }

    @Override
    public int getMaxElementCount() {
        return MAX_LIST_ELEMENTS;
    }

    @Override
    public void writeElements(Properties properties, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        for (int index = 0; index < count; ++index) {
            if (!properties.hasElement(index)) {
                buffer.writeByte(EncodingCodes.NULL);
                continue;
            }

            switch (index) {
                case 0:
                    encoder.writeObject(buffer, state, properties.getMessageId());
                    break;
                case 1:
                    ProtonEncodings.writeBinary(buffer, properties.getUserId().asProtonBuffer());
                    break;
                case 2:
                    ProtonEncodings.writeString(buffer, state, properties.getTo());
                    break;
                case 3:
                    ProtonEncodings.writeString(buffer, state, properties.getSubject());
                    break;
                case 4:
                    ProtonEncodings.writeString(buffer, state, properties.getReplyTo());
                    break;
                case 5:
                    encoder.writeObject(buffer, state, properties.getCorrelationId());
                    break;
                case 6:
                    ProtonEncodings.writeSymbol(buffer, properties.getContentType());
                    break;
                case 7:
                    ProtonEncodings.writeSymbol(buffer, properties.getContentEncoding());
                    break;
                case 8:
                    buffer.writeByte(EncodingCodes.TIMESTAMP);
                    buffer.writeLong(properties.getAbsoluteExpiryTime());
                    break;
                case 9:
                    buffer.writeByte(EncodingCodes.TIMESTAMP);
                    buffer.writeLong(properties.getCreationTime());
                    break;
                case 10:
                    ProtonEncodings.writeString(buffer, state, properties.getGroupId());
                    break;
                case 11:
                    ProtonEncodings.writeUnsignedInteger(buffer, properties.getGroupSequence());
                    break;
                case 12:
                    ProtonEncodings.writeString(buffer, state, properties.getReplyToGroupId());
                    break;
            }
        }
    }
}
