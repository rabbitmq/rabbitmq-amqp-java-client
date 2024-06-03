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
import org.apache.qpid.protonj2.types.messaging.Properties;

/**
 * Encoder of AMQP Properties type value to a byte stream.
 */
public final class PropertiesTypeEncoder extends AbstractDescribedListTypeEncoder<Properties> {

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
    public void writeElement(Properties properties, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        switch (index) {
            case 0:
                encoder.writeObject(buffer, state, properties.getMessageId());
                break;
            case 1:
                encoder.writeBinary(buffer, state, properties.getUserId());
                break;
            case 2:
                encoder.writeString(buffer, state, properties.getTo());
                break;
            case 3:
                encoder.writeString(buffer, state, properties.getSubject());
                break;
            case 4:
                encoder.writeString(buffer, state, properties.getReplyTo());
                break;
            case 5:
                encoder.writeObject(buffer, state, properties.getCorrelationId());
                break;
            case 6:
                encoder.writeSymbol(buffer, state, properties.getContentType());
                break;
            case 7:
                encoder.writeSymbol(buffer, state, properties.getContentEncoding());
                break;
            case 8:
                if (properties.hasAbsoluteExpiryTime()) {
                    encoder.writeTimestamp(buffer, state, properties.getAbsoluteExpiryTime());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 9:
                if (properties.hasCreationTime()) {
                    encoder.writeTimestamp(buffer, state, properties.getCreationTime());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 10:
                encoder.writeString(buffer, state, properties.getGroupId());
                break;
            case 11:
                if (properties.hasGroupSequence()) {
                    encoder.writeUnsignedInteger(buffer, state, properties.getGroupSequence());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 12:
                encoder.writeString(buffer, state, properties.getReplyToGroupId());
                break;
            default:
                throw new IllegalArgumentException("Unknown Properties value index: " + index);
        }
    }

    @Override
    public byte getListEncoding(Properties value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Properties properties) {
        return properties.getElementCount();
    }
}
