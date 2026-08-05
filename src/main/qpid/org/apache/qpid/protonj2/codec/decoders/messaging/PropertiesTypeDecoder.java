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
package org.apache.qpid.protonj2.codec.decoders.messaging;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Properties;

/**
 * Decoder of AMQP Properties type values from a byte stream
 */
public final class PropertiesTypeDecoder extends AbstractDescribedListTypeDecoder<Properties> {

    public static final PropertiesTypeDecoder INSTANCE = new PropertiesTypeDecoder();

    private static final int MIN_PROPERTIES_LIST_ENTRIES = 0;
    private static final int MAX_PROPERTIES_LIST_ENTRIES = 13;

    @Override
    public Class<Properties> getTypeClass() {
        return Properties.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Properties.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Properties.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_PROPERTIES_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_PROPERTIES_LIST_ENTRIES;
    }

    @Override
    protected Properties readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Properties properties = new Properties();

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            if (buffer.peekByte() == EncodingCodes.NULL) {
                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    properties.setMessageId(decoder.readObject(buffer, state));
                    break;
                case 1:
                    properties.setUserId(decoder.readBinary(buffer, state));
                    break;
                case 2:
                    properties.setTo(decoder.readString(buffer, state));
                    break;
                case 3:
                    properties.setSubject(decoder.readString(buffer, state));
                    break;
                case 4:
                    properties.setReplyTo(decoder.readString(buffer, state));
                    break;
                case 5:
                    properties.setCorrelationId(decoder.readObject(buffer, state));
                    break;
                case 6:
                    properties.setContentType(decoder.readSymbol(buffer, state, null));
                    break;
                case 7:
                    properties.setContentEncoding(decoder.readSymbol(buffer, state, null));
                    break;
                case 8:
                    properties.setAbsoluteExpiryTime(decoder.readTimestamp(buffer, state, 0l));
                    break;
                case 9:
                    properties.setCreationTime(decoder.readTimestamp(buffer, state, 0l));
                    break;
                case 10:
                    properties.setGroupId(decoder.readString(buffer, state));
                    break;
                case 11:
                    properties.setGroupSequence(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 12:
                    properties.setReplyToGroupId(decoder.readString(buffer, state));
                    break;
            }
        }

        return properties;
    }

    @Override
    protected Properties readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Properties properties = new Properties();

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                if (ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL) {
                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    properties.setMessageId(decoder.readObject(stream, state));
                    break;
                case 1:
                    properties.setUserId(decoder.readBinary(stream, state));
                    break;
                case 2:
                    properties.setTo(decoder.readString(stream, state));
                    break;
                case 3:
                    properties.setSubject(decoder.readString(stream, state));
                    break;
                case 4:
                    properties.setReplyTo(decoder.readString(stream, state));
                    break;
                case 5:
                    properties.setCorrelationId(decoder.readObject(stream, state));
                    break;
                case 6:
                    properties.setContentType(decoder.readSymbol(stream, state, null));
                    break;
                case 7:
                    properties.setContentEncoding(decoder.readSymbol(stream, state, null));
                    break;
                case 8:
                    properties.setAbsoluteExpiryTime(decoder.readTimestamp(stream, state, 0l));
                    break;
                case 9:
                    properties.setCreationTime(decoder.readTimestamp(stream, state, 0l));
                    break;
                case 10:
                    properties.setGroupId(decoder.readString(stream, state));
                    break;
                case 11:
                    properties.setGroupSequence(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 12:
                    properties.setReplyToGroupId(decoder.readString(stream, state));
                    break;
            }
        }

        return properties;
    }
}
