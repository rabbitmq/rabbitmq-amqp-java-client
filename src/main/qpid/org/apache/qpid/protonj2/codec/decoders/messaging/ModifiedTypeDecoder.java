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
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Modified;

/**
 * Decoder of AMQP Modified type values from a byte stream.
 */
public final class ModifiedTypeDecoder extends AbstractDescribedListTypeDecoder<Modified> {

    public static final ModifiedTypeDecoder INSTANCE = new ModifiedTypeDecoder();

    private static final int MIN_MODIFIED_LIST_ENTRIES = 0;
    private static final int MAX_MODIFIED_LIST_ENTRIES = 3;

    @Override
    public Class<Modified> getTypeClass() {
        return Modified.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Modified.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Modified.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_MODIFIED_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_MODIFIED_LIST_ENTRIES;
    }

    @Override
    protected Modified readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Modified modified = new Modified();

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    modified.setDeliveryFailed(decoder.readBoolean(buffer, state, false));
                    break;
                case 1:
                    modified.setUndeliverableHere(decoder.readBoolean(buffer, state, false));
                    break;
                case 2:
                    modified.setMessageAnnotations(decoder.readMap(buffer, state));
                    break;
                default:
                    throw new DecodeException("To many entries in Modified encoding");
            }
        }

        return modified;
    }

    @Override
    protected Modified readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Modified modified = new Modified();

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    modified.setDeliveryFailed(decoder.readBoolean(stream, state, false));
                    break;
                case 1:
                    modified.setUndeliverableHere(decoder.readBoolean(stream, state, false));
                    break;
                case 2:
                    modified.setMessageAnnotations(decoder.readMap(stream, state));
                    break;
                default:
                    throw new DecodeException("To many entries in Modified encoding");
            }
        }

        return modified;
    }
}
