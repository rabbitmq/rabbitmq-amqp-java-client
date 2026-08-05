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
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.apache.qpid.protonj2.types.messaging.TerminusExpiryPolicy;

/**
 * Decoder of AMQP Target type values from a byte stream
 */
public final class TargetTypeDecoder extends AbstractDescribedListTypeDecoder<Target> {

    public static final TargetTypeDecoder INSTANCE = new TargetTypeDecoder();

    private static final int MIN_TARGET_LIST_ENTRIES = 0;
    private static final int MAX_TARGET_LIST_ENTRIES = 7;

    @Override
    public Class<Target> getTypeClass() {
        return Target.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Target.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Target.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_TARGET_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_TARGET_LIST_ENTRIES;
    }

    @Override
    protected Target readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Target target = new Target();

        for (int index = 0; index < count; ++index) {
            if (buffer.peekByte() == EncodingCodes.NULL) {
                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    target.setAddress(decoder.readString(buffer, state));
                    break;
                case 1:
                    final long durability = decoder.readUnsignedInteger(buffer, state, 0);
                    target.setDurable(TerminusDurability.valueOf(durability));
                    break;
                case 2:
                    final Symbol expiryPolicy = decoder.readSymbol(buffer, state);
                    target.setExpiryPolicy(expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : TerminusExpiryPolicy.valueOf(expiryPolicy));
                    break;
                case 3:
                    final UnsignedInteger timeout = decoder.readUnsignedInteger(buffer, state);
                    target.setTimeout(timeout == null ? UnsignedInteger.ZERO : timeout);
                    break;
                case 4:
                    target.setDynamic(decoder.readBoolean(buffer, state, false));
                    break;
                case 5:
                    target.setDynamicNodeProperties(decoder.readMap(buffer, state));
                    break;
                case 6:
                    target.setCapabilities(decoder.readMultiple(buffer, state, Symbol.class));
                    break;
            }
        }

        return target;
    }

    @Override
    protected Target readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Target target = new Target();

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
                    target.setAddress(decoder.readString(stream, state));
                    break;
                case 1:
                    final long durability = decoder.readUnsignedInteger(stream, state, 0);
                    target.setDurable(TerminusDurability.valueOf(durability));
                    break;
                case 2:
                    final Symbol expiryPolicy = decoder.readSymbol(stream, state);
                    target.setExpiryPolicy(expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : TerminusExpiryPolicy.valueOf(expiryPolicy));
                    break;
                case 3:
                    final UnsignedInteger timeout = decoder.readUnsignedInteger(stream, state);
                    target.setTimeout(timeout == null ? UnsignedInteger.ZERO : timeout);
                    break;
                case 4:
                    target.setDynamic(decoder.readBoolean(stream, state, false));
                    break;
                case 5:
                    target.setDynamicNodeProperties(decoder.readMap(stream, state));
                    break;
                case 6:
                    target.setCapabilities(decoder.readMultiple(stream, state, Symbol.class));
                    break;
            }
        }

        return target;
    }
}
