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
import org.apache.qpid.protonj2.types.messaging.Target;

/**
 * Encoder of AMQP Target type values to a byte stream.
 */
public final class TargetTypeEncoder extends AbstractDescribedListTypeEncoder<Target> {

    public static final TargetTypeEncoder INSTANCE = new TargetTypeEncoder();

    private static final int MAX_LIST_ENCODINGS = 7;

    @Override
    public UnsignedLong getDescriptorCode() {
        return Target.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Target.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Target> getTypeClass() {
        return Target.class;
    }

    @Override
    public int getElementCount(Target target) {
        return target.getElementCount();
    }

    @Override
    public byte getListEncoding(Target value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getMaxElementCount() {
        return MAX_LIST_ENCODINGS;
    }

    @Override
    public void writeElements(Target target, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        for (int index = 0; index < count; ++index) {
            if (!target.hasElement(index)) {
                buffer.writeByte(EncodingCodes.NULL);
                continue;
            }

            switch (index) {
                case 0:
                    ProtonEncodings.writeString(buffer, state, target.getAddress());
                    break;
                case 1:
                    ProtonEncodings.writeUnsignedInteger(buffer, target.getDurable().getValue().intValue());
                    break;
                case 2:
                    ProtonEncodings.writeSymbol(buffer, target.getExpiryPolicy().getPolicy());
                    break;
                case 3:
                    ProtonEncodings.writeUnsignedInteger(buffer, target.getTimeout().intValue());
                    break;
                case 4:
                    buffer.writeByte(target.isDynamic() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 5:
                    encoder.writeMap(buffer, state, target.getDynamicNodeProperties());
                    break;
                case 6:
                    encoder.writeArray(buffer, state, target.getCapabilities());
                    break;
            }
        }
    }
}
