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
package org.apache.qpid.protonj2.codec.encoders.transactions;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transactions.Coordinator;

/**
 * Encoder of AMQP Coordinator type values to a byte stream.
 */
public final class CoordinatorTypeEncoder extends AbstractDescribedListTypeEncoder<Coordinator> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Coordinator.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Coordinator.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Coordinator> getTypeClass() {
        return Coordinator.class;
    }

    @Override
    public void writeElement(Coordinator coordinator, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        switch (index) {
            case 0:
                encoder.writeArray(buffer, state, coordinator.getCapabilities());
                break;
            default:
                throw new IllegalArgumentException("Unknown Coordinator value index: " + index);
        }
    }

    @Override
    public byte getListEncoding(Coordinator value) {
        if (value.getCapabilities() != null) {
            return EncodingCodes.LIST32;
        } else {
            return EncodingCodes.LIST0;
        }
    }

    @Override
    public int getElementCount(Coordinator coordinator) {
        if (coordinator.getCapabilities() != null) {
            return 1;
        } else {
            return 0;
        }
    }
}
