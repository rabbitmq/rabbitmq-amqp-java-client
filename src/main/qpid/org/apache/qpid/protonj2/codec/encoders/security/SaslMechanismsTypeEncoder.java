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
package org.apache.qpid.protonj2.codec.encoders.security;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.security.SaslMechanisms;

/**
 * Encoder of AMQP SaslMechanisms type values to a byte stream
 */
public final class SaslMechanismsTypeEncoder extends AbstractDescribedListTypeEncoder<SaslMechanisms> {

    @Override
    public Class<SaslMechanisms> getTypeClass() {
        return SaslMechanisms.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return SaslMechanisms.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return SaslMechanisms.DESCRIPTOR_SYMBOL;
    }

    @Override
    public void writeElement(SaslMechanisms mechanisms, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        switch (index) {
            case 0:
                encoder.writeArray(buffer, state, mechanisms.getSaslServerMechanisms());
                break;
            default:
                throw new IllegalArgumentException("Unknown SaslChallenge value index: " + index);
        }
    }

    @Override
    public int getElementCount(SaslMechanisms challenge) {
        return 1;
    }

    @Override
    public int getMinElementCount() {
        return 1;
    }
}
