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
package org.apache.qpid.protonj2.codec.encoders;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeEncoder;
import org.apache.qpid.protonj2.types.DescribedType;

/**
 * Encoder of AMQP Described Types to a byte stream.
 */
public final class UnknownDescribedTypeEncoder implements TypeEncoder<DescribedType> {

    @Override
    public Class<DescribedType> getTypeClass() {
        return DescribedType.class;
    }

    @Override
    public boolean isArrayType() {
        return false;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, DescribedType value) {
        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        state.getEncoder().writeObject(buffer, state, value.getDescriptor());
        state.getEncoder().writeObject(buffer, state, value.getDescribed());
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Object[] value) {
        throw new UnsupportedOperationException("Cannot write array of unknown described types.");
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        throw new UnsupportedOperationException("Cannot write array of unknown described types.");
    }
}
