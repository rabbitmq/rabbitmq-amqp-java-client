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
package org.apache.qpid.protonj2.codec.encoders.primitives;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractPrimitiveTypeEncoder;

/**
 * Encoder of AMQP Null type values to a byte stream.
 */
public final class NullTypeEncoder extends AbstractPrimitiveTypeEncoder<Void> {

    @Override
    public Class<Void> getTypeClass() {
        return Void.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Void value) {
        buffer.writeByte(EncodingCodes.NULL);
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Object[] value) {
        throw new IllegalArgumentException("Cannot write an array of nulls");
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        throw new IllegalArgumentException("Cannot write an array of nulls");
    }
}
