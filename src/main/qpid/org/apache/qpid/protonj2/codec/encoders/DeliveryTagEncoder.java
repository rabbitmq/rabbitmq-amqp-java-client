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
import org.apache.qpid.protonj2.codec.TypeEncoder;
import org.apache.qpid.protonj2.types.DeliveryTag;

/**
 * Custom encoder for writing DeliveryTag types to a {@link ProtonBuffer}.
 */
public final class DeliveryTagEncoder implements TypeEncoder<DeliveryTag> {

    public static final DeliveryTagEncoder INSTANCE = new DeliveryTagEncoder();

    @Override
    public Class<DeliveryTag> getTypeClass() {
        return DeliveryTag.class;
    }

    @Override
    public boolean isArrayType() {
        return false;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, DeliveryTag value) {
        ProtonEncodings.writeDeliveryTag(buffer, value);
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        throw new UnsupportedOperationException("Cannot Write Arrays of Delivery Tags, use Binary types instead.");
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        throw new UnsupportedOperationException("Cannot Write Arrays of Delivery Tags, use Binary types instead.");
    }
}
