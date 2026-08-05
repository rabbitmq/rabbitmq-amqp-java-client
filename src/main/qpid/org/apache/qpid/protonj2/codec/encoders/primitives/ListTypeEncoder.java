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

import java.util.List;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncodeException;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.AbstractPrimitiveTypeEncoder;

/**
 * Encoder of AMQP List type values to a byte stream.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public final class ListTypeEncoder extends AbstractPrimitiveTypeEncoder<List> {

    public static final ListTypeEncoder INSTANCE = new ListTypeEncoder();

    @Override
    public Class<List> getTypeClass() {
        return List.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, List value) {
        if (value.isEmpty()) {
            buffer.writeByte(EncodingCodes.LIST0);
        } else {
            buffer.writeByte(EncodingCodes.LIST32);
            writeValue(buffer, state, value);
        }
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.LIST32);
        for (Object value : values) {
            writeValue(buffer, state, (List) value);
        }
    }

    private void writeValue(ProtonBuffer buffer, EncoderState state, List value) {
        final int startIndex = buffer.getWriteOffset();
        final int size = value.size();

        // Write the count of list elements.
        buffer.writeLong(size);

        TypeEncoder typeEncoder = this;

        // Write the list elements and then compute total size written, try not to lookup
        // encoders when the types in the list all match.
        for (int i = 0; i < size; ++i) {
            final Object entry = value.get(i);

            if (!typeEncoder.getTypeClass().equals(entry.getClass())) {
                typeEncoder = state.getEncoder().getTypeEncoder(entry);

                if (typeEncoder == null) {
                    throw new EncodeException("Cannot find encoder for type " + entry);
                }
            }

            typeEncoder.writeType(buffer, state, entry);
        }

        // Move back and write the size
        buffer.setInt(startIndex, buffer.getWriteOffset() - startIndex - Integer.BYTES);
    }
}
