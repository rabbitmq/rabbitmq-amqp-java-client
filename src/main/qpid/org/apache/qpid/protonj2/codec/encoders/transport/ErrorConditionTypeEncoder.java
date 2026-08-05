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
package org.apache.qpid.protonj2.codec.encoders.transport;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncodings;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Encoder of AMQP ErrorCondition type values to a byte stream
 */
public final class ErrorConditionTypeEncoder extends AbstractDescribedListTypeEncoder<ErrorCondition> {

    public static final ErrorConditionTypeEncoder INSTANCE = new ErrorConditionTypeEncoder();

    private static final int MAX_LIST_ELEMENTS = 3;
    private static final int MIN_LIST_ELEMENTS = 1;

    @Override
    public UnsignedLong getDescriptorCode() {
        return ErrorCondition.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return ErrorCondition.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<ErrorCondition> getTypeClass() {
        return ErrorCondition.class;
    }

    @Override
    public byte getListEncoding(ErrorCondition value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getMaxElementCount() {
        return MAX_LIST_ELEMENTS;
    }

    @Override
    public int getMinElementCount() {
        return MIN_LIST_ELEMENTS;
    }

    @Override
    public int getElementCount(ErrorCondition error) {
        return error.getElementCount();
    }

    @Override
    public void writeElements(ErrorCondition error, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        ProtonEncodings.writeSymbol(buffer, error.getCondition());

        if (count >= 2) {
            ProtonEncodings.writeString(buffer, state, error.getDescription());
        }

        if (count == 3) {
            encoder.writeMap(buffer, state, error.getInfo());
        }
    }
}
