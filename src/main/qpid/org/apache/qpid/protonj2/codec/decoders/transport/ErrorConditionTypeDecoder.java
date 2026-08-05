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
package org.apache.qpid.protonj2.codec.decoders.transport;

import java.io.InputStream;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Decoder of AMQP ErrorCondition type values from a byte stream.
 */
public final class ErrorConditionTypeDecoder extends AbstractDescribedListTypeDecoder<ErrorCondition> {

    public static final ErrorConditionTypeDecoder INSTANCE = new ErrorConditionTypeDecoder();

    private static final int MIN_ERROR_CONDITION_LIST_ENTRIES = 1;
    private static final int MAX_ERROR_CONDITION_LIST_ENTRIES = 3;

    @Override
    public Class<ErrorCondition> getTypeClass() {
        return ErrorCondition.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return ErrorCondition.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return ErrorCondition.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_ERROR_CONDITION_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_ERROR_CONDITION_LIST_ENTRIES;
    }

    @Override
    protected ErrorCondition readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Symbol condition = decoder.readSymbol(buffer, state);

        if (condition == null) {
            throw new DecodeException("ErrorCondition requries an assigned condition value be sent but was null");
        }

        String description = null;

        if (count >= 2) {
            description = decoder.readString(buffer, state);
        }

        Map<Symbol, Object> info = null;

        if (count == 3) {
            info = decoder.readMap(buffer, state);
        }

        return new ErrorCondition(condition, description, info);
    }

    @Override
    protected ErrorCondition readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Symbol condition = decoder.readSymbol(stream, state);

        if (condition == null) {
            throw new DecodeException("ErrorCondition requries an assigned condition value be sent but was null");
        }

        String description = null;

        if (count >= 2) {
            description = decoder.readString(stream, state);
        }

        Map<Symbol, Object> info = null;

        if (count == 3) {
            info = decoder.readMap(stream, state);
        }

        return new ErrorCondition(condition, description, info);
    }
}
