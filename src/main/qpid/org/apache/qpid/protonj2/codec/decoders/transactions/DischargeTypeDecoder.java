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
package org.apache.qpid.protonj2.codec.decoders.transactions;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.BinaryTypeDecoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transactions.Discharge;

/**
 * Decoder of AMQP Discharge type values from a byte stream.
 */
public final class DischargeTypeDecoder extends AbstractDescribedListTypeDecoder<Discharge> {

    public static final DischargeTypeDecoder INSTANCE = new DischargeTypeDecoder();

    private static final int MIN_DISCHARGE_LIST_ENTRIES = 1;
    private static final int MAX_DISCHARGE_LIST_ENTRIES = 2;

    @Override
    public Class<Discharge> getTypeClass() {
        return Discharge.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Discharge.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Discharge.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return MIN_DISCHARGE_LIST_ENTRIES;
    }

    @Override
    protected int getMaxListElements() {
        return MAX_DISCHARGE_LIST_ENTRIES;
    }

    @Override
    protected Discharge readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        final Discharge discharge = new Discharge();
        final TypeDecoder<?> typeDecoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (typeDecoder instanceof BinaryTypeDecoder) {
            discharge.setTxnId(new Binary(((BinaryTypeDecoder) typeDecoder).readValueAsArray(buffer, state)));
        } else if (typeDecoder.isNull()) {
            throw new DecodeException("The txn-id field cannot be omitted from the Discharge");
        } else {
            throw new DecodeException(
                "Expected a Binary encoding but got decoder for type: " + typeDecoder.getTypeClass().getName());
        }

        if (count == 2) {
            discharge.setFail(state.getDecoder().readBoolean(buffer, state, false));
        }

        return discharge;
    }

    @Override
    protected Discharge readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        final Discharge discharge = new Discharge();
        final StreamTypeDecoder<?> typeDecoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (typeDecoder instanceof BinaryTypeDecoder) {
            discharge.setTxnId(new Binary(((BinaryTypeDecoder) typeDecoder).readValueAsArray(stream, state)));
        } else if (typeDecoder.isNull()) {
            throw new DecodeException("The txn-id field cannot be omitted from the Discharge");
        } else {
            throw new DecodeException(
                "Expected a Binary encoding but got decoder for type: " + typeDecoder.getTypeClass().getName());
        }

        if (count == 2) {
            discharge.setFail(state.getDecoder().readBoolean(stream, state, false));
        }

        return discharge;
    }
}
