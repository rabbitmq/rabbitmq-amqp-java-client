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
import org.apache.qpid.protonj2.codec.EncodeException;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transactions.Declared;

/**
 * Encoder of AMQP Declared type values to a byte stream.
 */
public final class DeclaredTypeEncoder extends AbstractDescribedListTypeEncoder<Declared> {

    public static final DeclaredTypeEncoder INSTANCE = new DeclaredTypeEncoder();

    @Override
    public UnsignedLong getDescriptorCode() {
        return Declared.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Declared.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Declared> getTypeClass() {
        return Declared.class;
    }

    @Override
    public int getElementCount(Declared declared) {
        return 1;
    }

    @Override
    public int getMinElementCount() {
        return 1;
    }

    @Override
    public int getMaxElementCount() {
        return 1;
    }

    @Override
    public byte getListEncoding(Declared value) {
        if (value.getTxnId() != null && value.getTxnId().getLength() > 240) {
            return EncodingCodes.LIST32;
        } else {
            return EncodingCodes.LIST8;
        }
    }

    @Override
    public void writeElements(Declared declared, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        final Binary txnId = declared.getTxnId();

        if (txnId != null && txnId.getLength() > 0) {
            encoder.writeBinary(buffer, state, txnId);
        } else {
            throw new EncodeException("Cannot write a Declared instance without a transaction Id assigned");
        }
    }
}
