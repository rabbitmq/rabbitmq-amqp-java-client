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
import org.apache.qpid.protonj2.types.transactions.TransactionalState;

/**
 * Encoder of AMQP TransactionState type values to a byte stream.
 */
public final class TransactionStateTypeEncoder extends AbstractDescribedListTypeEncoder<TransactionalState> {

    public static final TransactionStateTypeEncoder INSTANCE = new TransactionStateTypeEncoder();

    @Override
    public UnsignedLong getDescriptorCode() {
        return TransactionalState.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return TransactionalState.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<TransactionalState> getTypeClass() {
        return TransactionalState.class;
    }

    @Override
    public byte getListEncoding(TransactionalState value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getMinElementCount() {
        return 1;
    }

    @Override
    public int getMaxElementCount() {
        return 2;
    }

    @Override
    public int getElementCount(TransactionalState txState) {
        return txState.getOutcome() == null ? 1 : 2;
    }

    @Override
    public void writeElements(TransactionalState txState, int count, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        final Binary txnId = txState.getTxnId();

        if (txnId != null && txnId.getLength() > 0) {
            encoder.writeBinary(buffer, state, txnId);
        } else {
            throw new EncodeException("Cannot write a TransactionalState instance without a transaction Id assigned");
        }

        if (count == 2) {
            encoder.writeObject(buffer, state, txState.getOutcome());
        }
    }
}
