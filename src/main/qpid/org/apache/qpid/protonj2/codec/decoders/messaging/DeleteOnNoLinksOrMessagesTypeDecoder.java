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
package org.apache.qpid.protonj2.codec.decoders.messaging;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.DeleteOnNoLinksOrMessages;

/**
 * Decoder of AMQP DeleteOnNoLinksOrMessages type values from a byte stream
 */
public final class DeleteOnNoLinksOrMessagesTypeDecoder extends AbstractDescribedListTypeDecoder<DeleteOnNoLinksOrMessages> {

    public static final DeleteOnNoLinksOrMessagesTypeDecoder INSTANCE = new DeleteOnNoLinksOrMessagesTypeDecoder();

    @Override
    public Class<DeleteOnNoLinksOrMessages> getTypeClass() {
        return DeleteOnNoLinksOrMessages.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return DeleteOnNoLinksOrMessages.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DeleteOnNoLinksOrMessages.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected int getMinListElements() {
        return 0;
    }

    @Override
    protected int getMaxListElements() {
        return 0;
    }

    @Override
    protected DeleteOnNoLinksOrMessages readSingle(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        listDecoder.skipValue(buffer, state);

        return DeleteOnNoLinksOrMessages.getInstance();
    }

    @Override
    protected DeleteOnNoLinksOrMessages readSingle(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        listDecoder.skipValue(stream, state);

        return DeleteOnNoLinksOrMessages.getInstance();
    }

    @Override
    protected DeleteOnNoLinksOrMessages readType(int count, ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        throw new DecodeException("Invalid API called for empty list type: " + getClass().getName());
    }

    @Override
    protected DeleteOnNoLinksOrMessages readType(int count, InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        throw new DecodeException("Invalid API called for empty list type: " + getClass().getName());
    }
}
