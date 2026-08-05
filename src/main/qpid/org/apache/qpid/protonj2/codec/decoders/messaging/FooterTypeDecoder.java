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
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedMapTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonScanningContext;
import org.apache.qpid.protonj2.codec.decoders.ScanningContext;
import org.apache.qpid.protonj2.codec.decoders.StreamScanningContext;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Footer;

/**
 * Decoder of AMQP Footer type values from a byte stream.
 */
public final class FooterTypeDecoder extends AbstractDescribedMapTypeDecoder<Footer, Symbol> {

    public static final FooterTypeDecoder INSTANCE = new FooterTypeDecoder();

    @Override
    public Class<Footer> getTypeClass() {
        return Footer.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Footer.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Footer.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected Symbol readKey(ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        return decoder.readSymbol(buffer, state);
    }

    @Override
    protected Symbol readKey(InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        return decoder.readSymbol(stream, state);
    }

    @Override
    protected Footer createDescribed(Map<Symbol, Object> map) {
        return new Footer(map);
    }

    /**
     * Creates a new scanning context for the given collection of {@link Symbol} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of string keys.
     */
    public static ScanningContext<Symbol> createScanContext(Symbol...keys) {
        return ProtonScanningContext.createSymbolScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link Symbol} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of string keys.
     */
    public static ScanningContext<Symbol> createScanContext(Collection<Symbol> keys) {
        return ProtonScanningContext.createSymbolScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link Symbol} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of Symbol keys.
     */
    public static StreamScanningContext<Symbol> createStreamScanContext(Symbol...keys) {
        return ProtonScanningContext.createSymbolScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link Symbol} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of Symbol keys.
     */
    public static StreamScanningContext<Symbol> createStreamScanContext(Collection<Symbol> keys) {
        return ProtonScanningContext.createSymbolScanContext(keys);
    }

    /**
     * Scans through the encoded {@link Footer} map looking for keys that match with
     * the provided {@link ScanningContext}.  When a match is found the provided match consumer
     * is called with the matched key and the decoded value mapped to that key. When the method
     * returns the caller can assume that all bytes of the encoded Footer have been read and
     * that decoding of the next object can begin if the provided buffer remains readable.
     *
     * @param buffer
     * 		The buffer to scan for specific key / value mappings
     * @param state
     * 		The decoder state used during the scanning
     * @param context
     * 		A matching context primed with the match data needed to match encoded keys.
     * @param matchConsumer
     * 		A {@link BiConsumer} that will be notified of each matching key / value pair.
     *
     * @throws DecodeException if an error is encountered while reading the next value.
     */
    public void scanAnnotations(ProtonBuffer buffer, DecoderState state, ScanningContext<Symbol> context, BiConsumer<Symbol, Object> matchConsumer) throws DecodeException {
        scanMapEntries(buffer, state, context, matchConsumer);
    }

    /**
     * Scans through the encoded {@link Footer} map looking for keys that match with
     * the provided {@link ScanningContext}.  When a match is found the provided match consumer
     * is called with the matched key and the decoded value mapped to that key. When the method
     * returns the caller can assume that all bytes of the encoded Footer have been read and
     * that decoding of the next object can begin if the provided stream remains readable.
     *
     * @param stream
     * 		The {@link InputStream} to scan for specific key / value mappings
     * @param state
     * 		The decoder state used during the scanning
     * @param context
     * 		A matching context primed with the match data needed to match encoded keys.
     * @param matchConsumer
     * 		A {@link BiConsumer} that will be notified of each matching key / value pair.
     *
     * @throws DecodeException if an error is encountered while reading the next value.
     */
    public void scanAnnotations(InputStream stream, StreamDecoderState state, StreamScanningContext<Symbol> context, BiConsumer<Symbol, Object> matchConsumer) throws DecodeException {
        scanMapEntries(stream, state, context, matchConsumer);
    }
}
