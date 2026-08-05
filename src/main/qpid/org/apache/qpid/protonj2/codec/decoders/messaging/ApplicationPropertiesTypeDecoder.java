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
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;

/**
 * Decoder of AMQP ApplicationProperties types from a byte stream
 */
public final class ApplicationPropertiesTypeDecoder extends AbstractDescribedMapTypeDecoder<ApplicationProperties, String> {

    public static final ApplicationPropertiesTypeDecoder INSTANCE = new ApplicationPropertiesTypeDecoder();

    @Override
    public Class<ApplicationProperties> getTypeClass() {
        return ApplicationProperties.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return ApplicationProperties.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return ApplicationProperties.DESCRIPTOR_SYMBOL;
    }

    @Override
    protected String readKey(ProtonBuffer buffer, Decoder decoder, DecoderState state) throws DecodeException {
        return decoder.readString(buffer, state);
    }

    @Override
    protected String readKey(InputStream stream, StreamDecoder decoder, StreamDecoderState state) throws DecodeException {
        return decoder.readString(stream, state);
    }

    @Override
    protected ApplicationProperties createDescribed(Map<String, Object> map) {
        return new ApplicationProperties(map);
    }

    /**
     * Creates a new scanning context for the given collection of {@link String} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of string keys.
     */
    public static ScanningContext<String> createScanContext(String...keys) {
        return ProtonScanningContext.createStringScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link String} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of string keys.
     */
    public static ScanningContext<String> createScanContext(Collection<String> keys) {
        return ProtonScanningContext.createStringScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link String} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of string keys.
     */
    public static StreamScanningContext<String> createStreamScanContext(String...keys) {
        return ProtonScanningContext.createStringScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link String} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of string keys.
     */
    public static StreamScanningContext<String> createStreamScanContext(Collection<String> keys) {
        return ProtonScanningContext.createStringScanContext(keys);
    }

    /**
     * Scans through the encoded {@link ApplicationProperties} map looking for keys that match with
     * the provided {@link ScanningContext}.  When a match is found the provided match consumer
     * is called with the matched key and the decoded value mapped to that key. When the method
     * returns the caller can assume that all bytes of the encoded ApplicationProperties have been
     * read and that decoding of the next object can begin if the provided buffer remains readable.
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
    public void scanProperties(ProtonBuffer buffer, DecoderState state, ScanningContext<String> context, BiConsumer<String, Object> matchConsumer) throws DecodeException {
        scanMapEntries(buffer, state, context, matchConsumer);
    }

    /**
     * Scans through the encoded {@link ApplicationProperties} map looking for keys that match with
     * the provided {@link ScanningContext}.  When a match is found the provided match consumer
     * is called with the matched key and the decoded value mapped to that key. When the method
     * returns the caller can assume that all bytes of the encoded ApplicationProperties have been
     * read and that decoding of the next object can begin if the provided stream remains readable.
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
    public void scanProperties(InputStream stream, StreamDecoderState state, StreamScanningContext<String> context, BiConsumer<String, Object> matchConsumer) throws DecodeException {
        scanMapEntries(stream, state, context, matchConsumer);
    }
}
