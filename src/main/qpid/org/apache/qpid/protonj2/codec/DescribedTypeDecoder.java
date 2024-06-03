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
package org.apache.qpid.protonj2.codec;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

/**
 * Interface for all DescribedType decoder implementations
 *
 * @param <V> The type this decoder handles
 */
public interface DescribedTypeDecoder<V> extends TypeDecoder<V> {

    /**
     * Returns the AMQP descriptor code for the type this decoder reads.
     *
     * @return an unsigned long descriptor code value.
     */
    UnsignedLong getDescriptorCode();

    /**
     * Returns the AMQP descriptor symbol for the type this decoder reads.
     *
     * @return an symbol descriptor code value.
     */
    Symbol getDescriptorSymbol();

    /**
     * Allow the type decoder to react to being registered with a given {@link Decoder} instance.
     *
     * @param decoder
     * 		The {@link Decoder} that this type decoder is now registered with.
     *
     * @return this type decoder value after having initialized itself following registration.
     */
    default DescribedTypeDecoder<V> decoderRegistered(Decoder decoder) {
        return this;
    }
}
