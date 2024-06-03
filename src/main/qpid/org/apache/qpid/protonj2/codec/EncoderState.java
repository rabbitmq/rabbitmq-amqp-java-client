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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;

/**
 * Retains Encoder state information either between calls or across encode iterations.
 */
public interface EncoderState {

    /**
     * @return the Encoder instance that create this state object.
     */
    Encoder getEncoder();

    /**
     * Resets any intermediate state back to default values.
     *
     * @return this {@link EncoderState} instance.
     */
    EncoderState reset();

    /**
     * Encodes the given sequence of characters in UTF8 to the given buffer.
     *
     * @param buffer
     *      A ProtonBuffer where the UTF-8 encoded bytes should be written.
     * @param sequence
     *      A {@link CharSequence} representing the UTF-8 bytes to encode
     *
     * @return a reference to the encoding buffer for chaining
     *
     * @throws EncodeException if an error occurs while encoding the {@link CharSequence}
     */
    ProtonBuffer encodeUTF8(ProtonBuffer buffer, CharSequence sequence) throws EncodeException;

}
