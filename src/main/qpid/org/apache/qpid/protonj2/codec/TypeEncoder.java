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
 * Interface for an encoder of a specific AMQP Type.
 *
 * @param <V> the concrete Type that this encoder handles.
 */
public interface TypeEncoder<V> {

    /**
     * @return the Class type that this encoder handles.
     */
    Class<V> getTypeClass();

    /**
     * @return true if the type handled by this encoded is an AMQP Array type.
     */
    boolean isArrayType();

    /**
     * Write the full AMQP type data to the given byte buffer.
     * <p>
     * This can consist of writing both a type constructor value and
     * the bytes that make up the value of the type being written.
     *
     * @param buffer
     * 		The buffer to write the AMQP type to
     * @param state
     * 		The current encoder state
     * @param value
     * 		The value that is to be written.
     *
     * @throws EncodeException if an error occurs while encoding the given value.
     */
    void writeType(ProtonBuffer buffer, EncoderState state, V value) throws EncodeException;

    /**
     * Write an array elements of the AMQP type to the given byte buffer.
     * <p>
     * This method writes the full Array type definition of an array of the
     * type this encoder manages.
     *
     * @param buffer
     *      The buffer to write the AMQP array elements to
     * @param state
     *      The current encoder state
     * @param values
     *      The array of values that is to be written.
     *
     * @throws EncodeException if an error occurs while encoding the given value.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, Object[] values) throws EncodeException;

    /**
     * Write an array elements of the AMQP type to the given byte buffer.
     * <p>
     * This method writes only the body portion of an AMQP array of this type, the
     * array encoding, size and element count should be assumed to be managed by
     * the caller.
     *
     * @param buffer
     *      The buffer to write the AMQP array elements to
     * @param state
     *      The current encoder state
     * @param values
     *      The array of values that is to be written.
     *
     * @throws EncodeException if an error occurs while encoding the given value.
     */
    void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) throws EncodeException;

}
