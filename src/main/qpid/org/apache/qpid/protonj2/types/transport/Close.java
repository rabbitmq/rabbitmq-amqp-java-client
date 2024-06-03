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
package org.apache.qpid.protonj2.types.transport;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

/**
 * Defines an AMQP Close performative used to end AMQP Connection instances.
 */
public final class Close implements Performative {

    /**
     * The {@link UnsignedLong} descriptor code that defines this AMQP type.
     */
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000018L);

    /**
     * The {@link Symbol} descriptor code that defines this AMQP type.
     */
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:close:list");

    private ErrorCondition error;

    /**
     * @return the {@link ErrorCondition} conveyed in the {@link Close} or null if non set.
     */
    public ErrorCondition getError() {
        return error;
    }

    /**
     * Sets the error that should be conveyed with the AMQP {@link Close}.
     *
     * @param error
     * 		The {@link ErrorCondition} to convey with the {@link Close} or null if none.
     *
     * @return this {@link Close} instance.
     */
    public Close setError(ErrorCondition error) {
        this.error = error;
        return this;
    }

    @Override
    public Close copy() {
        Close copy = new Close();
        copy.setError(error == null ? null : error.copy());
        return copy;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.CLOSE;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleClose(this, payload, channel, context);
    }

    @Override
    public String toString() {
        return "Close{" + "error=" + error + '}';
    }
}
