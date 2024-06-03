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
package org.apache.qpid.protonj2.types.messaging;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class AmqpSequence<E> implements Section<List<E>> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000076L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:amqp-sequence:list");

    private final List<E> value;

    public AmqpSequence(List<E> value) {
        this.value = value;
    }

    @Override
    public List<E> getValue() {
        return value;
    }

    public AmqpSequence<E> copy() {
        return new AmqpSequence<>(value != null ? new ArrayList<>(value) : null);
    }

    @Override
    public String toString() {
        return "AmqpSequence{ " + value + " }";
    }

    @Override
    public SectionType getType() {
        return SectionType.AmqpSequence;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        AmqpSequence<?> other = (AmqpSequence<?>) obj;
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }

        return true;
    }
}
