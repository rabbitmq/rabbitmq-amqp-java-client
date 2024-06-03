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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class DeliveryAnnotations implements Section<Map<Symbol, Object>> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000071L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:delivery-annotations:map");

    private final Map<Symbol, Object> value;

    @SuppressWarnings("unchecked")
    public DeliveryAnnotations(Map<Symbol, ?> value) {
        this.value = (Map<Symbol, Object>) value;
    }

    public DeliveryAnnotations copy() {
        return new DeliveryAnnotations(value == null ? null : new LinkedHashMap<>(value));
    }

    @Override
    public Map<Symbol, Object> getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "DeliveryAnnotations{ " + value + " }";
    }

    @Override
    public SectionType getType() {
        return SectionType.DeliveryAnnotations;
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

        DeliveryAnnotations other = (DeliveryAnnotations) obj;
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
