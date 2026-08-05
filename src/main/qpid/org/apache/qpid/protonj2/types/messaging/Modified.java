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

import java.util.Map;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.DeliveryState;

public final class Modified implements DeliveryState, Outcome {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000027L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:modified:list");

    private static final int DELIVERY_FAILED = 1;
    private static final int UNDELIVERABLE_HERE = 2;
    private static final int ANNOTATIONS = 4;

    private int modified = 0;

    private boolean deliveryFailed;
    private boolean undeliverableHere;
    private Map<Symbol, Object> messageAnnotations;

    public Modified() {}

    public Modified(boolean deliveryFailed, boolean undeliverableHere) {
        this(deliveryFailed, undeliverableHere, null);
    }

    public Modified(boolean deliveryFailed, boolean undeliverableHere, Map<Symbol, Object> annotations) {
        setDeliveryFailed(deliveryFailed);
        setUndeliverableHere(undeliverableHere);
        setMessageAnnotations(annotations);
    }

    public boolean isEmpty() {
        return modified == 0;
    }

    public int getElementCount() {
        return 32 - Integer.numberOfLeadingZeros(modified);
    }

    public boolean hasElement(int index) {
        final int value = 1 << index;
        return (modified & value) == value;
    }

    public boolean hasDeliveryFailed() {
        return (modified & DELIVERY_FAILED) == DELIVERY_FAILED;
    }

    public boolean hasUndeliverableHere() {
        return (modified & UNDELIVERABLE_HERE) == UNDELIVERABLE_HERE;
    }

    public boolean hasAnnotations() {
        return (modified & ANNOTATIONS) == ANNOTATIONS;
    }

    public boolean isDeliveryFailed() {
        return deliveryFailed;
    }

    public Modified setDeliveryFailed(boolean deliveryFailed) {
        if (deliveryFailed) {
            modified |= DELIVERY_FAILED;
        } else {
            modified &= ~DELIVERY_FAILED;
        }

        this.deliveryFailed = deliveryFailed;
        return this;
    }

    public boolean isUndeliverableHere() {
        return undeliverableHere;
    }

    public Modified setUndeliverableHere(boolean undeliverableHere) {
        if (undeliverableHere) {
            modified |= UNDELIVERABLE_HERE;
        } else {
            modified &= ~UNDELIVERABLE_HERE;
        }

        this.undeliverableHere = undeliverableHere;
        return this;
    }

    public Map<Symbol, Object> getMessageAnnotations() {
        return messageAnnotations;
    }

    @SuppressWarnings("unchecked")
    public Modified setMessageAnnotations(Map<Symbol, ?> messageAnnotations) {
        if (messageAnnotations != null) {
            modified |= ANNOTATIONS;
        } else {
            modified &= ~ANNOTATIONS;
        }

        this.messageAnnotations = (Map<Symbol, Object>) messageAnnotations;
        return this;
    }

    @Override
    public String toString() {
        return "Modified{" +
               "deliveryFailed=" + deliveryFailed +
               ", undeliverableHere=" + undeliverableHere +
               ", messageAnnotations=" + messageAnnotations +
               '}';
    }

    @Override
    public DeliveryStateType getType() {
        return DeliveryStateType.Modified;
    }
}
