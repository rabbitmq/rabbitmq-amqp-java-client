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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class Target implements Terminus {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000029L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:target:list");

    private static final int ADDRESS = 1;
    private static final int DURABLE = 2;
    private static final int EXPIRY_PLICY = 4;
    private static final int TIMEOUT = 8;
    private static final int DYNAMIC = 16;
    private static final int DYNAMIC_NODE_PROPERTIES = 32;
    private static final int CAPABILITIES = 64;

    private int modified = 0;

    private String address;
    private TerminusDurability durable = TerminusDurability.NONE;
    private TerminusExpiryPolicy expiryPolicy = TerminusExpiryPolicy.SESSION_END;
    private UnsignedInteger timeout = UnsignedInteger.ZERO;
    private boolean dynamic;
    private Map<Symbol, Object> dynamicNodeProperties;
    private Symbol[] capabilities;

    public Target() {
    }

    private Target(Target other) {
        this.address = other.address;
        this.durable = other.durable;
        this.expiryPolicy = other.expiryPolicy;
        this.timeout = other.timeout;
        this.dynamic = other.dynamic;

        if (other.dynamicNodeProperties != null) {
            this.dynamicNodeProperties = new HashMap<>(other.dynamicNodeProperties);
        }

        if (other.capabilities != null) {
            this.capabilities = other.capabilities.clone();
        }

        this.modified = other.modified;
    }

    //----- Query the state of the Target object -----------------------------//

    public boolean isEmpty() {
        return modified == 0;
    }

    public boolean hasElement(int index) {
        final int value = 1 << index;
        return (modified & value) == value;
    }

    public int getElementCount() {
        return 32 - Integer.numberOfLeadingZeros(modified);
    }

    public boolean hasAddress() {
        return (modified & ADDRESS) == ADDRESS;
    }

    public boolean hasDurable() {
        return (modified & DURABLE) == DURABLE;
    }

    public boolean hasExpiryPolicy() {
        return (modified & EXPIRY_PLICY) == EXPIRY_PLICY;
    }

    public boolean hasTimeout() {
        return (modified & TIMEOUT) == TIMEOUT;
    }

    public boolean hasDynamic() {
        return (modified & DYNAMIC) == DYNAMIC;
    }

    public boolean hasDynamicNodeProperties() {
        return (modified & DYNAMIC_NODE_PROPERTIES) == DYNAMIC_NODE_PROPERTIES;
    }

    public boolean hasCapabilities() {
        return (modified & CAPABILITIES) == CAPABILITIES;
    }

    @Override
    public Target copy() {
        return new Target(this);
    }

    public String getAddress() {
        return address;
    }

    public Target setAddress(String address) {
        if (address == null) {
            modified &= ~ADDRESS;
        } else {
            modified |= ADDRESS;
        }

        this.address = address;
        return this;
    }

    public TerminusDurability getDurable() {
        return durable;
    }

    public Target setDurable(TerminusDurability durable) {
        if (durable == null) {
            modified &= ~DURABLE;
        } else {
            modified |= DURABLE;
        }

        this.durable = durable == null ? TerminusDurability.NONE : durable;
        return this;
    }

    public TerminusExpiryPolicy getExpiryPolicy() {
        return expiryPolicy;
    }

    public Target setExpiryPolicy(TerminusExpiryPolicy expiryPolicy) {
        if (expiryPolicy == null) {
            modified &= ~EXPIRY_PLICY;
        } else {
            modified |= EXPIRY_PLICY;
        }

        this.expiryPolicy = expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : expiryPolicy;
        return this;
    }

    public UnsignedInteger getTimeout() {
        return timeout;
    }

    public Target setTimeout(UnsignedInteger timeout) {
        if (timeout == null) {
            modified &= ~TIMEOUT;
        } else {
            modified |= TIMEOUT;
        }

        this.timeout = timeout;
        return this;
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public Target setDynamic(boolean dynamic) {
        if (dynamic == false) {
            modified &= ~DYNAMIC;
        } else {
            modified |= DYNAMIC;
        }

        this.dynamic = dynamic;
        return this;
    }

    public Map<Symbol, Object> getDynamicNodeProperties() {
        return dynamicNodeProperties;
    }

    @SuppressWarnings("unchecked")
    public Target setDynamicNodeProperties(Map<Symbol, ?> dynamicNodeProperties) {
        if (dynamicNodeProperties == null) {
            modified &= ~DYNAMIC_NODE_PROPERTIES;
        } else {
            modified |= DYNAMIC_NODE_PROPERTIES;
        }

        this.dynamicNodeProperties = (Map<Symbol, Object>) dynamicNodeProperties;
        return this;
    }

    public Symbol[] getCapabilities() {
        return capabilities;
    }

    public Target setCapabilities(Symbol... capabilities) {
        if (capabilities == null) {
            modified &= ~CAPABILITIES;
        } else {
            modified |= CAPABILITIES;
        }

        this.capabilities = capabilities;
        return this;
    }

    @Override
    public String toString() {
        return "Target{" +
               "address='" + getAddress() + '\'' +
               ", durable=" + getDurable() +
               ", expiryPolicy=" + getExpiryPolicy() +
               ", timeout=" + getTimeout() +
               ", dynamic=" + isDynamic() +
               ", dynamicNodeProperties=" + getDynamicNodeProperties() +
               ", capabilities=" + (getCapabilities() == null ? null : Arrays.asList(getCapabilities())) +
               '}';
    }
}
