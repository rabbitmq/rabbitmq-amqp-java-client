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

public final class Source implements Terminus {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000028L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:source:list");

    private static final int ADDRESS = 1;
    private static final int DURABLE = 2;
    private static final int EXPIRY_PLICY = 4;
    private static final int TIMEOUT = 8;
    private static final int DYNAMIC = 16;
    private static final int DYNAMIC_NODE_PROPERTIES = 32;
    private static final int DISTRIBUTION_MODE = 64;
    private static final int FILTER = 128;
    private static final int DEFAULT_OUTCOME = 256;
    private static final int OUTCOMES = 512;
    private static final int CAPABILITIES = 1024;

    private int modified = 0;

    private String address;
    private TerminusDurability durable = TerminusDurability.NONE;
    private TerminusExpiryPolicy expiryPolicy = TerminusExpiryPolicy.SESSION_END;
    private UnsignedInteger timeout = UnsignedInteger.ZERO;
    private boolean dynamic;
    private Map<Symbol, Object> dynamicNodeProperties;
    private Symbol distributionMode;
    private Map<Symbol, Object> filter;
    private Outcome defaultOutcome;
    private Symbol[] outcomes;
    private Symbol[] capabilities;

    public Source() {
    }

    private Source(Source other) {
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

        this.distributionMode = other.distributionMode;

        if (other.filter != null) {
            this.filter = new HashMap<>(other.filter);
        }

        this.defaultOutcome = other.defaultOutcome;

        if (other.outcomes != null) {
            this.outcomes = other.outcomes.clone();
        }

        this.modified = other.modified;
    }

    @Override
    public Source copy() {
        return new Source(this);
    }

    //----- Query the state of the Source object -----------------------------//

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

    public boolean hasDistributionMode() {
        return (modified & DISTRIBUTION_MODE) == DISTRIBUTION_MODE;
    }

    public boolean hasFilter() {
        return (modified & FILTER) == FILTER;
    }

    public boolean hasDefaultOutcome() {
        return (modified & DEFAULT_OUTCOME) == DEFAULT_OUTCOME;
    }

    public boolean hasOutcomes() {
        return (modified & OUTCOMES) == OUTCOMES;
    }

    public boolean hasCapabilities() {
        return (modified & CAPABILITIES) == CAPABILITIES;
    }

    public String getAddress() {
        return address;
    }

    public Source setAddress(String address) {
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

    public Source setDurable(TerminusDurability durable) {
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

    public Source setExpiryPolicy(TerminusExpiryPolicy expiryPolicy) {
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

    public Source setTimeout(UnsignedInteger timeout) {
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

    public Source setDynamic(boolean dynamic) {
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
    public Source setDynamicNodeProperties(Map<Symbol, ?> dynamicNodeProperties) {
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

    public Source setCapabilities(Symbol... capabilities) {
        if (capabilities == null) {
            modified &= ~CAPABILITIES;
        } else {
            modified |= CAPABILITIES;
        }

        this.capabilities = capabilities;
        return this;
    }

    public Symbol getDistributionMode() {
        return distributionMode;
    }

    public Source setDistributionMode(Symbol distributionMode) {
        if (distributionMode == null) {
            modified &= ~DISTRIBUTION_MODE;
        } else {
            modified |= DISTRIBUTION_MODE;
        }

        this.distributionMode = distributionMode;
        return this;
    }

    public Map<Symbol, Object> getFilter() {
        return filter;
    }

    @SuppressWarnings("unchecked")
    public Source setFilter(Map<Symbol, ?> filter) {
        if (filter == null) {
            modified &= ~FILTER;
        } else {
            modified |= FILTER;
        }

        this.filter = (Map<Symbol, Object>) filter;
        return this;
    }

    public Outcome getDefaultOutcome() {
        return defaultOutcome;
    }

    public Source setDefaultOutcome(Outcome defaultOutcome) {
        if (defaultOutcome == null) {
            modified &= ~DEFAULT_OUTCOME;
        } else {
            modified |= DEFAULT_OUTCOME;
        }

        this.defaultOutcome = defaultOutcome;
        return this;
    }

    public Symbol[] getOutcomes() {
        return outcomes;
    }

    public Source setOutcomes(Symbol... outcomes) {
        if (outcomes == null) {
            modified &= ~OUTCOMES;
        } else {
            modified |= OUTCOMES;
        }

        this.outcomes = outcomes;
        return this;
    }

    @Override
    public String toString() {
        return "Source{" +
               "address='" + getAddress() + '\'' +
               ", durable=" + getDurable() +
               ", expiryPolicy=" + getExpiryPolicy() +
               ", timeout=" + getTimeout() +
               ", dynamic=" + isDynamic() +
               ", dynamicNodeProperties=" + getDynamicNodeProperties() +
               ", distributionMode=" + distributionMode +
               ", filter=" + filter +
               ", defaultOutcome=" + defaultOutcome +
               ", outcomes=" + (outcomes == null ? null : Arrays.asList(outcomes)) +
               ", capabilities=" + (getCapabilities() == null ? null : Arrays.asList(getCapabilities())) +
               '}';
    }
}
