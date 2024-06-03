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

public final class Disposition implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000015L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:disposition:list");

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static final int ROLE = 1;
    private static final int FIRST = 2;
    private static final int LAST = 4;
    private static final int SETTLED = 8;
    private static final int STATE = 16;
    private static final int BATCHABLE = 32;

    private int modified = 0;

    private Role role = Role.SENDER;
    private long first;
    private long last;
    private boolean settled;
    private DeliveryState state;
    private boolean batchable;

    //----- Query the state of the Header object -----------------------------//

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

    public boolean hasRole() {
        return (modified & ROLE) == ROLE;
    }

    public boolean hasFirst() {
        return (modified & FIRST) == FIRST;
    }

    public boolean hasLast() {
        return (modified & LAST) == LAST;
    }

    public boolean hasSettled() {
        return (modified & SETTLED) == SETTLED;
    }

    public boolean hasState() {
        return (modified & STATE) == STATE;
    }

    public boolean hasBatchable() {
        return (modified & BATCHABLE) == BATCHABLE;
    }

    //----- Access to the member data with state checks

    public Role getRole() {
        return role;
    }

    public Disposition setRole(Role role) {
        if (role == null) {
            throw new NullPointerException("Role cannot be null");
        } else {
            modified |= ROLE;
        }

        this.role = role;
        return this;
    }

    public Disposition clearRole() {
        modified &= ~ROLE;
        role = Role.SENDER;
        return this;
    }

    public long getFirst() {
        return first;
    }

    public Disposition setFirst(int first) {
        modified |= FIRST;
        this.first = Integer.toUnsignedLong(first);
        return this;
    }

    public Disposition setFirst(long first) {
        if (first < 0 || first > UINT_MAX) {
            throw new IllegalArgumentException("First value given is out of range: " + first);
        } else {
            modified |= FIRST;
        }

        this.first = first;
        return this;
    }

    public Disposition clearFirst() {
        modified &= ~FIRST;
        first = 0;
        return this;
    }

    public long getLast() {
        return last;
    }

    public Disposition setLast(int last) {
        modified |= LAST;
        this.last = Integer.toUnsignedLong(last);
        return this;
    }

    public Disposition setLast(long last) {
        if (last < 0 || last > UINT_MAX) {
            throw new IllegalArgumentException("Last value given is out of range: " + last);
        } else {
            modified |= LAST;
        }

        this.last = last;
        return this;
    }

    public Disposition clearLast() {
        modified &= ~LAST;
        last = 0;
        return this;
    }

    public boolean getSettled() {
        return settled;
    }

    public Disposition setSettled(boolean settled) {
        this.modified |= SETTLED;
        this.settled = settled;
        return this;
    }

    public Disposition clearSettled() {
        modified &= ~SETTLED;
        settled = false;
        return this;
    }

    public DeliveryState getState() {
        return state;
    }

    public Disposition setState(DeliveryState state) {
        if (state != null) {
            this.modified |= STATE;
        } else {
            this.modified &= ~STATE;
        }

        this.state = state;
        return this;
    }

    public Disposition clearState() {
        modified &= ~STATE;
        state = null;
        return this;
    }

    public boolean getBatchable() {
        return batchable;
    }

    public Disposition setBatchable(boolean batchable) {
        this.modified |= BATCHABLE;
        this.batchable = batchable;
        return this;
    }

    public Disposition clearBatchable() {
        modified &= ~BATCHABLE;
        batchable = false;
        return this;
    }

    public Disposition reset() {
        modified = 0;
        role = Role.SENDER;
        first = 0;
        last = 0;
        settled = false;
        state = null;
        batchable = false;

        return this;
    }

    @Override
    public Disposition copy() {
        Disposition copy = new Disposition();

        copy.role = role;
        copy.first = first;
        copy.last = last;
        copy.settled = settled;
        copy.state = state;
        copy.batchable = batchable;
        copy.modified = modified;

        return copy;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.DISPOSITION;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleDisposition(this, payload, channel, context);
    }

    @Override
    public String toString() {
        return "Disposition{" +
               "role=" + role +
               ", first=" + first +
               ", last=" + last +
               ", settled=" + settled +
               ", state=" + state +
               ", batchable=" + batchable +
               '}';
    }
}
