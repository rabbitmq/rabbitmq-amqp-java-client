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

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.DeliveryState;

public final class Received implements DeliveryState {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000023L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:received:list");

    private static final int SECTION_NUMBER = 1;
    private static final int SECTION_OFFSET = 2;

    private int modified = 0;

    private UnsignedInteger sectionNumber;
    private UnsignedLong sectionOffset;

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

    public boolean hasSectionNumber() {
        return (modified & SECTION_NUMBER) == SECTION_NUMBER;
    }

    public boolean hasSectionOffset() {
        return (modified & SECTION_OFFSET) == SECTION_OFFSET;
    }

    public UnsignedInteger getSectionNumber() {
        return sectionNumber;
    }

    public Received setSectionNumber(UnsignedInteger sectionNumber) {
        if (sectionNumber != null) {
            modified |= SECTION_NUMBER;
        } else {
            modified &= ~SECTION_NUMBER;
        }

        this.sectionNumber = sectionNumber;
        return this;
    }

    public UnsignedLong getSectionOffset() {
        return sectionOffset;
    }

    public Received setSectionOffset(UnsignedLong sectionOffset) {
        if (sectionOffset != null) {
            modified |= SECTION_OFFSET;
        } else {
            modified &= ~SECTION_OFFSET;
        }

        this.sectionOffset = sectionOffset;
        return this;
    }

    @Override
    public String toString() {
        return "Received{" +
               "sectionNumber=" + sectionNumber +
               ", sectionOffset=" + sectionOffset +
               '}';
    }

    @Override
    public DeliveryStateType getType() {
        return DeliveryStateType.Received;
    }
}
