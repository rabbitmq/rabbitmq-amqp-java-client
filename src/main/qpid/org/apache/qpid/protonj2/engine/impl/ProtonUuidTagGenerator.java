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
package org.apache.qpid.protonj2.engine.impl;

import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferUtils;
import org.apache.qpid.protonj2.engine.DeliveryTagGenerator;
import org.apache.qpid.protonj2.types.DeliveryTag;

/**
 * Built in proton {@link DeliveryTagGenerator} that creates new {@link DeliveryTag} values
 * backed by randomly generated UUID instances.
 */
public class ProtonUuidTagGenerator extends ProtonDeliveryTagGenerator {

    @Override
    public DeliveryTag nextTag() {
        return new ProtonUuidDeliveryTag(UUID.randomUUID());
    }

    private static final class ProtonUuidDeliveryTag implements DeliveryTag {

        private static final int BYTES = 16;

        private final UUID tagValue;

        public ProtonUuidDeliveryTag(UUID tagValue) {
            this.tagValue = tagValue;
        }

        @Override
        public int tagLength() {
            return BYTES;
        }

        @Override
        public byte[] tagBytes() {
            final byte[] tagView = new byte[BYTES];

            ProtonBufferUtils.writeLong(tagValue.getMostSignificantBits(), tagView, 0);
            ProtonBufferUtils.writeLong(tagValue.getLeastSignificantBits(), tagView, Long.BYTES);

            return tagView;
        }

        @Override
        public ProtonBuffer tagBuffer() {
            return ProtonBufferAllocator.defaultAllocator().copy(tagBytes()).convertToReadOnly();
        }

        @Override
        public DeliveryTag copy() {
            return new ProtonUuidDeliveryTag(tagValue);
        }

        @Override
        public void writeTo(ProtonBuffer buffer) {
            buffer.writeLong(tagValue.getMostSignificantBits());
            buffer.writeLong(tagValue.getLeastSignificantBits());
        }

        @Override
        public int hashCode() {
            return tagValue.hashCode();
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

            return tagValue.equals(((ProtonUuidDeliveryTag) obj).tagValue);
        }

        @Override
        public String toString() {
            return tagValue.toString();
        }
    }
}
