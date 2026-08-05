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
package org.apache.qpid.protonj2.engine;

import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;

/**
 * Configuration options for the Engine
 */
public interface EngineConfiguration {

    /**
     * Sets the ProtonBufferAllocator used by this Engine.
     * <p>
     * When copying data, encoding types or otherwise needing to allocate memory
     * storage the Engine will use the assigned {@link ProtonBufferAllocator}.
     * If no allocator is assigned the Engine will use the default allocator.
     *
     * @param allocator
     *      The Allocator instance to use from this {@link Engine}.
     *
     * @return this {@link EngineConfiguration} for chaining.
     */
    EngineConfiguration setBufferAllocator(ProtonBufferAllocator allocator);

    /**
     * {@return the currently assigned {@link ProtonBufferAllocator}}
     */
    ProtonBufferAllocator getBufferAllocator();

    /**
     * Enables AMQP frame tracing from engine to the system output.  Depending
     * on the underlying engine composition frame tracing may not be possible
     * in which case this method will have no effect and the access method
     * {@link EngineConfiguration#isTraceFrames()} will return false.
     *
     * @param traceFrames
     *      true to enable engine frame tracing, false to disable it.
     *
     * @return this {@link EngineConfiguration} for chaining.
     */
    EngineConfiguration setTraceFrames(boolean traceFrames);

    /**
     * {@return true if the engine will emit frames to system output}
     */
    boolean isTraceFrames();

    /**
     * Sets the configured maximum number of Transfer frames that can make up a single completed
     * delivery. If the delivery is not completed within this number of transfer frames the engine
     * may either close the associated link or the connection in its entirety. An engine implementation
     * may opt not to implement this feature in which case the value should be fixed at zero and any
     * assignment should be ignored. If the value is configured as zero then the behavior should be
     * to treat that as no limit was assigned.
     *
     * @param maxTransfers
     * 		The maximum number of Transfers allowed for a single inbound delivery.
     *
     * @return this {@link EngineConfiguration} for chaining.
     */
    default EngineConfiguration setMaxTransfersPerDelivery(int maxTransfers) {
        throw new UnsupportedOperationException("Default configuration does not support assigning a default max transfers value");
    }

    /**
     * {@return the maximum number of Transfer frames allowed for a single Delivery}
     */
    default int getMaxTransfersPerDelivery() {
        return 0;
    }
}
