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

/**
 * Describes the state of a delivery at a link end-point.
 *
 * Note that the the sender is the owner of the state.
 * The receiver merely influences the state.
 */
public interface DeliveryState {

    /**
     * An enumeration of the valid {@link DeliveryState} types that the library can provide.
     */
    enum DeliveryStateType {
        Accepted,
        Declared,
        Modified,
        Received,
        Rejected,
        Released,
        Transactional
    }

    /**
     * @return the {@link DeliveryStateType} that this instance represents.
     */
    DeliveryStateType getType();

}
