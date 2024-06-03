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
package org.apache.qpid.protonj2.client.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Tracks available known capabilities for the connection to allow the client
 * to know what features are supported on the current connection.
 */
public final class ClientConnectionCapabilities {

    private boolean anonymousRelaySupported;
    private boolean delayedDeliverySupported;

    /**
     * @return true if the remote indicated that it supports anonymous relay links.
     */
    public boolean anonymousRelaySupported() {
        return this.anonymousRelaySupported;
    }

    /**
     * @return true if the remote indicated that it supports delivery delay annotations from client messages.
     */
    public boolean deliveryDelaySupported() {
        return this.delayedDeliverySupported;
    }

    @SuppressWarnings("unchecked")
    ClientConnectionCapabilities determineCapabilities(Connection connection) {
        final Symbol[] desired = connection.getDesiredCapabilities();
        final Symbol[] offered = connection.getRemoteOfferedCapabilities();

        final List<Symbol> offeredSymbols = offered != null ? Arrays.asList(offered) : Collections.EMPTY_LIST;
        final List<Symbol> desiredSymbols = desired != null ? Arrays.asList(desired) : Collections.EMPTY_LIST;

        anonymousRelaySupported = checkAnonymousRelaySupported(desiredSymbols, offeredSymbols);
        delayedDeliverySupported = checkDeliveryRelaySupported(desiredSymbols, offeredSymbols);

        return this;
    }

    private boolean checkAnonymousRelaySupported(List<Symbol> desired, List<Symbol> offered) {
        return offered.contains(ClientConstants.ANONYMOUS_RELAY);
    }

    private boolean checkDeliveryRelaySupported(List<Symbol> desired, List<Symbol> offered) {
        return offered.contains(ClientConstants.DELAYED_DELIVERY);
    }
}
