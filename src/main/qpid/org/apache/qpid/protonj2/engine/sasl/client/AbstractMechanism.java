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
package org.apache.qpid.protonj2.engine.sasl.client;

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;

/**
 * Base class for SASL Authentication Mechanism that implements the basic
 * methods of a Mechanism class.
 */
public abstract class AbstractMechanism implements Mechanism {

    protected static final ProtonBuffer EMPTY = ProtonBufferAllocator.defaultAllocator().allocate(0).convertToReadOnly();

    @Override
    public ProtonBuffer getInitialResponse(SaslCredentialsProvider credentials) throws SaslException {
        return EMPTY;
    }

    @Override
    public ProtonBuffer getChallengeResponse(SaslCredentialsProvider credentials, ProtonBuffer challenge) throws SaslException {
        return EMPTY;
    }

    @Override
    public void verifyCompletion() throws SaslException {
        // Default is always valid.
    }

    @Override
    public boolean isEnabledByDefault() {
        return true;
    }

    @Override
    public String toString() {
        return "SASL-" + getName();
    }
}
