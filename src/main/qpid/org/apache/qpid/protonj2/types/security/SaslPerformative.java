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
package org.apache.qpid.protonj2.types.security;

/**
 * Marker interface for AMQP Performatives
 */
public interface SaslPerformative {

    enum SaslPerformativeType {
        INIT,
        MECHANISMS,
        CHALLENGE,
        RESPONSE,
        OUTCOME
    }

    SaslPerformative copy();

    SaslPerformativeType getPerformativeType();

    interface SaslPerformativeHandler<E> {

        default void handleMechanisms(SaslMechanisms saslMechanisms, E context) {}
        default void handleInit(SaslInit saslInit, E context) {}
        default void handleChallenge(SaslChallenge saslChallenge, E context) {}
        default void handleResponse(SaslResponse saslResponse, E context) {}
        default void handleOutcome(SaslOutcome saslOutcome, E context) {}

    }

    <E> void invoke(SaslPerformativeHandler<E> handler, E context);

}
