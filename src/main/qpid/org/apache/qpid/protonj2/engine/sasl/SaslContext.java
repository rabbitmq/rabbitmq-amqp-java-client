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
package org.apache.qpid.protonj2.engine.sasl;

import org.apache.qpid.protonj2.engine.Attachments;
import org.apache.qpid.protonj2.engine.EngineSaslDriver.SaslState;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * The basic SASL context APIs common to both client and server sides of the SASL exchange.
 */
public interface SaslContext {

    /**
     * Indicates the role that this SASL context plays either server or client.
     */
    enum Role { CLIENT, SERVER }

    /**
     * Links a given resource to this {@link SaslContext}.
     *
     * @param resource
     *      The resource to link to this {@link SaslContext}.
     *
     * @return this {@link SaslContext} instance.
     */
    SaslContext setLinkedResource(Object resource);

    /**
     * @param <T> The type that the linked resource should be cast to on return.
     *
     * @return the user set linked resource for this {@link SaslContext} instance.
     */
    <T> T getLinkedResource();

    /**
     * Gets the linked resource (if set) and returns it using the type information
     * provided to cast the returned value.
     *
     * @param <T> The type to cast the linked resource to if one is set.
     * @param typeClass the type's Class which is used for casting the returned value.
     *
     * @return the user set linked resource for this Context instance.
     *
     * @throws ClassCastException if the linked resource cannot be cast to the type requested.
     */
    <T> T getLinkedResource(Class<T> typeClass);

    /**
     * Returns a mutable context that the application layer can use to store meaningful data for itself
     * in relation to this specific SASL context object.
     *
     * @return the {@link Attachments} instance that is associated with this {@link SaslContext}
     */
    Attachments getAttachments();

    /**
     * Return the Role of the context implementation.
     *
     * @return the Role of this SASL Context
     */
    Role getRole();

    /**
     * Returns if the SASL context has been marked as completed. A completed context
     * does not imply the SASL authentication was successful, the caller should check
     * the state of the {@link #getSaslOutcome()} value to determine if the SASL
     * authentication process was successful or not.
     *
     * @return true if SASL authentication has completed
     */
    boolean isDone();

    /**
     * @return true if this is a SASL server context.
     */
    default boolean isServer() {
        return getRole() == Role.SERVER;
    }

    /**
     * @return true if this is a SASL client context.
     */
    default boolean isClient() {
        return getRole() == Role.SERVER;
    }

    /**
     * Provides a low level outcome value for the SASL authentication process.
     * <p>
     * If the SASL exchange is ongoing or the SASL layer was skipped because a
     * particular engine configuration allows such behavior then this method
     * should return null to indicate no SASL outcome is available.
     *
     * @return the SASL outcome code that results from authentication
     */
    SaslOutcome getSaslOutcome();

    /**
     * Returns a SaslState that indicates the current operating state of the SASL
     * negotiation process or conversely if no SASL layer is configured this method
     * should return the no-SASL state.  This method must never return a null result.
     *
     * @return the current state of SASL Authentication.
     */
    SaslState getSaslState();

    /**
     * After the server has sent its supported mechanisms this method will return a
     * copy of that list for review by the server event handler.  If called before
     * the server has sent the mechanisms list this method will return null.
     *
     * @return the mechanisms that the server offered to the client.
     */
    Symbol[] getServerMechanisms();

    /**
     * Returns the mechanism that was sent to the server to select the SASL mechanism
     * to use for negotiations.  If called before the client has sent its chosen mechanism
     * this method returns null.
     *
     * @return the SASL mechanism that the client selected to use for negotiation.
     */
    Symbol getChosenMechanism();

    /**
     * The DNS name of the host (either fully qualified or relative) that was sent to the server
     * which define the host the sending peer is connecting to.  If called before the client sent
     * the host name information to the server this method returns null.
     *
     * @return the host name the client has requested to connect to.
     */
    String getHostname();

}
