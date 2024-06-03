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
package org.apache.qpid.protonj2.client;

import java.io.InputStream;
import java.util.Map;

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;

/**
 * Incoming Delivery type that provides access to the message and the delivery
 * data along with methods for settling the delivery when processing completes.
 */
public interface Delivery {

    /**
     * @return the {@link Receiver} that originated this {@link Delivery}.
     */
    Receiver receiver();

    /**
     * Decode the {@link Delivery} payload and return an {@link Message} object.
     * <p>
     * If the incoming message carried any delivery annotations they can be accessed via the
     * {@link #annotations()} method.  Re-sending the returned message will not also
     * send the incoming delivery annotations, the sender must include them in the
     * {@link Sender#send(Message, Map)} call if they are to be forwarded onto the next recipient.
     * <p>
     * Calling this message claims the payload of the delivery for the returned {@link Message} and
     * excludes use of the {@link #rawInputStream()} method of the {@link Delivery} object.  Calling
     * the {@link #rawInputStream()} method after calling this method throws {@link ClientIllegalStateException}.
     * <p>
     * Care should be taken if attempting to specify anything other than the wild card type for the body
     * of the returned message as the wrong type specification can lead to an exception.
     *
     * @param <E> The type of message body that should be contained in the returned {@link Message}.
     *
     * @return a {@link Message} instance that wraps the decoded payload.
     *
     * @throws ClientException if an error occurs while decoding the payload.
     */
    <E> Message<E> message() throws ClientException;

    /**
     * Create and return an {@link InputStream} that reads the raw payload bytes of the given {@link Delivery}.
     * <p>
     * Calling this method claims the payload of the delivery for the returned {@link InputStream} and excludes
     * use of the {@link #message()} and {@link #annotations()} methods of the {@link Delivery} object.  Closing
     * the returned input stream discards any unread bytes from the delivery payload.  Calling the {@link #message()}
     * or {@link #annotations()} methods after calling this method throws {@link ClientIllegalStateException}.
     *
     * @return an {@link InputStream} instance that can be used to read the raw delivery payload.
     *
     * @throws ClientException if an error occurs while decoding the payload.
     */
    InputStream rawInputStream() throws ClientException;

    /**
     * Decodes the {@link Delivery} payload and returns a {@link Map} containing a copy
     * of any associated {@link DeliveryAnnotations} that were transmitted with the {@link Message}
     * payload of this {@link Delivery}.
     * <p>
     * Calling this message claims the payload of the delivery for the returned {@link Map} and the decoded
     * {@link Message} that can be accessed via the {@link #message()} method and  excludes use of the
     * {@link #rawInputStream()} method of the {@link Delivery} object.  Calling the {@link #rawInputStream()}
     * method after calling this method throws {@link ClientIllegalStateException}.
     *
     * @return copy of the delivery annotations that were transmitted with the {@link Message} payload.
     *
     * @throws ClientException if an error occurs while decoding the payload.
     */
    Map<String, Object> annotations() throws ClientException;

    /**
     * Accepts and settles the delivery.
     *
     * @return this {@link Delivery} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    Delivery accept() throws ClientException;

    /**
     * Releases and settles the delivery.
     *
     * @return this {@link Delivery} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    Delivery release() throws ClientException;

    /**
     * Rejects and settles the delivery, sending supplied error information along
     * with the rejection.
     *
     * @param condition
     *      The error condition value to supply with the rejection.
     * @param description
     *      The error description value to supply with the rejection.
     *
     * @return this {@link Delivery} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    Delivery reject(String condition, String description) throws ClientException;

    /**
     * Modifies and settles the delivery.
     *
     * @param deliveryFailed
     *      Indicates if the modified delivery failed.
     * @param undeliverableHere
     *      Indicates if the modified delivery should not be returned here again.
     *
     * @return this {@link Delivery} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    Delivery modified(boolean deliveryFailed, boolean undeliverableHere) throws ClientException;

    /**
     * Updates the DeliveryState, and optionally settle the delivery as well.
     *
     * @param state
     *            the delivery state to apply
     * @param settle
     *            whether to {@link #settle()} the delivery at the same time
     *
     * @return this {@link Delivery} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    Delivery disposition(DeliveryState state, boolean settle) throws ClientException;

    /**
     * Settles the delivery locally.
     *
     * @return the delivery
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    Delivery settle() throws ClientException;

    /**
     * @return true if the delivery has been locally settled.
     *
     * @throws ClientException if an error occurs while reading the settled state
     */
    boolean settled() throws ClientException;

    /**
     * Gets the current local state for the delivery.
     *
     * @return the delivery state
     *
     * @throws ClientException if an error occurs while reading the delivery state
     */
    DeliveryState state() throws ClientException;

    /**
     * Gets the current remote state for the delivery.
     *
     * @return the remote delivery state
     *
     * @throws ClientException if an error occurs while reading the remote delivery state
     */
    DeliveryState remoteState() throws ClientException;

    /**
     * Gets whether the delivery was settled by the remote peer yet.
     *
     * @return whether the delivery is remotely settled
     *
     * @throws ClientException if an error occurs while reading the remote settlement state
     */
    boolean remoteSettled() throws ClientException;

    /**
     * Gets the message format for the current delivery.
     *
     * @return the message format
     *
     * @throws ClientException if an error occurs while reading the delivery message format
     */
    int messageFormat() throws ClientException;

}