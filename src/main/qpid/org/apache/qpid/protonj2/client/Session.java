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

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientTransactionNotActiveException;
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;

/**
 * Session object used to create {@link Sender} and {@link Receiver} instances.
 * <p>
 * A session can serve as a context for a running transaction in which case care should be
 * taken to ensure that work done is inside the scope of the transaction begin and end calls
 * such as ensuring that receivers accept and settle before calling transaction commit if
 * the work is meant to be within the transaction scope.
 */
public interface Session extends AutoCloseable {

    /**
     * @return the {@link Client} instance that holds this session's {@link Connection}
     */
    Client client();

    /**
     * @return the {@link Connection} that created and holds this {@link Session}.
     */
    Connection connection();

    /**
     * @return a {@link Future} that will be completed when the remote opens this {@link Session}.
     */
    Future<Session> openFuture();

    /**
     * Requests a close of the {@link Session} at the remote and waits until the Session has been
     * fully closed or until the configured {@link SessionOptions#closeTimeout()} is exceeded.
     */
    @Override
    void close();

    /**
     * Requests a close of the {@link Session} at the remote and waits until the Session has been
     * fully closed or until the configured {@link SessionOptions#closeTimeout()} is exceeded.
     *
     * @param error
     *      The {@link ErrorCondition} to transmit to the remote along with the close operation.
     */
    void close(ErrorCondition error);

    /**
     * Requests a close of the {@link Session} at the remote and returns a {@link Future} that will be
     * completed once the session has been remotely closed or an error occurs.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Session}.
     */
    Future<Session> closeAsync();

    /**
     * Requests a close of the {@link Session} at the remote and returns a {@link Future} that will be
     * completed once the session has been remotely closed or an error occurs.
     *
     * @param error
     * 		The {@link ErrorCondition} to transmit to the remote along with the close operation.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Session}.
     */
    Future<Session> closeAsync(ErrorCondition error);

    /**
     * Creates a receiver used to consume messages from the given node address.
     *
     * @param address
     *            The source address to attach the consumer to.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openReceiver(String address) throws ClientException;

    /**
     * Creates a receiver used to consume messages from the given node address.
     *
     * @param address
     *            The source address to attach the consumer to.
     * @param receiverOptions
     *            The options for this receiver.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a receiver used to consume messages from the given node address and configure it
     * such that the remote create a durable node.
     *
     * @param address
     * 			The source address to attach the consumer to.
     * @param subscriptionName
     * 			The name to give the subscription (link name).
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDurableReceiver(String address, String subscriptionName) throws ClientException;

    /**
     * Creates a receiver used to consume messages from the given node address and configure it
     * such that the remote create a durable node.
     *
     * @param address
     *            The source address to attach the consumer to.
     * @param subscriptionName
     * 			The name to give the subscription (link name).
     * @param receiverOptions
     *            The options for this receiver.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDurableReceiver(String address, String subscriptionName, ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from the given node address.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver() throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from the given node address.
     *
     * @param dynamicNodeProperties
     * 		The dynamic node properties to be applied to the node created by the remote.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties) throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from the given node address.
     *
     * @param receiverOptions
     * 		The options for this receiver.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver(ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from the given node address.
     *
     * @param dynamicNodeProperties
     * 		The dynamic node properties to be applied to the node created by the remote.
     * @param receiverOptions
     *      The options for this receiver.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a sender used to send messages to the given node address. If no
     * address (i.e null) is specified then a sender will be established to the
     * 'anonymous relay' and each message must specify its destination address.
     *
     * @param address
     *            The target address to attach to, or null to attach to the
     *            anonymous relay.
     *
     * @return the newly created {@link Sender}.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openSender(String address) throws ClientException;

    /**
     * Creates a sender used to send messages to the given node address. If no
     * address (i.e null) is specified then a sender will be established to the
     * 'anonymous relay' and each message must specify its destination address.
     *
     * @param address
     *            The target address to attach to, or null to attach to the
     *            anonymous relay.
     * @param senderOptions
     *            The options for this sender.
     *
     * @return the newly created {@link Sender}.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openSender(String address, SenderOptions senderOptions) throws ClientException;

    /**
     * Creates a sender that is established to the 'anonymous relay' and as such each
     * message that is sent using this sender must specify an address in its destination
     * address field.
     *
     * @return the newly created {@link Sender}.
     *
     * @throws ClientException if an internal error occurs.
     * @throws ClientUnsupportedOperationException if the remote did not signal support for anonymous relays.
     */
    Sender openAnonymousSender() throws ClientException;

    /**
     * Creates a sender that is established to the 'anonymous relay' and as such each
     * message that is sent using this sender must specify an address in its destination
     * address field.
     *
     * @param senderOptions
     *            The options for this sender.
     *
     * @return the newly created {@link Sender}.
     *
     * @throws ClientException if an internal error occurs.
     * @throws ClientUnsupportedOperationException if the remote did not signal support for anonymous relays.
     */
    Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException;

    /**
     * Returns the properties that the remote provided upon successfully opening the {@link Session}.  If the
     * open has not completed yet this method will block to await the open response which carries the remote
     * properties.  If the remote provides no properties this method will return null.
     *
     * @return any properties provided from the remote once the session has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Session} remote properties.
     */
    Map<String, Object> properties() throws ClientException;

    /**
     * Returns the offered capabilities that the remote provided upon successfully opening the {@link Session}.
     * If the open has not completed yet this method will block to await the open response which carries the
     * remote offered capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any capabilities provided from the remote once the session has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Session} remote offered capabilities.
     */
    String[] offeredCapabilities() throws ClientException;

    /**
     * Returns the desired capabilities that the remote provided upon successfully opening the {@link Session}.
     * If the open has not completed yet this method will block to await the open response which carries the
     * remote desired capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any desired capabilities provided from the remote once the session has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Session} remote desired capabilities.
     */
    String[] desiredCapabilities() throws ClientException;

    /**
     * Opens a new transaction scoped to this {@link Session} if one is not already active.
     *
     * A {@link Session} that has an active transaction will perform all sends and all delivery dispositions
     * under that active transaction.  If the user wishes to send with the same session but outside of a
     * transaction the user must commit the active transaction and not request that a new one be started.
     * A session can only have one active transaction at a time and as such any call to begin while there is
     * a currently active transaction will throw an {@link ClientIllegalStateException} to indicate that
     * the operation being requested is not valid at that time.
     *
     * This is a blocking method that will return successfully only after a new transaction has been started.
     *
     * @return this {@link Session} instance.
     *
     * @throws ClientException if an error occurs while attempting to begin a new transaction.
     */
    Session beginTransaction() throws ClientException;

    /**
     * Commit the currently active transaction in this Session.
     *
     * Commit the currently active transaction in this Session but does not start a new transaction
     * automatically.  If there is no current transaction this method will throw an {@link ClientTransactionNotActiveException}
     * to indicate this error.  If the active transaction has entered an in doubt state or was remotely rolled
     * back this method will throw an error to indicate that the commit failed and that a new transaction
     * need to be started by the user.  When a transaction rolled back error occurs the user should assume that
     * all work performed under that transaction has failed and will need to be attempted under a new transaction.
     *
     * This is a blocking method that will return successfully only after the current transaction has been committed.
     *
     * @return this {@link Session} instance.
     *
     * @throws ClientException if an error occurs while attempting to commit the current transaction.
     */
    Session commitTransaction() throws ClientException;

    /**
     * Roll back the currently active transaction in this Session.
     *
     * Roll back the currently active transaction in this Session but does not automatically start a new
     * transaction.  If there is no current transaction this method will throw an {@link ClientTransactionNotActiveException}
     * to indicate this error.  If the active transaction has entered an in doubt state or was remotely rolled
     * back this method will throw an error to indicate that the roll back failed and that a new transaction need
     * to be started by the user.
     *
     * @return this {@link Session} instance.
     *
     * @throws ClientException if an error occurs while attempting to roll back the current transaction.
     */
    Session rollbackTransaction() throws ClientException;

    /**
     * Waits indefinitely for a receiver created from this session to have a delivery ready for
     * receipt. The selection of the next receiver when more than one exits which has pending
     * deliveries is based upon the configured value of the {@link SessionOptions#defaultNextReceiverPolicy()}
     * used to create this session or if none was provided then the value is taken from the value
     * of the {@link ConnectionOptions#defaultNextReceiverPolicy()}.
     *
     * @return the next receiver that has a pending delivery available based on policy.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver nextReceiver() throws ClientException;

    /**
     * Waits indefinitely for a receiver created from this session to have a delivery ready for
     * receipt. The selection of the next receiver when more than one exits which has pending
     * deliveries is based upon the value of the {@link NextReceiverPolicy} that is provided by
     * the caller.
     *
     * @param policy
     *      The policy to apply when selecting the next receiver.
     *
     * @return the next receiver that has a pending delivery available based on policy.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver nextReceiver(NextReceiverPolicy policy) throws ClientException;

    /**
     * Waits for the given duration for a receiver created from this session to have a delivery ready
     * for receipt. The selection of the next receiver when more than one exits which has pending
     * deliveries is based upon the configured value of the {@link SessionOptions#defaultNextReceiverPolicy()}
     * used to create this session or if none was provided then the value is taken from the value
     * of the {@link ConnectionOptions#defaultNextReceiverPolicy()}. If no receiver has an available
     * delivery within the given timeout this method returns null.
     *
     * @param timeout
     *      The timeout value used to control how long the method waits for a new {@link Delivery} to be available.
     * @param unit
     *      The unit of time that the given timeout represents.
     *
     * @return the next receiver that has a pending delivery available based on policy.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver nextReceiver(long timeout, TimeUnit unit) throws ClientException;

    /**
     * Waits for the given duration for a receiver created from this session to have a delivery ready
     * for receipt. The selection of the next receiver when more than one exits which has pending
     * deliveries is based upon the value of the {@link NextReceiverPolicy} provided by the caller. If
     * no receiver has an available delivery within the given timeout this method returns null.
     *
     * @param policy
     *      The policy to apply when selecting the next receiver.
     * @param timeout
     *      The timeout value used to control how long the method waits for a new {@link Delivery} to be available.
     * @param unit
     *      The unit of time that the given timeout represents.
     *
     * @return the next receiver that has a pending delivery available based on policy.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver nextReceiver(NextReceiverPolicy policy, long timeout, TimeUnit unit) throws ClientException;

}
