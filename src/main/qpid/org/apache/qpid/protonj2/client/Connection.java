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
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;

/**
 * Top level {@link Connection} object that can be used as a stand alone API for sending
 * messages and creating {@link Receiver} instances for message consumption. The Connection
 * API also exposes a {@link Session} based API for more advanced messaging use cases.
 *
 * When a Connection is closed all the resources created by the connection are implicitly closed.
 */
public interface Connection extends AutoCloseable {

    /**
     * @return the {@link Client} instance that holds this {@link Connection}
     */
    Client client();

    /**
     * When a {@link Connection} is created it may not be opened on the remote peer, the future returned
     * from this method allows the caller to await the completion of the Connection open by the remote before
     * proceeding on to other messaging operations.  If the open of the connection fails at the remote an
     * {@link Exception} is thrown from the {@link Future#get()} method when called.
     *
     * @return a {@link Future} that will be completed when the remote opens this {@link Connection}.
     */
    Future<Connection> openFuture();

    /**
     * Requests a close of the {@link Connection} at the remote and waits until the Connection has been
     * fully closed or until the configured {@link ConnectionOptions#closeTimeout()} is exceeded.
     */
    @Override
    void close();

    /**
     * Requests a close of the {@link Connection} at the remote and waits until the Connection has been
     * fully closed or until the configured {@link ConnectionOptions#closeTimeout()} is exceeded.
     *
     * @param error
     *      The {@link ErrorCondition} to transmit to the remote along with the close operation.
     */
    void close(ErrorCondition error);

    /**
     * Requests a close of the {@link Connection} at the remote and returns a {@link Future} that will be
     * completed once the Connection has been fully closed.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Connection}.
     */
    Future<Connection> closeAsync();

    /**
     * Requests a close of the {@link Connection} at the remote and returns a {@link Future} that will be
     * completed once the Connection has been fully closed.
     *
     * @param error
     * 		The {@link ErrorCondition} to transmit to the remote along with the close operation.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Connection}.
     */
    Future<Connection> closeAsync(ErrorCondition error);

    /**
     * Creates a receiver used to consumer messages from the given node address.  The returned receiver will
     * be configured using default options and will take its timeout configuration values from those specified
     * in the parent {@link Connection}.
     *
     * The returned receiver may not have been opened on the remote when it is returned.  Some methods of the
     * {@link Receiver} can block until the remote fully opens the receiver, the user can wait for the remote
     * to respond to the open request by calling the {@link Receiver#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param address
     *            The source address to attach the consumer to.
     *
     * @return the consumer.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openReceiver(String address) throws ClientException;

    /**
     * Creates a receiver used to consumer messages from the given node address.  The returned receiver
     * will be configured using the options provided in the given {@link ReceiverOptions} instance.
     *
     * The returned receiver may not have been opened on the remote when it is returned.  Some methods of the
     * {@link Receiver} can block until the remote fully opens the receiver, the user can wait for the remote
     * to respond to the open request by calling the {@link Receiver#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param address
     *            The source address to attach the consumer to.
     * @param receiverOptions
     *            The options for this receiver.
     *
     * @return the newly created {@link Receiver} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a receiver used to consume messages from the given node address and configure it
     * such that the remote create a durable node.  The returned receiver will be configured using
     * default options and will take its timeout configuration values from those specified in the
     * parent {@link Connection}.
     *
     * The returned receiver may not have been opened on the remote when it is returned.  Some methods of the
     * {@link Receiver} can block until the remote fully opens the receiver, the user can wait for the remote
     * to respond to the open request by calling the {@link Receiver#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param address
     * 			The source address to attach the consumer to.
     * @param subscriptionName
     * 			The name to give the subscription (link name).
     *
     * @return the newly created {@link Receiver} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDurableReceiver(String address, String subscriptionName) throws ClientException;

    /**
     * Creates a receiver used to consume messages from the given node address and configure it
     * such that the remote create a durable node.  The returned receiver will be configured using
     * provided options.
     *
     * The returned receiver may not have been opened on the remote when it is returned.  Some methods of the
     * {@link Receiver} can block until the remote fully opens the receiver, the user can wait for the remote
     * to respond to the open request by calling the {@link Receiver#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param address
     *            The source address to attach the consumer to.
     * @param subscriptionName
     * 			The name to give the subscription (link name).
     * @param receiverOptions
     *            The options for this receiver.
     *
     * @return the newly created {@link Receiver} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDurableReceiver(String address, String subscriptionName, ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from the given node address.  The returned receiver
     * will be configured using default options and will take its timeout configuration values from those
     * specified in the parent {@link Connection}.
     *
     * The returned receiver may not have been opened on the remote when it is returned.  Some methods of the
     * {@link Receiver} can block until the remote fully opens the receiver, the user can wait for the remote
     * to respond to the open request by calling the {@link Receiver#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @return the newly created {@link Receiver} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver() throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from a dynamically generated node on the remote..
     *
     * The returned receiver may not have been opened on the remote when it is returned.  Some methods of the
     * {@link Receiver} can block until the remote fully opens the receiver, the user can wait for the remote
     * to respond to the open request by calling the {@link Receiver#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param dynamicNodeProperties
     * 		The dynamic node properties to be applied to the node created by the remote.
     *
     * @return the newly created {@link Receiver} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties) throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from a dynamically generated node on the remote..
     *
     * The returned receiver may not have been opened on the remote when it is returned.  Some methods of the
     * {@link Receiver} can block until the remote fully opens the receiver, the user can wait for the remote
     * to respond to the open request by calling the {@link Receiver#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param receiverOptions
     * 		The options for this receiver.
     *
     * @return the newly created {@link Receiver} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver(ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from the given node address.
     *
     * The returned receiver may not have been opened on the remote when it is returned.  Some methods of the
     * {@link Receiver} can block until the remote fully opens the receiver, the user can wait for the remote
     * to respond to the open request by calling the {@link Receiver#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param dynamicNodeProperties
     * 		The dynamic node properties to be applied to the node created by the remote.
     * @param receiverOptions
     *      The options for this receiver.
     *
     * @return the newly created {@link Receiver} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a streaming message receiver used to consume large messages from the given node address.  The
     * returned {@link StreamReceiver} will be configured using default options and will take its timeout
     * configuration values from those specified in the parent {@link Connection}.
     *
     * The returned stream receiver may not have been opened on the remote when it is returned.  Some methods of
     * the {@link StreamReceiver} can block until the remote fully opens the receiver link, the user can wait for
     * the remote to respond to the open request by calling the {@link StreamReceiver#openFuture()} method and using
     * the {@link Future#get()} methods to wait for completion.
     *
     * @param address
     *            The source address to attach the consumer to.
     *
     * @return the newly created {@link StreamReceiver} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    StreamReceiver openStreamReceiver(String address) throws ClientException;

    /**
     * Creates a streaming message receiver used to consume large messages from the given node address.  The
     * returned receiver will be configured using the options provided in the given {@link ReceiverOptions}
     * instance.
     *
     * The returned {@link StreamReceiver} may not have been opened on the remote when it is returned.  Some
     * methods of the {@link StreamReceiver} can block until the remote fully opens the receiver link, the user
     * can wait for the remote to respond to the open request by calling the {@link StreamReceiver#openFuture()}
     * method and using the {@link Future#get()} methods to wait for completion.
     *
     * @param address
     *            The source address to attach the consumer to.
     * @param receiverOptions
     *            The options for this receiver.
     *
     * @return the newly created {@link StreamReceiver} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    StreamReceiver openStreamReceiver(String address, StreamReceiverOptions receiverOptions) throws ClientException;

    /**
     * Returns the default anonymous sender used by this {@link Connection} for {@link #send(Message)}
     * calls.  If the sender has not been created yet this call will initiate its creation and open with
     * the remote peer.
     *
     * @return the sender.
     *
     * @throws ClientException if an internal error occurs opening the default sender.
     * @throws ClientUnsupportedOperationException if the remote did not signal support for anonymous relays.
     */
    Sender defaultSender() throws ClientException;

    /**
     * Creates a sender used to send messages to the given node address.  The returned sender will
     * be configured using default options and will take its timeout configuration values from those
     * specified in the parent {@link Connection}.
     *
     * The returned {@link Sender} may not have been opened on the remote when it is returned.  Some methods
     * of the {@link Sender} can block until the remote fully opens the sender, the user can wait for the
     * remote to respond to the open request by calling the {@link Sender#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param address
     *            The target address to attach to, cannot be null.
     *
     * @return the sender.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openSender(String address) throws ClientException;

    /**
     * Creates a sender used to send messages to the given node address.
     *
     * The returned {@link Sender} may not have been opened on the remote when it is returned.  Some methods
     * of the {@link Sender} can block until the remote fully opens the sender, the user can wait for the
     * remote to respond to the open request by calling the {@link Sender#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param address
     *            The target address to attach to, cannot be null.
     * @param senderOptions
     *            The options for this sender.
     *
     * @return the sender.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openSender(String address, SenderOptions senderOptions) throws ClientException;

    /**
     * Creates a stream sender used to send large messages to the given node address.  The returned sender will
     * be configured using default options and will take its timeout configuration values from those
     * specified in the parent {@link Connection}.
     *
     * The returned {@link StreamSender} may not have been opened on the remote when it is returned.  Some methods
     * of the {@link StreamSender} can block until the remote fully opens the sender, the user can wait for the
     * remote to respond to the open request by calling the {@link StreamSender#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param address
     *            The target address to attach to, cannot be null.
     *
     * @return the stream sender.
     *
     * @throws ClientException if an internal error occurs.
     */
    StreamSender openStreamSender(String address) throws ClientException;

    /**
     * Creates a streaming sender used to send large messages to the given node address.
     * <p>
     * The returned {@link StreamSender} may not have been opened on the remote when it is returned.  Some methods
     * of the {@link StreamSender} can block until the remote fully opens the sender, the user can wait for the
     * remote to respond to the open request by calling the {@link StreamSender#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param address
     *            The target address to attach to, cannot be null.
     * @param senderOptions
     *            The options for this sender.
     *
     * @return the sender.
     *
     * @throws ClientException if an internal error occurs.
     */
    StreamSender openStreamSender(String address, StreamSenderOptions senderOptions) throws ClientException;

    /**
     * Creates a sender that is established to the 'anonymous relay' and as such each message
     * that is sent using this sender must specify an address in its destination address field.
     * The returned sender will be configured using default options and will take its timeout
     * configuration values from those specified in the parent {@link Connection}.
     *
     * The returned {@link Sender} may not have been opened on the remote when it is returned.  Some methods
     * of the {@link Sender} can block until the remote fully opens the sender, the user can wait for the
     * remote to respond to the open request by calling the {@link Sender#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @return the sender.
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
     * The returned {@link Sender} may not have been opened on the remote when it is returned.  Some methods
     * of the {@link Sender} can block until the remote fully opens the sender, the user can wait for the
     * remote to respond to the open request by calling the {@link Sender#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param senderOptions
     *            The options for this sender.
     *
     * @return the sender.
     *
     * @throws ClientException if an internal error occurs.
     * @throws ClientUnsupportedOperationException if the remote did not signal support for anonymous relays.
     */
    Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException;

    /**
     * Returns the default {@link Session} instance that is used by this Connection to
     * create the default anonymous connection {@link Sender} as well as creating those
     * resources created from the {@link Connection} such as {@link Sender} and {@link Receiver}
     * instances not married to a specific {@link Session}.
     * <p>
     * While it is possible to use the returned Session to cause Connection level resources
     * to operate within a transaction it is strongly discouraged. Transactions should be
     * performed from a user created Session with a single {@link Sender} or {@link Receiver}
     * link for best results.
     *
     * @return a new {@link Session} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Session defaultSession() throws ClientException;

    /**
     * Creates a new {@link Session} instance for use by the client application.  The returned session
     * will be configured using default options and will take its timeout configuration values from those
     * specified in the parent {@link Connection}.
     *
     * The returned {@link Session} may not have been opened on the remote when it is returned.  Some methods
     * of the {@link Session} can block until the remote fully opens the session, the user can wait for the
     * remote to respond to the open request by calling the {@link Session#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @return a new {@link Session} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Session openSession() throws ClientException;

    /**
     * Creates a new {@link Session} instance for use by the client application.
     *
     * The returned {@link Session} may not have been opened on the remote when it is returned.  Some methods
     * of the {@link Session} can block until the remote fully opens the session, the user can wait for the
     * remote to respond to the open request by calling the {@link Session#openFuture()} method and using the
     * {@link Future#get()} methods to wait for completion.
     *
     * @param options
     *      The {@link SessionOptions} that control properties of the created session.
     *
     * @return a new {@link Session} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Session openSession(SessionOptions options) throws ClientException;

    /**
     * Sends the given {@link Message} using the internal connection sender.
     * <p>
     * The connection {@link Sender} is an anonymous AMQP sender which requires that the
     * given message has a valid to value set.
     *
     * @param message
     * 		The message to send
     *
     * @return a {@link Tracker} that allows the client to track settlement of the message.
     *
     * @throws ClientException if an internal error occurs.
     */
    Tracker send(Message<?> message) throws ClientException;

    /**
     * Waits indefinitely for a receiver created from the connection default session to have a
     * delivery ready for receipt. The selection of the next receiver when more than one exists
     * which has pending deliveries is based upon the configured value of the
     * {@link ConnectionOptions#defaultNextReceiverPolicy()}.
     *
     * @return the next receiver that has a pending delivery available based on policy.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver nextReceiver() throws ClientException;

    /**
     * Waits indefinitely for a receiver created from the connection default session to have a
     * delivery ready for receipt. The selection of the next receiver when more than one exists
     * which has pending deliveries is based upon the value of the {@link NextReceiverPolicy}
     * that is provided by the caller.
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
     * Waits for the configured time interval for a receiver created from the connection default
     * session to have a delivery ready for receipt. The selection of the next receiver when more
     * than one exists which has pending deliveries is based upon the configured value of the
     * {@link ConnectionOptions#defaultNextReceiverPolicy()}. If no receiver has an incoming delivery
     * before the given timeout expires the method returns null.
     *
     * @param timeout
     *      The timeout value used to control how long the method waits for a new {@link Delivery} to be available.
     * @param unit
     *      The unit of time that the given timeout represents.
     *
     * @return the next receiver that has a pending delivery available based on policy or null if the timeout is reached.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver nextReceiver(long timeout, TimeUnit unit) throws ClientException;

    /**
     * Waits for the configured time interval for a receiver created from the connection default
     * session to have a delivery ready for receipt. The selection of the next receiver when more
     * than one exists which has pending deliveries is based upon the {@link NextReceiverPolicy}
     * provided by the caller. If no receiver has an incoming delivery before the given timeout
     * expires the method returns null.
     *
     * @param policy
     *      The policy to apply when selecting the next receiver.
     * @param timeout
     *      The timeout value used to control how long the method waits for a new {@link Delivery} to be available.
     * @param unit
     *      The unit of time that the given timeout represents.
     *
     * @return the next receiver that has a pending delivery available based on policy or null if the timeout is reached.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver nextReceiver(NextReceiverPolicy policy, long timeout, TimeUnit unit) throws ClientException;

    /**
     * Returns the properties that the remote provided upon successfully opening the {@link Connection}.  If the
     * open has not completed yet this method will block to await the open response which carries the remote
     * properties.  If the remote provides no properties this method will return null.
     *
     * @return any properties provided from the remote once the connection has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Connection} remote properties.
     */
    Map<String, Object> properties() throws ClientException;

    /**
     * Returns the offered capabilities that the remote provided upon successfully opening the {@link Connection}.
     * If the open has not completed yet this method will block to await the open response which carries the
     * remote offered capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any capabilities provided from the remote once the connection has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Connection} remote offered capabilities.
     */
    String[] offeredCapabilities() throws ClientException;

    /**
     * Returns the desired capabilities that the remote provided upon successfully opening the {@link Connection}.
     * If the open has not completed yet this method will block to await the open response which carries the
     * remote desired capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any desired capabilities provided from the remote once the connection has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Connection} remote desired capabilities.
     */
    String[] desiredCapabilities() throws ClientException;

}
