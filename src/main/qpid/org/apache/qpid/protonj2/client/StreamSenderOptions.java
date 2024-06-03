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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.apache.qpid.protonj2.engine.DeliveryTagGenerator;

/**
 * Options class that controls various aspects of a {@link StreamSenderMessage} instance and how
 * a streamed message transfer is written.
 */
public class StreamSenderOptions extends LinkOptions<StreamSenderOptions> implements Cloneable {

    /**
     * Defines the default pending write buffering size which is used to control how much outgoing
     * data can be buffered for local writing before the sender has back pressured applied to avoid
     * out of memory conditions due to overly large pending batched writes.
     */
    public static final int DEFAULT_PENDING_WRITES_BUFFER_SIZE = SessionOptions.DEFAULT_SESSION_OUTGOING_CAPACITY;

    private long sendTimeout = ConnectionOptions.DEFAULT_SEND_TIMEOUT;

    private int pendingWritesBufferSize = DEFAULT_PENDING_WRITES_BUFFER_SIZE;

    private Supplier<DeliveryTagGenerator> tagGeneratorSupplier;

    /**
     * Defines the default minimum size that the context write buffer will allocate
     * which drives the interval auto flushing of written data for this context.
     */
    public static final int MIN_BUFFER_SIZE_LIMIT = 256;

    private int writeBufferSize;

    /**
     * Creates a {@link StreamSenderOptions} instance with default values for all options
     */
    public StreamSenderOptions() {
    }

    @Override
    public StreamSenderOptions clone() {
        return copyInto(new StreamSenderOptions());
    }

    /**
     * Create a {@link StreamSenderOptions} instance that copies all configuration from the given
     * {@link StreamSenderOptions} instance.
     *
     * @param options
     *      The options instance to copy all configuration values from.
     */
    public StreamSenderOptions(StreamSenderOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Copy all options from this {@link StreamSenderOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return the {@link StreamSenderOptions} instance that was given.
     */
    protected StreamSenderOptions copyInto(StreamSenderOptions other) {
        super.copyInto(other);

        other.writeBufferSize(writeBufferSize);
        other.sendTimeout(sendTimeout);
        other.pendingWritesBufferSize(pendingWritesBufferSize);

        return other;
    }

    /**
     * @return the configured context write buffering limit for the associated {@link StreamSender}
     */
    public int writeBufferSize() {
        return writeBufferSize;
    }

    /**
     * Sets the overall number of bytes the stream sender will buffer before automatically flushing the
     * currently buffered bytes.  By default the stream sender implementation chooses a value for this
     * buffer limit based on the configured frame size limits of the connection.
     *
     * @param writeBufferSize
     *       The number of bytes that can be written before the context performs a flush operation.
     *
     * @return this {@link StreamSenderOptions} instance.
     */
    public StreamSenderOptions writeBufferSize(int writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    /**
     * @return the configured pending write buffering limit for the associated {@link StreamSender}
     */
    public int pendingWritesBufferSize() {
        return this.pendingWritesBufferSize;
    }

    /**
     * Sets the overall number of bytes the stream sender will allow to be pending for write before applying
     * back pressure to the stream write caller.  By default the stream sender implementation chooses a value
     * for this pending write limit based on the configured frame size limits of the connection.  This is an
     * advanced option and should not be used unless the impact of doing so is understood by the user.
     *
     * @param pendingWritesBufferSize
     *       The number of bytes that can be pending for write before the sender applies back pressure.
     *
     * @return this {@link StreamSenderOptions} instance.
     */
    public StreamSenderOptions pendingWritesBufferSize(int pendingWritesBufferSize) {
        this.pendingWritesBufferSize = pendingWritesBufferSize;
        return this;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a resource is message send.
     */
    public long sendTimeout() {
        return sendTimeout;
    }

    /**
     * Configures the timeout used when awaiting a send operation to complete.  A send will block if the
     * remote has not granted the {@link Sender} or the {@link Session} credit to do so, if the send blocks
     * for longer than this timeout the send call will fail with an {@link ClientSendTimedOutException}
     * exception to indicate that the send did not complete.
     *
     * @param sendTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link StreamSenderOptions} instance.
     */
    public StreamSenderOptions sendTimeout(long sendTimeout) {
        return sendTimeout(sendTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a send operation to complete.  A send will block if the
     * remote has not granted the {@link StreamSender} or the {@link Session} credit to do so, if the send blocks
     * for longer than this timeout the send call will fail with an {@link ClientSendTimedOutException}
     * exception to indicate that the send did not complete.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link StreamSenderOptions} instance.
     */
    public StreamSenderOptions sendTimeout(long timeout, TimeUnit units) {
        this.sendTimeout = units.toMillis(timeout);
        return this;
    }

    /**
     * Configures a {@link Supplier} which provides unique instances of {@link DeliveryTagGenerator} objects
     * for any {@link StreamSender} created using these options.
     * <p>
     * The client sender will use a default {@link DeliveryTagGenerator} under normal circumstances and the
     * user is not required to configure a {@link Supplier}. In some cases where the user is communicating
     * with a system that requires a specific format of delivery tag this option allows use of a custom
     * generator. The caller is responsible for providing a supplier that will create a unique instance of
     * a tag generator as they are not meant to be shared amongst senders. Once a sender has been created
     * the tag generator it uses cannot be changed so future calls to this method will not affect previously
     * created {@link StreamSender} instances.
     *
     * @param supplier
     * 		The {@link Supplier} of {@link DeliveryTagGenerator} instances.
     *
     * @return the {@link StreamSenderOptions} instance that was given.
     */
    public StreamSenderOptions deliveryTagGeneratorSupplier(Supplier<DeliveryTagGenerator> supplier) {
        this.tagGeneratorSupplier = supplier;
        return this;
    }

    /**
     * @return the configured delivery tag {@link Supplier} or null if none was set.
     */
    public Supplier<DeliveryTagGenerator> deliveryTagGeneratorSupplier() {
        return tagGeneratorSupplier;
    }

    @Override
    protected StreamSenderOptions self() {
        return this;
    }
}
