// Copyright (c) 2024 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.
package com.rabbitmq.model.amqp;

import static com.rabbitmq.model.Resource.State.OPEN;

import com.rabbitmq.model.Message;
import com.rabbitmq.model.ModelException;
import com.rabbitmq.model.Publisher;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpPublisher extends ResourceBase implements Publisher {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpPublisher.class);

  private final Long id;
  private volatile Sender sender;
  private final ExecutorService executorService;
  private final String address;
  private final AmqpConnection connection;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  AmqpPublisher(AmqpPublisherBuilder builder) {
    super(builder.listeners());
    this.id = ID_SEQUENCE.getAndIncrement();
    this.executorService = builder.connection().executorService();
    this.address = builder.address();
    this.connection = builder.connection();
    this.sender = this.createSender(builder.connection().nativeSession(), this.address);
    this.state(OPEN);
  }

  @Override
  public Message message() {
    return new AmqpMessage();
  }

  @Override
  public void publish(Message message, Callback callback) {
    checkOpen();
    try {
      // TODO catch ClientSendTimedOutException
      org.apache.qpid.protonj2.client.Message<?> nativeMessage =
          ((AmqpMessage) message).nativeMessage();
      // TODO track confirmation task to cancel them during recovery
      Tracker tracker = this.sender.send(nativeMessage.durable(true));
      this.executorService.submit(
          () -> {
            Status status;
            try {
              tracker.settlementFuture().get();
              status =
                  tracker.remoteState() == DeliveryState.accepted()
                      ? Status.ACCEPTED
                      : Status.FAILED;
            } catch (InterruptedException | ExecutionException e) {
              status = Status.FAILED;
            }
            callback.handle(new DefaultContext(message, status));
          });
    } catch (ClientLinkRemotelyClosedException e) {
      if (ExceptionUtils.resourceDeleted(e)) {
        this.close();
      }
    } catch (ClientException e) {
      throw new ModelException(e);
    }
  }

  void recoverAfterConnectionFailure() {
    this.sender = this.createSender(this.connection.nativeSession(false), this.address);
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.state(State.CLOSING);
      this.connection.removePublisher(this);
      try {
        this.sender.close();
      } catch (Exception e) {
        LOGGER.warn("Error while closing sender", e);
      }
      this.state(State.CLOSED);
    }
  }

  // internal API

  private Sender createSender(Session session, String address) {
    SenderOptions senderOptions = new SenderOptions().deliveryMode(DeliveryMode.AT_LEAST_ONCE);
    try {
      if (address == null) {
        return session.openAnonymousSender(senderOptions);
      } else {
        return session.openSender(address, senderOptions);
      }
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e, "Error while creating publisher to {}", address);
    }
  }

  private static class DefaultContext implements Publisher.Context {

    private final Message message;
    private final Status status;

    private DefaultContext(Message message, Status status) {
      this.message = message;
      this.status = status;
    }

    @Override
    public Message message() {
      return this.message;
    }

    @Override
    public Status status() {
      return this.status;
    }
  }

  Long id() {
    return this.id;
  }

  String address() {
    return this.address;
  }
}
