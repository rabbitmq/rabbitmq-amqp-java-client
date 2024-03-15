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

import static com.rabbitmq.model.Resource.State.*;
import static com.rabbitmq.model.amqp.Utils.newThread;

import com.rabbitmq.model.Consumer;
import com.rabbitmq.model.ModelException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpConsumer extends ResourceBase implements Consumer {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConsumer.class);

  private volatile Receiver nativeReceiver;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile Thread receiveLoop;
  private final int initialCredits;
  private final MessageHandler messageHandler;
  private final Long id;
  private final String address;
  private final AmqpConnection connection;

  AmqpConsumer(AmqpConsumerBuilder builder) {
    super(builder.listeners());
    this.id = ID_SEQUENCE.getAndIncrement();
    this.initialCredits = builder.initialCredits();
    this.messageHandler = builder.messageHandler();
    this.address = builder.address();
    this.nativeReceiver =
        createNativeReceiver(builder.connection(), this.address, this.initialCredits);
    this.connection = builder.connection();
    this.startReceivingLoop();
    this.state(OPEN);
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.state(CLOSING);
      this.connection.removeConsumer(this);
      if (this.receiveLoop != null) {
        this.receiveLoop.interrupt();
      }
      try {
        this.nativeReceiver.close();
      } catch (Exception e) {
        LOGGER.warn("Error while closing receiver", e);
      }
      this.state(CLOSED);
    }
  }

  // internal API

  private Receiver createNativeReceiver(
      AmqpConnection connection, String address, int initialCredits) {
    try {
      return connection
          .nativeSession()
          .openReceiver(
              address,
              new ReceiverOptions()
                  .deliveryMode(DeliveryMode.AT_LEAST_ONCE)
                  .autoAccept(false)
                  .autoSettle(false)
                  .creditWindow(initialCredits));
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e, "Error while creating receiver from '%s'", address);
    }
  }

  private Runnable createReceiveTask(
      Receiver receiver, MessageHandler messageHandler, Semaphore inFlightMessages) {
    return () -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Delivery delivery = receiver.receive(100, TimeUnit.MILLISECONDS);
          if (delivery != null) {
            inFlightMessages.acquire();
            AmqpMessage message = new AmqpMessage(delivery.message());
            Consumer.Context context =
                new Consumer.Context() {

                  @Override
                  public void accept() {
                    inFlightMessages.release();
                    try {
                      delivery.disposition(DeliveryState.accepted(), true);
                    } catch (ClientException e) {
                      throw new ModelException(e);
                    }
                  }

                  @Override
                  public void discard() {
                    inFlightMessages.release();
                    try {
                      // TODO propagate condition and description for "rejected" delivery
                      // state
                      delivery.disposition(DeliveryState.rejected("", ""), true);
                    } catch (ClientException e) {
                      throw new ModelException(e);
                    }
                  }

                  @Override
                  public void requeue() {
                    inFlightMessages.release();
                    try {
                      delivery.disposition(DeliveryState.released(), true);
                    } catch (ClientException e) {
                      throw new ModelException(e);
                    }
                  }
                };
            messageHandler.handle(context, message);
          }
        }

      } catch (ClientConnectionRemotelyClosedException | ClientLinkRemotelyClosedException e) {
        // receiver is closed
      } catch (ClientException e) {
        java.util.function.Consumer<String> log =
            this.closed.get() ? m -> LOGGER.debug(m, e) : m -> LOGGER.warn(m, e);
        log.accept("Error while polling AMQP receiver");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
  }

  private void startReceivingLoop() {
    Semaphore inFlightMessages = new Semaphore(initialCredits);
    Runnable receiveTask = createReceiveTask(nativeReceiver, messageHandler, inFlightMessages);
    this.receiveLoop = newThread("rabbitmq-amqp-consumer-" + this.id, receiveTask);
    this.receiveLoop.start();
  }

  void recoverAfterConnectionFailure() {
    this.nativeReceiver = createNativeReceiver(this.connection, this.address, this.initialCredits);
    startReceivingLoop();
  }
}
