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

import com.rabbitmq.model.Consumer;
import com.rabbitmq.model.ModelException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpConsumer implements Consumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConsumer.class);

  private final AmqpEnvironment environment;
  private final Receiver receiver;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  AmqpConsumer(
      AmqpEnvironment environment,
      String address,
      MessageHandler messageHandler,
      int initialCredits) {
    this.environment = environment;
    try {
      this.receiver =
          this.environment
              .connection()
              .openReceiver(
                  address,
                  new ReceiverOptions()
                      .deliveryMode(DeliveryMode.AT_LEAST_ONCE)
                      .autoAccept(false)
                      .autoSettle(false)
                      .creditWindow(initialCredits));
      Semaphore inFlightMessages = new Semaphore(initialCredits);
      executorService.submit(
          () -> {
            try {
              while (!Thread.currentThread().isInterrupted()) {
                Delivery delivery = receiver.receive(100, TimeUnit.MILLISECONDS);
                inFlightMessages.acquire();
                if (delivery != null) {
                  AmqpMessage message = new AmqpMessage(delivery.message());
                  Context context =
                      new Context() {

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

            } catch (ClientConnectionRemotelyClosedException
                | ClientLinkRemotelyClosedException e) {
              // receiver is closed
            } catch (ClientException e) {
              LOGGER.warn("Error while polling AMQP receiver", e);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
    } catch (ClientException e) {
      executorService.shutdownNow();
      throw new ModelException(e);
    }
  }

  @Override
  public void close() {
    executorService.shutdownNow();
    this.receiver.close();
  }
}
