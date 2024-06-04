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
package com.rabbitmq.amqp.client.impl;

import static com.rabbitmq.amqp.client.Resource.State.*;

import com.rabbitmq.amqp.client.Consumer;
import com.rabbitmq.amqp.client.ModelException;
import com.rabbitmq.amqp.client.metrics.MetricsCollector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.*;
import org.apache.qpid.protonj2.client.impl.ClientReceiver;
import org.apache.qpid.protonj2.client.util.DeliveryQueue;
import org.apache.qpid.protonj2.engine.EventHandler;
import org.apache.qpid.protonj2.engine.Scheduler;
import org.apache.qpid.protonj2.engine.impl.ProtonLinkCreditState;
import org.apache.qpid.protonj2.engine.impl.ProtonReceiver;
import org.apache.qpid.protonj2.engine.impl.ProtonSessionIncomingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AmqpConsumer extends ResourceBase implements Consumer {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConsumer.class);

  private volatile ClientReceiver nativeReceiver;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile Future<?> receiveLoop;
  private final int initialCredits;
  private final MessageHandler messageHandler;
  private final Long id;
  private final String address;
  private final AmqpConnection connection;
  private final AtomicBoolean paused = new AtomicBoolean(false);
  private final AtomicReference<CountDownLatch> echoedFlowAfterPauseLatch = new AtomicReference<>();
  private final MetricsCollector metricsCollector;
  // native receiver internal state, accessed only in the native executor/scheduler
  private ProtonReceiver protonReceiver;
  private Scheduler protonExecutor;
  private DeliveryQueue protonDeliveryQueue;
  private ProtonSessionIncomingWindow sessionWindow;
  private ProtonLinkCreditState creditState;

  AmqpConsumer(AmqpConsumerBuilder builder) {
    super(builder.listeners());
    this.id = ID_SEQUENCE.getAndIncrement();
    this.initialCredits = builder.initialCredits();
    this.messageHandler =
        builder
            .connection()
            .observationCollector()
            .subscribe(builder.queue(), builder.messageHandler());
    this.address = "/queue/" + builder.queue();
    this.nativeReceiver = createNativeReceiver(builder.connection().nativeSession(), this.address);
    this.initStateFromNativeReceiver(this.nativeReceiver);
    this.connection = builder.connection();
    this.metricsCollector = this.connection.metricsCollector();
    this.startReceivingLoop();
    this.state(OPEN);
    this.metricsCollector.openConsumer();
  }

  @Override
  public void close() {
    this.close(null);
  }

  // internal API

  private ClientReceiver createNativeReceiver(Session nativeSession, String address) {
    try {
      return (ClientReceiver)
          nativeSession.openReceiver(
              address,
              new ReceiverOptions()
                  .deliveryMode(DeliveryMode.AT_LEAST_ONCE)
                  .autoAccept(false)
                  .autoSettle(false)
                  .creditWindow(0));
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e, "Error while creating receiver from '%s'", address);
    }
  }

  private Runnable createReceiveTask(Receiver receiver, MessageHandler messageHandler) {
    return () -> {
      try {
        receiver.addCredit(this.initialCredits);
        while (!Thread.currentThread().isInterrupted()) {
          Delivery delivery = receiver.receive(100, TimeUnit.MILLISECONDS);
          if (delivery != null) {
            this.metricsCollector.consume();
            AmqpMessage message = new AmqpMessage(delivery.message());
            AtomicBoolean disposed = new AtomicBoolean(false);
            Consumer.Context context =
                new Consumer.Context() {

                  @Override
                  public void accept() {
                    if (disposed.compareAndSet(false, true)) {
                      try {
                        protonExecutor.execute(() -> replenishCreditIfNeeded());
                        delivery.disposition(DeliveryState.accepted(), true);
                        metricsCollector.consumeDisposition(
                            MetricsCollector.ConsumeDisposition.ACCEPTED);
                      } catch (ClientIllegalStateException | ClientIOException e) {
                        LOGGER.debug("message accept failed: {}", e.getMessage());
                      } catch (ClientException e) {
                        throw ExceptionUtils.convert(e);
                      }
                    }
                  }

                  @Override
                  public void discard() {
                    if (disposed.compareAndSet(false, true)) {
                      try {
                        protonExecutor.execute(() -> replenishCreditIfNeeded());
                        delivery.disposition(DeliveryState.rejected("", ""), true);
                        metricsCollector.consumeDisposition(
                            MetricsCollector.ConsumeDisposition.DISCARDED);
                      } catch (ClientIllegalStateException | ClientIOException e) {
                        LOGGER.debug("message discard failed: {}", e.getMessage());
                      } catch (ClientException e) {
                        throw ExceptionUtils.convert(e);
                      }
                    }
                  }

                  @Override
                  public void requeue() {
                    if (disposed.compareAndSet(false, true)) {
                      try {
                        protonExecutor.execute(() -> replenishCreditIfNeeded());
                        delivery.disposition(DeliveryState.released(), true);
                        metricsCollector.consumeDisposition(
                            MetricsCollector.ConsumeDisposition.REQUEUED);
                      } catch (ClientIllegalStateException | ClientIOException e) {
                        LOGGER.debug("message requeue failed: {}", e.getMessage());
                      } catch (ClientException e) {
                        throw ExceptionUtils.convert(e);
                      }
                    }
                  }
                };
            messageHandler.handle(context, message);
          }
        }
      } catch (ClientLinkRemotelyClosedException e) {
        if (ExceptionUtils.notFound(e) || ExceptionUtils.resourceDeleted(e)) {
          this.close(ExceptionUtils.convert(e));
        }
      } catch (ClientConnectionRemotelyClosedException e) {
        // receiver is closed
      } catch (ClientException e) {
        java.util.function.Consumer<String> log =
            this.closed.get() ? m -> LOGGER.debug(m, e) : m -> LOGGER.warn(m, e);
        log.accept("Error while polling AMQP receiver");
      } catch (Exception e) {
        LOGGER.warn("Unexpected error in consumer loop", e);
      }
    };
  }

  private void startReceivingLoop() {
    Runnable receiveTask = createReceiveTask(nativeReceiver, messageHandler);
    this.receiveLoop = this.connection.executorService().submit(receiveTask);
  }

  void recoverAfterConnectionFailure() {
    this.nativeReceiver = createNativeReceiver(this.connection.nativeSession(false), this.address);
    this.initStateFromNativeReceiver(this.nativeReceiver);
    this.paused.set(false);
    startReceivingLoop();
  }

  private void close(Throwable cause) {
    if (this.closed.compareAndSet(false, true)) {
      this.state(CLOSING, cause);
      if (cause == null) {
        // normal closing, pausing message dispatching
        this.pause();
      }
      this.connection.removeConsumer(this);
      if (this.receiveLoop != null) {
        this.receiveLoop.cancel(true);
      }
      try {
        this.nativeReceiver.close();
      } catch (Exception e) {
        LOGGER.warn("Error while closing receiver", e);
      }
      this.state(CLOSED, cause);
      this.metricsCollector.closeConsumer();
    }
  }

  long id() {
    return this.id;
  }

  String address() {
    return this.address;
  }

  private void initStateFromNativeReceiver(ClientReceiver receiver) {
    try {
      Scheduler protonExecutor = receiver.executor();
      CountDownLatch fieldsSetLatch = new CountDownLatch(1);
      protonExecutor.execute(
          () -> {
            this.protonReceiver = (ProtonReceiver) receiver.protonReceiver();
            this.creditState = this.protonReceiver.getCreditState();
            this.sessionWindow = this.protonReceiver.sessionWindow();
            this.protonDeliveryQueue = receiver.deliveryQueue();

            EventHandler<org.apache.qpid.protonj2.engine.Receiver> eventHandler =
                this.protonReceiver.linkCreditUpdatedHandler();
            EventHandler<org.apache.qpid.protonj2.engine.Receiver> decorator =
                target -> {
                  eventHandler.handle(target);
                  CountDownLatch latch = this.echoedFlowAfterPauseLatch.getAndSet(null);
                  if (latch != null) {
                    latch.countDown();
                  }
                };
            this.protonReceiver.creditStateUpdateHandler(decorator);

            this.protonExecutor = protonExecutor;
            fieldsSetLatch.countDown();
          });
      if (!fieldsSetLatch.await(10, TimeUnit.SECONDS)) {
        throw new ModelException("Could not initialize consumer internal state");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void replenishCreditIfNeeded() {
    if (!this.paused()) {
      int creditWindow = this.initialCredits;
      int currentCredit = protonReceiver.getCredit();
      if (currentCredit <= creditWindow * 0.5) {
        int potentialPrefetch = currentCredit + this.protonDeliveryQueue.size();
        if (potentialPrefetch <= creditWindow * 0.7) {
          int additionalCredit = creditWindow - potentialPrefetch;
          try {
            protonReceiver.addCredit(additionalCredit);
          } catch (Exception ex) {
            LOGGER.debug("Error caught during credit top-up", ex);
          }
        }
      }
    }
  }

  void pause() {
    if (this.paused.compareAndSet(false, true)) {
      CountDownLatch latch = new CountDownLatch(1);
      this.echoedFlowAfterPauseLatch.set(latch);
      this.protonExecutor.execute(this::doPause);
      try {
        if (!latch.await(10, TimeUnit.SECONDS)) {
          LOGGER.warn("Did not receive echoed flow to pause receiver");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void doPause() {
    this.creditState.updateCredit(0);
    this.creditState.updateEcho(true);
    this.sessionWindow.writeFlow(this.protonReceiver);
  }

  void unpause() {
    checkOpen();
    if (this.paused.compareAndSet(true, false)) {
      try {
        this.nativeReceiver.addCredit(this.initialCredits);
      } catch (ClientException e) {
        throw ExceptionUtils.convert(e);
      }
    }
  }

  private boolean paused() {
    return this.paused.get();
  }
}
