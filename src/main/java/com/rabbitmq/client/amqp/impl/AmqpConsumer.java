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
package com.rabbitmq.client.amqp.impl;

import static com.rabbitmq.client.amqp.Resource.State.*;
import static java.time.Duration.ofSeconds;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private final String queue;
  private final Map<String, Object> filters;
  private final Map<String, Object> linkProperties;
  private final AmqpConnection connection;
  private final AtomicReference<PauseStatus> pauseStatus =
      new AtomicReference<>(PauseStatus.UNPAUSED);
  private final AtomicReference<CountDownLatch> echoedFlowAfterPauseLatch = new AtomicReference<>();
  private final MetricsCollector metricsCollector;
  private final SessionHandler sessionHandler;
  private final AtomicLong unsettledMessageCount = new AtomicLong(0);
  private final Runnable replenishCreditOperation = this::replenishCreditIfNeeded;
  // native receiver internal state, accessed only in the native executor/scheduler
  private ProtonReceiver protonReceiver;
  private volatile Scheduler protonExecutor;
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
    DefaultAddressBuilder<?> addressBuilder = Utils.addressBuilder();
    addressBuilder.queue(builder.queue());
    this.address = addressBuilder.address();
    this.queue = builder.queue();
    this.filters = Map.copyOf(builder.filters());
    this.linkProperties = Map.copyOf(builder.properties());
    this.connection = builder.connection();
    this.sessionHandler = this.connection.createSessionHandler();
    this.nativeReceiver =
        this.createNativeReceiver(
            this.sessionHandler.session(), this.address, this.linkProperties, this.filters);
    this.initStateFromNativeReceiver(this.nativeReceiver);
    this.metricsCollector = this.connection.metricsCollector();
    this.startReceivingLoop();
    this.state(OPEN);
    this.metricsCollector.openConsumer();
  }

  @Override
  public void pause() {
    if (this.pauseStatus.compareAndSet(PauseStatus.UNPAUSED, PauseStatus.PAUSING)) {
      try {
        CountDownLatch latch = new CountDownLatch(1);
        this.echoedFlowAfterPauseLatch.set(latch);
        this.protonExecutor.execute(this::doPause);
        try {
          boolean echoed = latch.await(10, TimeUnit.SECONDS);
          if (echoed) {
            this.pauseStatus.set(PauseStatus.PAUSED);
          } else {
            LOGGER.warn("Did not receive echoed flow to pause receiver");
            this.pauseStatus.set(PauseStatus.UNPAUSED);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      } catch (Exception e) {
        this.pauseStatus.set(PauseStatus.UNPAUSED);
      }
    }
  }

  @Override
  public void unpause() {
    checkOpen();
    if (this.pauseStatus.compareAndSet(PauseStatus.PAUSED, PauseStatus.UNPAUSED)) {
      try {
        this.nativeReceiver.addCredit(this.initialCredits);
      } catch (ClientException e) {
        throw ExceptionUtils.convert(e);
      }
    }
  }

  @Override
  public long unsettledMessageCount() {
    return unsettledMessageCount.get();
  }

  @Override
  public void close() {
    this.close(null);
  }

  // internal API

  private ClientReceiver createNativeReceiver(
      Session nativeSession,
      String address,
      Map<String, Object> properties,
      Map<String, Object> filters) {
    try {
      ReceiverOptions receiverOptions =
          new ReceiverOptions()
              .deliveryMode(DeliveryMode.AT_LEAST_ONCE)
              .autoAccept(false)
              .autoSettle(false)
              .creditWindow(0)
              .properties(properties);
      if (!filters.isEmpty()) {
        receiverOptions.sourceOptions().filters(filters);
      }
      return (ClientReceiver)
          ExceptionUtils.wrapGet(nativeSession.openReceiver(address, receiverOptions).openFuture());
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
            this.unsettledMessageCount.incrementAndGet();
            this.metricsCollector.consume();
            AmqpMessage message = new AmqpMessage(delivery.message());
            Consumer.Context context =
                new DeliveryContext(
                    delivery,
                    this.protonExecutor,
                    this.metricsCollector,
                    this.unsettledMessageCount,
                    this.replenishCreditOperation);
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
    this.receiveLoop = this.connection.environment().consumerExecutorService().submit(receiveTask);
  }

  void recoverAfterConnectionFailure() {
    this.nativeReceiver =
        RetryUtils.callAndMaybeRetry(
            () ->
                createNativeReceiver(
                    this.sessionHandler.sessionNoCheck(),
                    this.address,
                    this.linkProperties,
                    this.filters),
            e -> {
              boolean shouldRetry =
                  e instanceof AmqpException.AmqpResourceClosedException
                      && e.getMessage().contains("stream queue")
                      && e.getMessage()
                          .contains("does not have a running replica on the local node");
              LOGGER.debug("Retrying receiver creation on consumer recovery: {}", shouldRetry);
              return shouldRetry;
            },
            List.of(ofSeconds(1), ofSeconds(2), ofSeconds(3), BackOffDelayPolicy.TIMEOUT),
            "Create AMQP receiver to address '%s'",
            this.address);
    this.initStateFromNativeReceiver(this.nativeReceiver);
    this.pauseStatus.set(PauseStatus.UNPAUSED);
    this.unsettledMessageCount.set(0);
    startReceivingLoop();
  }

  private void close(Throwable cause) {
    if (this.closed.compareAndSet(false, true)) {
      this.state(CLOSING, cause);
      this.connection.removeConsumer(this);
      if (this.receiveLoop != null) {
        this.receiveLoop.cancel(true);
      }
      try {
        this.nativeReceiver.close();
        this.sessionHandler.close();
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

  String queue() {
    return this.queue;
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
        throw new AmqpException("Could not initialize consumer internal state");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void replenishCreditIfNeeded() {
    if (!this.pausedOrPausing() && this.state() == OPEN) {
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

  private void doPause() {
    this.creditState.updateCredit(0);
    this.creditState.updateEcho(true);
    this.sessionWindow.writeFlow(this.protonReceiver);
  }

  boolean pausedOrPausing() {
    return this.pauseStatus.get() != PauseStatus.UNPAUSED;
  }

  enum PauseStatus {
    UNPAUSED,
    PAUSING,
    PAUSED
  }

  private static class DeliveryContext implements Consumer.Context {

    private final AtomicBoolean settled = new AtomicBoolean(false);
    private final Delivery delivery;
    private final Scheduler protonExecutor;
    private final MetricsCollector metricsCollector;
    private final AtomicLong unsettledMessageCount;
    private final Runnable replenishCreditOperation;

    private DeliveryContext(
        Delivery delivery,
        Scheduler protonExecutor,
        MetricsCollector metricsCollector,
        AtomicLong unsettledMessageCount,
        Runnable replenishCreditOperation) {
      this.delivery = delivery;
      this.protonExecutor = protonExecutor;
      this.metricsCollector = metricsCollector;
      this.unsettledMessageCount = unsettledMessageCount;
      this.replenishCreditOperation = replenishCreditOperation;
    }

    @Override
    public void accept() {
      if (settled.compareAndSet(false, true)) {
        try {
          protonExecutor.execute(replenishCreditOperation);
          delivery.disposition(DeliveryState.accepted(), true);
          unsettledMessageCount.decrementAndGet();
          metricsCollector.consumeDisposition(MetricsCollector.ConsumeDisposition.ACCEPTED);
        } catch (ClientIllegalStateException | RejectedExecutionException | ClientIOException e) {
          LOGGER.debug("message accept failed: {}", e.getMessage());
        } catch (ClientException e) {
          throw ExceptionUtils.convert(e);
        }
      }
    }

    @Override
    public void discard() {
      if (settled.compareAndSet(false, true)) {
        try {
          protonExecutor.execute(replenishCreditOperation);
          delivery.disposition(DeliveryState.rejected("", ""), true);
          unsettledMessageCount.decrementAndGet();
          metricsCollector.consumeDisposition(MetricsCollector.ConsumeDisposition.DISCARDED);
        } catch (ClientIllegalStateException | RejectedExecutionException | ClientIOException e) {
          LOGGER.debug("message discard failed: {}", e.getMessage());
        } catch (ClientException e) {
          throw ExceptionUtils.convert(e);
        }
      }
    }

    @Override
    public void discard(Map<String, Object> annotations) {
      if (settled.compareAndSet(false, true)) {
        try {
          annotations = annotations == null ? Collections.emptyMap() : annotations;
          Utils.checkMessageAnnotations(annotations);
          protonExecutor.execute(replenishCreditOperation);
          delivery.disposition(DeliveryState.modified(true, true, annotations), true);
          unsettledMessageCount.decrementAndGet();
          metricsCollector.consumeDisposition(MetricsCollector.ConsumeDisposition.DISCARDED);
        } catch (ClientIllegalStateException | RejectedExecutionException | ClientIOException e) {
          LOGGER.debug("message discard (modified) failed: {}", e.getMessage());
        } catch (ClientException e) {
          throw ExceptionUtils.convert(e);
        }
      }
    }

    @Override
    public void requeue() {
      if (settled.compareAndSet(false, true)) {
        try {
          protonExecutor.execute(replenishCreditOperation);
          delivery.disposition(DeliveryState.released(), true);
          unsettledMessageCount.decrementAndGet();
          metricsCollector.consumeDisposition(MetricsCollector.ConsumeDisposition.REQUEUED);
        } catch (ClientIllegalStateException | RejectedExecutionException | ClientIOException e) {
          LOGGER.debug("message requeue failed: {}", e.getMessage());
        } catch (ClientException e) {
          throw ExceptionUtils.convert(e);
        }
      }
    }

    @Override
    public void requeue(Map<String, Object> annotations) {
      if (settled.compareAndSet(false, true)) {
        try {
          annotations = annotations == null ? Collections.emptyMap() : annotations;
          Utils.checkMessageAnnotations(annotations);
          protonExecutor.execute(replenishCreditOperation);
          delivery.disposition(DeliveryState.modified(false, false, annotations), true);
          unsettledMessageCount.decrementAndGet();
          metricsCollector.consumeDisposition(MetricsCollector.ConsumeDisposition.REQUEUED);
        } catch (ClientIllegalStateException | RejectedExecutionException | ClientIOException e) {
          LOGGER.debug("message requeue (modified) failed: {}", e.getMessage());
        } catch (ClientException e) {
          throw ExceptionUtils.convert(e);
        }
      }
    }
  }
}
