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

import static com.rabbitmq.client.amqp.Resource.State.CLOSED;
import static com.rabbitmq.client.amqp.Resource.State.CLOSING;
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.impl.AmqpConsumerBuilder.NO_OP_SUBSCRIPTION_LISTENER;
import static com.rabbitmq.client.amqp.metrics.MetricsCollector.ConsumeDisposition.ACCEPTED;
import static com.rabbitmq.client.amqp.metrics.MetricsCollector.ConsumeDisposition.DISCARDED;
import static com.rabbitmq.client.amqp.metrics.MetricsCollector.ConsumeDisposition.REQUEUED;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.ofNullable;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.ConsumerBuilder.StreamOptions;
import com.rabbitmq.client.amqp.ConsumerBuilder.SubscriptionListener;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryMode;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.DurabilityMode;
import org.apache.qpid.protonj2.client.ExpiryPolicy;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.impl.ClientConversionSupport;
import org.apache.qpid.protonj2.client.impl.ClientReceiver;
import org.apache.qpid.protonj2.client.util.DeliveryQueue;
import org.apache.qpid.protonj2.engine.EventHandler;
import org.apache.qpid.protonj2.engine.Scheduler;
import org.apache.qpid.protonj2.engine.impl.ProtonLinkCreditState;
import org.apache.qpid.protonj2.engine.impl.ProtonReceiver;
import org.apache.qpid.protonj2.engine.impl.ProtonSessionIncomingWindow;
import org.apache.qpid.protonj2.types.DescribedType;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AmqpConsumer extends ResourceBase implements Consumer {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);
  private static final Consumer.Context PRE_SETTLED_CONTEXT = new PreSettledContext();

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConsumer.class);

  private volatile ClientReceiver nativeReceiver;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final int initialCredits;
  private final boolean preSettled;
  private final Long id;
  private final String address;
  private volatile String directReplyToAddress;
  private final String queue;
  private final Map<String, DescribedType> filters;
  private final Map<String, Object> linkProperties;
  private final ConsumerBuilder.SubscriptionListener subscriptionListener;
  private final AmqpConnection connection;
  private final AtomicReference<PauseStatus> pauseStatus =
      new AtomicReference<>(PauseStatus.UNPAUSED);
  private final AtomicReference<CountDownLatch> echoedFlowAfterPauseLatch = new AtomicReference<>();
  private final MetricsCollector metricsCollector;
  private final SessionHandler sessionHandler;
  private final AtomicLong unsettledMessageCount = new AtomicLong(0);
  private final Runnable replenishCreditOperation = this::replenishCreditIfNeeded;
  private final java.util.function.Consumer<Delivery> nativeHandler;
  private final java.util.function.Consumer<ClientException> nativeCloseHandler;
  private final ConsumerWorkService consumerWorkService;
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
    this.preSettled = builder.isPreSettled();
    MessageHandler messageHandler =
        builder
            .connection()
            .observationCollector()
            .subscribe(builder.queue(), builder.messageHandler());
    if (builder.directReplyTo()) {
      this.address = null;
      this.queue = null;
    } else {
      DefaultAddressBuilder<?> addressBuilder = Utils.addressBuilder();
      addressBuilder.queue(builder.queue());
      this.address = addressBuilder.address();
      this.queue = builder.queue();
    }
    this.filters = Map.copyOf(builder.filters());
    this.linkProperties = Map.copyOf(builder.properties());
    this.subscriptionListener =
        ofNullable(builder.subscriptionListener()).orElse(NO_OP_SUBSCRIPTION_LISTENER);
    this.connection = builder.connection();
    this.sessionHandler = this.connection.createSessionHandler();
    this.nativeHandler = createNativeHandler(messageHandler);
    this.nativeCloseHandler =
        e -> {
          this.connection
              .consumerWorkService()
              .dispatch(
                  () -> {
                    // get result to make spotbugs happy
                    boolean ignored = maybeCloseConsumerOnException(this, e);
                  });
        };
    this.consumerWorkService = connection.consumerWorkService();
    this.consumerWorkService.register(this);
    this.nativeReceiver =
        createNativeReceiver(
            this.sessionHandler.session(),
            this.address,
            this.preSettled,
            this.linkProperties,
            this.filters,
            this.subscriptionListener,
            this.nativeHandler,
            this.nativeCloseHandler);
    try {
      this.directReplyToAddress = nativeReceiver.address();
      this.initStateFromNativeReceiver(this.nativeReceiver);
      this.metricsCollector = this.connection.metricsCollector();
      this.state(OPEN);
      this.nativeReceiver.addCredit(this.initialCredits);
    } catch (ClientException e) {
      AmqpException ex = ExceptionUtils.convert(e);
      this.close(ex);
      throw ex;
    }
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

  private static ClientReceiver createNativeReceiver(
      Session nativeSession,
      String address,
      boolean preSettled,
      Map<String, Object> properties,
      Map<String, DescribedType> filters,
      SubscriptionListener subscriptionListener,
      java.util.function.Consumer<Delivery> nativeHandler,
      java.util.function.Consumer<ClientException> closeHandler) {
    try {
      filters = new LinkedHashMap<>(filters);
      StreamOptions streamOptions = AmqpConsumerBuilder.streamOptions(filters);
      subscriptionListener.preSubscribe(() -> streamOptions);
      boolean directReplyTo = address == null;
      ReceiverOptions receiverOptions = new ReceiverOptions();

      if (directReplyTo) {
        receiverOptions
            .deliveryMode(DeliveryMode.AT_MOST_ONCE)
            .autoAccept(true)
            .autoSettle(true)
            .sourceOptions()
            .capabilities("rabbitmq:volatile-queue")
            .expiryPolicy(ExpiryPolicy.LINK_CLOSE)
            .durabilityMode(DurabilityMode.NONE);
      } else {
        if (preSettled) {
          receiverOptions.deliveryMode(DeliveryMode.AT_MOST_ONCE).autoAccept(true).autoSettle(true);
        } else {
          receiverOptions
              .deliveryMode(DeliveryMode.AT_LEAST_ONCE)
              .autoAccept(false)
              .autoSettle(false);
        }
      }
      receiverOptions
          .handler(nativeHandler)
          .closeHandler(closeHandler)
          .creditWindow(0)
          .properties(properties);
      Map<String, Object> localSourceFilters = Collections.emptyMap();
      if (!filters.isEmpty()) {
        localSourceFilters = Map.copyOf(filters);
        receiverOptions.sourceOptions().filters(localSourceFilters);
      }
      ClientReceiver receiver;
      if (directReplyTo) {
        receiver =
            (ClientReceiver)
                ExceptionUtils.wrapGet(
                    nativeSession.openDynamicReceiver(receiverOptions).openFuture());
      } else {
        receiver =
            (ClientReceiver)
                ExceptionUtils.wrapGet(
                    nativeSession.openReceiver(address, receiverOptions).openFuture());
      }

      boolean filterOk = true;
      if (!filters.isEmpty()) {
        Map<String, String> remoteSourceFilters = receiver.source().filters();
        for (Map.Entry<String, Object> localEntry : localSourceFilters.entrySet()) {
          if (!remoteSourceFilters.containsKey(localEntry.getKey())) {
            LOGGER.warn(
                "Missing filter value in attach response: {} => {}",
                localEntry.getKey(),
                localEntry.getValue());
            filterOk = false;
          }
        }
      }
      if (!filterOk) {
        receiver.close();
        throw new AmqpException(
            "The sending endpoint filters do not match the receiving endpoint filters");
      }
      return receiver;
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e, "Error while creating receiver from '%s'", address);
    }
  }

  private java.util.function.Consumer<Delivery> createNativeHandler(MessageHandler handler) {
    java.util.function.Consumer<Delivery> runnable;
    if (this.preSettled) {
      runnable =
          delivery -> {
            try {
              delivery.settle();
            } catch (ClientException e) {
              LOGGER.warn("Error while settling message: {}", e.getMessage());
            }
            AmqpMessage message;
            try {
              message = new AmqpMessage(delivery.message());
            } catch (ClientException e) {
              LOGGER.warn("Error while decoding message: {}", e.getMessage());
              return;
            }
            metricsCollector.consumeDisposition(ACCEPTED);
            protonExecutor.execute(replenishCreditOperation);
            handler.handle(PRE_SETTLED_CONTEXT, message);
          };
    } else {
      runnable =
          delivery -> {
            AmqpMessage message;
            try {
              message = new AmqpMessage(delivery.message());
            } catch (ClientException e) {
              LOGGER.warn("Error while decoding message: {}", e.getMessage());
              try {
                delivery.disposition(DeliveryState.rejected("", ""), true);
              } catch (ClientException ex) {
                LOGGER.warn("Error while rejecting non-decoded message: {}", ex.getMessage());
              }
              return;
            }
            Consumer.Context context =
                new DeliveryContext(
                    delivery,
                    this.protonExecutor,
                    this.protonReceiver,
                    this.metricsCollector,
                    this.unsettledMessageCount,
                    this.replenishCreditOperation,
                    this);
            handler.handle(context, message);
          };
    }
    return delivery -> {
      if (this.state() == OPEN) {
        this.unsettledMessageCount.incrementAndGet();
        this.metricsCollector.consume();
        this.consumerWorkService.dispatch(this, () -> runnable.accept(delivery));
      }
    };
  }

  void recoverAfterConnectionFailure() {
    this.nativeReceiver =
        RetryUtils.callAndMaybeRetry(
            () ->
                createNativeReceiver(
                    this.sessionHandler.sessionNoCheck(),
                    this.address,
                    this.preSettled,
                    this.linkProperties,
                    this.filters,
                    this.subscriptionListener,
                    this.nativeHandler,
                    this.nativeCloseHandler),
            e -> {
              boolean shouldRetry = ExceptionUtils.noRunningStreamMemberOnNode(e);
              LOGGER.debug("Retrying receiver creation on consumer recovery: {}", shouldRetry);
              return shouldRetry;
            },
            List.of(ofSeconds(1), ofSeconds(2), ofSeconds(3), BackOffDelayPolicy.TIMEOUT),
            "Create AMQP receiver to address '%s'",
            this.address);
    try {
      this.directReplyToAddress = this.nativeReceiver.address();
      this.initStateFromNativeReceiver(this.nativeReceiver);
      this.pauseStatus.set(PauseStatus.UNPAUSED);
      this.unsettledMessageCount.set(0);
      this.nativeReceiver.addCredit(this.initialCredits);
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e);
    }
  }

  void close(Throwable cause) {
    if (this.closed.compareAndSet(false, true)) {
      this.state(CLOSING, cause);
      if (this.consumerWorkService != null) {
        this.consumerWorkService.unregister(this);
      }
      this.connection.removeConsumer(this);
      try {
        if (this.nativeReceiver != null) {
          this.nativeReceiver.close();
        }
        this.sessionHandler.close();
      } catch (Exception e) {
        LOGGER.warn("Error while closing receiver", e);
      }
      this.state(CLOSED, cause);
      MetricsCollector mc = this.metricsCollector;
      if (mc != null) {
        mc.closeConsumer();
      }
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

    private static final DeliveryState REJECTED = DeliveryState.rejected(null, null);
    private final AtomicBoolean settled = new AtomicBoolean(false);
    private final Delivery delivery;
    private final Scheduler protonExecutor;
    private final ProtonReceiver protonReceiver;
    private final MetricsCollector metricsCollector;
    private final AtomicLong unsettledMessageCount;
    private final Runnable replenishCreditOperation;
    private final AmqpConsumer consumer;

    private DeliveryContext(
        Delivery delivery,
        Scheduler protonExecutor,
        ProtonReceiver protonReceiver,
        MetricsCollector metricsCollector,
        AtomicLong unsettledMessageCount,
        Runnable replenishCreditOperation,
        AmqpConsumer consumer) {
      this.delivery = delivery;
      this.protonExecutor = protonExecutor;
      this.protonReceiver = protonReceiver;
      this.metricsCollector = metricsCollector;
      this.unsettledMessageCount = unsettledMessageCount;
      this.replenishCreditOperation = replenishCreditOperation;
      this.consumer = consumer;
    }

    @Override
    public void accept() {
      this.settle(DeliveryState.accepted(), ACCEPTED, "accept");
    }

    @Override
    public void discard() {
      this.settle(REJECTED, DISCARDED, "discard");
    }

    @Override
    public void discard(Map<String, Object> annotations) {
      annotations = annotations == null ? Collections.emptyMap() : annotations;
      Utils.checkMessageAnnotations(annotations);
      this.settle(DeliveryState.modified(true, true, annotations), DISCARDED, "discard (modified)");
    }

    @Override
    public void requeue() {
      this.settle(DeliveryState.released(), REQUEUED, "requeue");
    }

    @Override
    public void requeue(Map<String, Object> annotations) {
      annotations = annotations == null ? Collections.emptyMap() : annotations;
      Utils.checkMessageAnnotations(annotations);
      this.settle(
          DeliveryState.modified(false, false, annotations), REQUEUED, "requeue (modified)");
    }

    @Override
    public BatchContext batch(int batchSizeHint) {
      return new BatchDeliveryContext(
          batchSizeHint,
          protonExecutor,
          protonReceiver,
          metricsCollector,
          unsettledMessageCount,
          replenishCreditOperation,
          consumer);
    }

    private void settle(
        DeliveryState state, MetricsCollector.ConsumeDisposition disposition, String label) {
      if (settled.compareAndSet(false, true)) {
        try {
          protonExecutor.execute(replenishCreditOperation);
          delivery.disposition(state, true);
          unsettledMessageCount.decrementAndGet();
          metricsCollector.consumeDisposition(disposition);
        } catch (Exception e) {
          handleContextException(this.consumer, e, label);
        }
      }
    }
  }

  String directReplyToAddress() {
    return this.directReplyToAddress;
  }

  @Override
  public String toString() {
    return "AmqpConsumer{" + "id=" + id + ", queue='" + queue + '\'' + '}';
  }

  private static final class BatchDeliveryContext implements BatchContext {

    private static final org.apache.qpid.protonj2.types.transport.DeliveryState REJECTED =
        new Rejected();
    private final List<DeliveryContext> contexts;
    private final AtomicBoolean settled = new AtomicBoolean(false);
    private final Scheduler protonExecutor;
    private final ProtonReceiver protonReceiver;
    private final MetricsCollector metricsCollector;
    private final AtomicLong unsettledMessageCount;
    private final Runnable replenishCreditOperation;
    private final AmqpConsumer consumer;

    private BatchDeliveryContext(
        int batchSizeHint,
        Scheduler protonExecutor,
        ProtonReceiver protonReceiver,
        MetricsCollector metricsCollector,
        AtomicLong unsettledMessageCount,
        Runnable replenishCreditOperation,
        AmqpConsumer consumer) {
      this.contexts = new ArrayList<>(batchSizeHint);
      this.protonExecutor = protonExecutor;
      this.protonReceiver = protonReceiver;
      this.metricsCollector = metricsCollector;
      this.unsettledMessageCount = unsettledMessageCount;
      this.replenishCreditOperation = replenishCreditOperation;
      this.consumer = consumer;
    }

    @Override
    public void add(Consumer.Context context) {
      if (this.settled.get()) {
        throw new IllegalStateException("Batch is closed");
      } else {
        if (context instanceof DeliveryContext) {
          DeliveryContext dctx = (DeliveryContext) context;
          // marking the context as settled avoids operation on it and deduplicates as well
          if (dctx.settled.compareAndSet(false, true)) {
            this.contexts.add(dctx);
          } else {
            throw new IllegalStateException("Message already settled");
          }
        } else {
          throw new IllegalArgumentException("Context type not supported: " + context);
        }
      }
    }

    @Override
    public int size() {
      return this.contexts.size();
    }

    @Override
    public void accept() {
      this.settle(Accepted.getInstance(), ACCEPTED, "accept");
    }

    @Override
    public void discard() {
      this.settle(REJECTED, DISCARDED, "discard");
    }

    @Override
    public void discard(Map<String, Object> annotations) {
      annotations = annotations == null ? Collections.emptyMap() : annotations;
      Utils.checkMessageAnnotations(annotations);
      Modified state =
          new Modified(false, true, ClientConversionSupport.toSymbolKeyedMap(annotations));
      this.settle(state, DISCARDED, "discard (modified)");
    }

    @Override
    public void requeue() {
      this.settle(Released.getInstance(), REQUEUED, "requeue");
    }

    @Override
    public void requeue(Map<String, Object> annotations) {
      annotations = annotations == null ? Collections.emptyMap() : annotations;
      Utils.checkMessageAnnotations(annotations);
      Modified state =
          new Modified(false, false, ClientConversionSupport.toSymbolKeyedMap(annotations));
      this.settle(state, REQUEUED, "requeue (modified)");
    }

    @Override
    public BatchContext batch(int batchSizeHint) {
      return this;
    }

    private void settle(
        org.apache.qpid.protonj2.types.transport.DeliveryState state,
        MetricsCollector.ConsumeDisposition disposition,
        String label) {
      if (settled.compareAndSet(false, true)) {
        int batchSize = this.contexts.size();
        try {
          protonExecutor.execute(replenishCreditOperation);
          long[][] ranges =
              SerialNumberUtils.ranges(this.contexts, ctx -> ctx.delivery.getDeliveryId());
          this.protonExecutor.execute(
              () -> {
                for (long[] range : ranges) {
                  this.protonReceiver.disposition(state, range);
                }
              });
          unsettledMessageCount.addAndGet(-batchSize);
          IntStream.range(0, batchSize)
              .forEach(
                  ignored -> {
                    metricsCollector.consumeDisposition(disposition);
                  });
        } catch (Exception e) {
          handleContextException(this.consumer, e, label);
        }
      }
    }
  }

  private static class PreSettledContext implements Consumer.Context {

    @Override
    public void accept() {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public void discard() {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public void discard(Map<String, Object> annotations) {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public void requeue() {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public void requeue(Map<String, Object> annotations) {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public BatchContext batch(int batchSizeHint) {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }
  }

  private static void handleContextException(
      AmqpConsumer consumer, Exception ex, String operation) {
    if (maybeCloseConsumerOnException(consumer, ex)) {
      return;
    }
    if (ex instanceof ClientIllegalStateException
        || ex instanceof RejectedExecutionException
        || ex instanceof ClientIOException) {
      LOGGER.debug("message {} failed: {}", operation, ex.getMessage());
    } else if (ex instanceof ClientException) {
      throw ExceptionUtils.convert((ClientException) ex);
    }
  }

  private static boolean maybeCloseConsumerOnException(AmqpConsumer consumer, Exception ex) {
    return ExceptionUtils.maybeCloseOnException(consumer::close, ex);
  }
}
