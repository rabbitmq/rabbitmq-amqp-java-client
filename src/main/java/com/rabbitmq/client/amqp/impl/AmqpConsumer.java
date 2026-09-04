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
import static com.rabbitmq.client.amqp.impl.Assert.notNull;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
import org.apache.qpid.protonj2.engine.EventHandler;
import org.apache.qpid.protonj2.engine.Scheduler;
import org.apache.qpid.protonj2.engine.impl.ProtonLinkCreditState;
import org.apache.qpid.protonj2.engine.impl.ProtonReceiver;
import org.apache.qpid.protonj2.engine.impl.ProtonSessionIncomingWindow;
import org.apache.qpid.protonj2.types.DescribedType;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The credit accounting and disposition path relies on five invariants:
 *
 * <ul>
 *   <li><b>I1</b> — credit is granted only after the {@link Link} is installed. Enforced by {@code
 *       creditWindow(0)}: a freshly opened receiver has zero credit until we grant it, so no
 *       delivery can arrive before its generation exists.
 *   <li><b>I2</b> — {@code Link.pendingWorkItems} is read and written only on that {@link Link}'s
 *       proton executor: a plain {@code int}, no atomics.
 *   <li><b>I3</b> — {@code pendingWorkItems} is incremented exactly once per delivery (at dispatch,
 *       already on the proton thread) and decremented exactly once (at work-item completion). One
 *       event each.
 *   <li><b>I4</b> — replenish is triggered from work-item completion and from every settle. It is
 *       conservative and idempotent (only ever tops up to the window), so an extra trigger is
 *       harmless and only a violation of I3 can stall the consumer.
 *   <li><b>I5</b> — a disposition is queued on the proton executor before the replenish that
 *       follows it.
 * </ul>
 */
final class AmqpConsumer extends ResourceBase implements Consumer {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);
  private static final Consumer.Context PRE_SETTLED_CONTEXT = new PreSettledContext();
  private static final String DEFERRAL_TOKENS_CAPABILITY = "rabbitmq:deferral-tokens";
  private static final Symbol DEFERRAL_TOKENS = Symbol.valueOf(DEFERRAL_TOKENS_CAPABILITY);
  private static final int MAX_TOKENS_PER_FLOW = 256; // ?MAX_DEFERRAL_TOKENS on the broker side

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConsumer.class);

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
  private final MessageHandler messageHandler;
  private final java.util.function.Consumer<ClientException> nativeCloseHandler;
  private final ConsumerWorkService consumerWorkService;
  // package-private for testing
  Duration pauseHandshakeTimeout = Duration.ofSeconds(10);
  // package-private for testing: runs after the native receiver has been reopened during
  // recovery, immediately before the new generation is activated
  volatile Runnable beforeActivateHook = () -> {};

  // Arbitrates the two writers of the consumer lifecycle: close() and the tail of
  // recoverAfterConnectionFailure. Held only across field writes, never across I/O.
  private final Object lifecycleLock = new Object();

  // One instance per receiver generation. Everything the credit path touches lives here, so a
  // generation's state cannot outlive the executor that owns it.
  private volatile Link link;

  AmqpConsumer(AmqpConsumerBuilder builder) {
    super(builder.listeners(), builder.connection().environment().executorService());
    this.id = ID_SEQUENCE.getAndIncrement();
    this.initialCredits = builder.initialCredits();
    this.preSettled = builder.isPreSettled() || builder.directReplyTo();
    this.messageHandler =
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
    this.consumerWorkService.register(this, this.initialCredits);
    // set before opening the link
    // (assigning it up front removes the need to reason about it)
    this.metricsCollector = this.connection.metricsCollector();
    try {
      Link openedLink = this.openLink(this.sessionHandler.session());
      this.directReplyToAddress = openedLink.receiver.address();
      if (this.activate(openedLink)) {
        openedLink.receiver.addCredit(this.initialCredits);
      }
    } catch (ClientException e) {
      AmqpException ex = ExceptionUtils.convert(e);
      this.close(ex);
      throw ex;
    }
    this.metricsCollector.openConsumer();
  }

  @Override
  public void pause() {
    checkOpen();
    if (this.pauseStatus.compareAndSet(PauseStatus.UNPAUSED, PauseStatus.PAUSING)) {
      try {
        CountDownLatch latch = new CountDownLatch(1);
        this.echoedFlowAfterPauseLatch.set(latch);
        Link linkAtPause = this.link;
        onExecutor(linkAtPause, () -> doPause(linkAtPause));
        try {
          boolean echoed =
              latch.await(this.pauseHandshakeTimeout.toMillis(), TimeUnit.MILLISECONDS);
          if (!echoed) {
            // doPause may already have zeroed the link credit, so falling back to UNPAUSED here
            // would leave the consumer stalled with no way to re-credit the link
            LOGGER.warn("Did not receive echoed flow to pause receiver");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      } catch (Exception e) {
        LOGGER.warn("Exception while pausing consumer: {}", e.getMessage(), e);
      } finally {
        // CAS, not set: a concurrent recovery may legitimately have moved the status, and its
        // write must win. The finally guarantees the status never stays at PAUSING (L3)
        this.pauseStatus.compareAndSet(PauseStatus.PAUSING, PauseStatus.PAUSED);
      }
    }
  }

  @Override
  public void unpause() {
    checkOpen();
    if (this.pauseStatus.compareAndSet(PauseStatus.PAUSED, PauseStatus.UNPAUSED)) {
      // top up through the same formula as any other replenish, accounting for
      // pendingWorkItems, instead of blindly granting a full window of credit
      Link currentLink = this.link;
      onExecutor(currentLink, () -> replenish(currentLink));
    }
  }

  @Override
  public long unsettledMessageCount() {
    return this.link.unsettled.get();
  }

  @Override
  public void claimDeferred(String... tokens) {
    checkOpen();
    notNull(tokens, "Tokens");
    Link claimLink = this.link;
    if (!claimLink.deferralSupported()) {
      throw new AmqpException(
          "The queue this consumer is attached to does not support deferral tokens "
              + "(only quorum queues do)");
    }
    for (String token : tokens) {
      Utils.checkDeferralToken(token);
    }
    for (int offset = 0; offset < tokens.length; offset += MAX_TOKENS_PER_FLOW) {
      String[] batch =
          Arrays.copyOfRange(tokens, offset, Math.min(offset + MAX_TOKENS_PER_FLOW, tokens.length));
      onExecutor(
          claimLink,
          () -> {
            if (!claimLink.isValid()) {
              // generation superseded by recovery; claims do not survive it anyway
              return;
            }
            try {
              claimLink.protonReceiver.writeFlowWithProperties(
                  Collections.singletonMap(DEFERRAL_TOKENS, batch));
            } catch (Exception e) {
              LOGGER.debug("Error while claiming deferred messages", e);
            }
          });
    }
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

  // Builds the native delivery handler for one receiver generation. The handler reads the
  // generation's Link through linkHolder rather than through a field, so it can only ever see
  // the Link that belongs to its own receiver: the holder is populated before any credit
  // is granted, and a stale generation's receiver never delivers again once superseded.
  private java.util.function.Consumer<Delivery> createNativeHandler(
      MessageHandler handler, AtomicReference<Link> linkHolder) {
    java.util.function.Consumer<Link> maybeIncrementUnsettled;
    java.util.function.Consumer<Delivery> dispatchedRunnable;
    if (this.preSettled) {
      maybeIncrementUnsettled = link -> {};
      dispatchedRunnable =
          delivery -> {
            try {
              delivery.settle();
            } catch (ClientException e) {
              LOGGER.warn("Error while settling message: {}", e.getMessage());
            }
            Link link = linkHolder.get();
            AmqpMessage message;
            try {
              message = new AmqpMessage(delivery.message());
            } catch (ClientException e) {
              LOGGER.warn("Error while decoding message: {}", e.getMessage());
              onExecutor(link, () -> completeWorkItem(link));
              return;
            }
            metricsCollector.consumeDisposition(ACCEPTED);
            try {
              handler.handle(PRE_SETTLED_CONTEXT, message);
            } catch (Exception ex) {
              LOGGER.warn("Error in message handler", ex);
            }
            onExecutor(link, () -> completeWorkItem(link));
          };
    } else {
      maybeIncrementUnsettled = link -> link.unsettled.incrementAndGet();
      dispatchedRunnable =
          delivery -> {
            Link link = linkHolder.get();
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
              link.unsettled.decrementAndGet();
              onExecutor(link, () -> completeWorkItem(link));
              return;
            }
            Consumer.Context context = new DeliveryContext(delivery, link, this);
            try {
              handler.handle(context, message);
            } catch (Exception ex) {
              LOGGER.warn("Error in message handler, discarding message", ex);
              try {
                context.discard();
              } catch (Exception iex) {
                LOGGER.warn("Error while discarding message", iex);
              }
            } finally {
              // pendingWorkItems is decremented exactly once, at work-item completion,
              // regardless of whether or when the application settles the message.
              onExecutor(link, () -> completeWorkItem(link));
            }
          };
    }
    return delivery -> {
      if (this.state() == OPEN) {
        Link link = linkHolder.get();
        maybeIncrementUnsettled.accept(link);
        this.metricsCollector.consume();
        link.pendingWorkItems++;
        this.consumerWorkService.dispatch(this, () -> dispatchedRunnable.accept(delivery));
      } else {
        // Consumer is not open (RECOVERING, CLOSING, CLOSED), release delivery back to broker
        // to prevent message loss and credit issues
        try {
          if (!this.preSettled) {
            delivery.disposition(DeliveryState.released(), true);
            LOGGER.debug(
                "Released delivery when consumer {} is in state {}", this.id, this.state());
          } else {
            // For pre-settled deliveries, just log since they're already settled by broker
            LOGGER.debug(
                "Dropping pre-settled delivery when consumer {} is in state {}",
                this.id,
                this.state());
          }
        } catch (ClientException e) {
          LOGGER.debug(
              "Failed to release delivery when consumer {} is not open: {}",
              this.id,
              e.getMessage());
        }
      }
    };
  }

  // Opens the first generation of the native receiver for this consumer.
  private Link openLink(Session nativeSession) {
    AtomicReference<Link> holder = new AtomicReference<>();
    ClientReceiver receiver =
        createNativeReceiver(
            nativeSession,
            this.address,
            this.preSettled,
            this.linkProperties,
            this.filters,
            this.subscriptionListener,
            createNativeHandler(this.messageHandler, holder),
            this.nativeCloseHandler);
    return buildLink(receiver, holder);
  }

  // Builds the Link for an already-open native receiver and publishes it to the
  // generation-local holder (read by the native handler), on the proton executor, before
  // returning. this.link is published separately, by activate(), which arbitrates the
  // publication against close(). The holder is still populated before any credit is granted,
  // which is all invariant I1 needs.
  private Link buildLink(ClientReceiver receiver, AtomicReference<Link> holder) {
    try {
      Scheduler executor = receiver.executor();
      boolean deferralSupported = deferralSupported(receiver);
      CountDownLatch publishedLatch = new CountDownLatch(1);
      executor.execute(
          () -> {
            ProtonReceiver protonReceiver = (ProtonReceiver) receiver.protonReceiver();
            Link newLink =
                new Link(
                    receiver,
                    executor,
                    protonReceiver,
                    protonReceiver.getCreditState(),
                    protonReceiver.sessionWindow(),
                    deferralSupported);

            EventHandler<org.apache.qpid.protonj2.engine.Receiver> eventHandler =
                protonReceiver.linkCreditUpdatedHandler();
            EventHandler<org.apache.qpid.protonj2.engine.Receiver> decorator =
                target -> {
                  eventHandler.handle(target);
                  CountDownLatch latch = this.echoedFlowAfterPauseLatch.getAndSet(null);
                  if (latch != null) {
                    latch.countDown();
                  }
                };
            protonReceiver.creditStateUpdateHandler(decorator);

            holder.set(newLink);
            publishedLatch.countDown();
          });
      if (!publishedLatch.await(10, TimeUnit.SECONDS)) {
        throw new AmqpException("Could not initialize consumer internal state");
      }
      return holder.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  // the receiver capability is advertised on the attach response, so it is available as soon as
  // the native receiver has opened, before buildLink hops onto the proton executor
  private static boolean deferralSupported(ClientReceiver receiver) {
    try {
      String[] capabilities = receiver.offeredCapabilities();
      return capabilities != null
          && Arrays.asList(capabilities).contains(DEFERRAL_TOKENS_CAPABILITY);
    } catch (ClientException e) {
      return false;
    }
  }

  // Publishes the new generation and moves the consumer to OPEN, atomically with respect to
  // close(). Returns false if the consumer was closed concurrently: the caller then still owns
  // the receiver it opened and must close it.
  private boolean activate(Link newLink) {
    synchronized (this.lifecycleLock) {
      if (this.closed.get()) {
        return false;
      }
      Link previous = this.link;
      if (previous != null && previous != newLink) {
        previous.invalidate();
      }
      this.link = newLink;
      this.state(OPEN);
      return true;
    }
  }

  void recoverAfterConnectionFailure() {
    // optimization that avoids a pointless retry loop for a consumer already closed;
    // activate() is the actual guarantee
    if (this.closed.get()) {
      LOGGER.debug("Consumer {} is closed, skipping recovery", this.id);
      return;
    }
    AtomicReference<Link> holder = new AtomicReference<>();
    ClientReceiver newReceiver =
        RetryUtils.callAndMaybeRetry(
            () ->
                createNativeReceiver(
                    this.sessionHandler.sessionNoCheck(),
                    this.address,
                    this.preSettled,
                    this.linkProperties,
                    this.filters,
                    this.subscriptionListener,
                    createNativeHandler(this.messageHandler, holder),
                    this.nativeCloseHandler),
            e -> {
              boolean shouldRetry = ExceptionUtils.noRunningStreamMemberOnNode(e);
              LOGGER.debug("Retrying receiver creation on consumer recovery: {}", shouldRetry);
              return shouldRetry;
            },
            List.of(ofSeconds(1), ofSeconds(2), ofSeconds(3), BackOffDelayPolicy.TIMEOUT),
            "Create AMQP receiver to address '%s'",
            this.address);

    // optimization that avoids a pointless buildLink for a consumer already closed after a
    // potentially long retry operation; activate() is the actual guarantee
    if (this.closed.get()) {
      LOGGER.debug("Consumer {} was closed during recovery, cleaning up new receiver", this.id);
      closeQuietly(newReceiver);
      return;
    }

    try {
      Link newLink = this.buildLink(newReceiver, holder);
      this.directReplyToAddress = newReceiver.address();
      this.beforeActivateHook.run();
      if (!this.activate(newLink)) {
        LOGGER.debug("Consumer {} was closed during recovery, closing new receiver", this.id);
        closeQuietly(newReceiver);
        return;
      }
      // the previous generation is gone; its counters are irrelevant from here on, so there is
      // no hand-reset of unsettledMessageCount: unsettledMessageCount() already reads the new
      // Link's counter, which starts at zero
      if (this.pausedOrPausing()) {
        LOGGER.debug("Consumer {} is paused, not granting credit after recovery", this.id);
      } else {
        newLink.receiver.addCredit(this.initialCredits);
      }
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e);
    }
  }

  void close(Throwable cause) {
    Link linkToClose;
    synchronized (this.lifecycleLock) {
      if (!this.closed.compareAndSet(false, true)) {
        return;
      }
      linkToClose = this.link;
      this.state(CLOSING, cause);
    }
    if (this.consumerWorkService != null) {
      this.consumerWorkService.unregister(this);
    }
    this.connection.removeConsumer(this);
    try {
      if (linkToClose != null) {
        linkToClose.receiver.close();
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

  private static void closeQuietly(ClientReceiver receiver) {
    try {
      receiver.close();
    } catch (Exception e) {
      LOGGER.debug("Error while closing receiver during cleanup: {}", e.getMessage());
    }
  }

  long id() {
    return this.id;
  }

  String queue() {
    return this.queue;
  }

  // proton executor only
  private void completeWorkItem(Link link) {
    link.pendingWorkItems--;
    replenish(link);
  }

  // proton executor only
  private void replenish(Link link) {
    if (!link.isValid() || !active()) {
      return;
    }
    int window = this.initialCredits;
    // how many messages the broker is allowed to send
    int credit = link.protonReceiver.getCredit();
    if (credit > window * 0.5) {
      // we should still receive enough messages,
      // no need to top up with a small value, this prevents unnecessary traffic
      return;
    }
    // Pre-settled messages have no settlement event to gate on, so pendingWorkItems (handler
    // concurrency) is the only available signal. Non-presettled messages are gated on
    // unsettled instead: it is decremented synchronously in DeliveryContext.settle, so a
    // replenish racing a settle on the proton executor never reads a stale, pre-decrement
    // value the way pendingWorkItems (decremented via a separately queued task) could.
    int outstanding = this.preSettled ? link.pendingWorkItems : (int) link.unsettled.get();
    // in-flight = credit (potential incoming messages) + outstanding
    int inFlight = credit + outstanding;
    if (inFlight > window * 0.7) {
      // still have plenty of work, no need to top up
      return;
    }
    try {
      link.protonReceiver.addCredit(window - inFlight);
    } catch (Exception e) {
      LOGGER.debug("Error caught during credit top-up", e);
    }
  }

  // the only thing the credit path asks about lifecycle; reimplemented in Track B
  private boolean active() {
    return !pausedOrPausing() && state() == OPEN;
  }

  // A dead executor means the generation is gone and its counters are irrelevant.
  private static void onExecutor(Link link, Runnable task) {
    try {
      link.executor.execute(task);
    } catch (RejectedExecutionException e) {
      LOGGER.debug("Proton executor gone, skipping task for stale link");
    }
  }

  private void doPause(Link link) {
    link.creditState.updateCredit(0);
    link.creditState.updateEcho(true);
    link.sessionWindow.writeFlow(link.protonReceiver);
  }

  boolean pausedOrPausing() {
    return this.pauseStatus.get() != PauseStatus.UNPAUSED;
  }

  PauseStatus pauseStatus() {
    return this.pauseStatus.get();
  }

  enum PauseStatus {
    UNPAUSED,
    PAUSING,
    PAUSED
  }

  // One instance per receiver generation. Everything the credit path touches lives here, so a
  // generation's state cannot outlive the executor that owns it.
  private static final class Link {
    private final ClientReceiver receiver;
    private final Scheduler executor;
    private final ProtonReceiver protonReceiver;
    private final ProtonLinkCreditState creditState;
    private final ProtonSessionIncomingWindow sessionWindow;
    private final AtomicLong unsettled = new AtomicLong(0);
    private int pendingWorkItems; // I2: proton-executor-confined, plain int
    private volatile boolean current = true; // false once superseded
    // set once, from the attach response, before this Link is published
    private final boolean deferralSupported;

    private Link(
        ClientReceiver receiver,
        Scheduler executor,
        ProtonReceiver protonReceiver,
        ProtonLinkCreditState creditState,
        ProtonSessionIncomingWindow sessionWindow,
        boolean deferralSupported) {
      this.receiver = receiver;
      this.executor = executor;
      this.protonReceiver = protonReceiver;
      this.creditState = creditState;
      this.sessionWindow = sessionWindow;
      this.deferralSupported = deferralSupported;
    }

    private boolean deferralSupported() {
      return this.deferralSupported;
    }

    boolean isValid() {
      return this.current;
    }

    void invalidate() {
      this.current = false;
    }
  }

  private static class DeliveryContext implements Consumer.Context {

    private static final DeliveryState REJECTED = DeliveryState.rejected(null, null);
    private final AtomicBoolean settled = new AtomicBoolean(false);
    private final Delivery delivery;
    private final Link link;
    private final AmqpConsumer consumer;

    private DeliveryContext(Delivery delivery, Link link, AmqpConsumer consumer) {
      this.delivery = delivery;
      this.link = link;
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
      this.requeue(annotations, false);
    }

    @Override
    public void requeue(Map<String, Object> annotations, boolean deliveryFailed) {
      annotations = annotations == null ? Collections.emptyMap() : annotations;
      Utils.checkMessageAnnotations(annotations);
      this.settle(
          DeliveryState.modified(deliveryFailed, false, annotations),
          REQUEUED,
          "requeue (modified)");
    }

    @Override
    public void delayedRetry(Duration delay) {
      this.delayedRetry(delay, false);
    }

    @Override
    public void delayedRetry(Duration delay, boolean deliveryFailed) {
      notNull(delay, "Delay");
      this.delayedRetry(Instant.now().plus(delay), deliveryFailed);
    }

    @Override
    public void delayedRetry(Instant deliveryTime) {
      this.delayedRetry(deliveryTime, false);
    }

    @Override
    public void delayedRetry(Instant deliveryTime, boolean deliveryFailed) {
      notNull(deliveryTime, "Delivery time");
      Map<String, Object> annotations =
          Collections.singletonMap(AmqpUtils.ANN_DELIVERY_TIME, Date.from(deliveryTime));
      this.requeue(annotations, deliveryFailed);
    }

    @Override
    public void delayedRetry(Duration delay, boolean deliveryFailed, String deferralToken) {
      notNull(delay, "Delay");
      this.delayedRetry(Instant.now().plus(delay), deliveryFailed, deferralToken);
    }

    @Override
    public void delayedRetry(Instant deliveryTime, boolean deliveryFailed, String deferralToken) {
      notNull(deliveryTime, "Delivery time");
      Utils.checkDeferralToken(deferralToken);
      Utils.checkDeferralDeliveryTime(deliveryTime);
      Map<String, Object> annotations =
          Map.of(
              AmqpUtils.ANN_DELIVERY_TIME,
              Date.from(deliveryTime),
              AmqpUtils.ANN_DEFERRAL_TOKEN,
              deferralToken);
      this.requeue(annotations, deliveryFailed);
    }

    @Override
    public BatchContext batch(int batchSizeHint) {
      return new BatchDeliveryContext(batchSizeHint, link, consumer);
    }

    private void settle(
        DeliveryState state, MetricsCollector.ConsumeDisposition disposition, String label) {
      if (settled.compareAndSet(false, true)) {
        try {
          delivery.disposition(state, true); // queues the disposition frame
        } catch (Exception e) {
          handleContextException(this.consumer, e, label); // may rethrow, as today
        } finally {
          link.unsettled.decrementAndGet(); // no drift if the write above failed
          consumer.metricsCollector.consumeDisposition(disposition);
          onExecutor(link, () -> consumer.replenish(link)); // I5: queued after the disposition
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
    private final Link link;
    private final AmqpConsumer consumer;

    private BatchDeliveryContext(int batchSizeHint, Link link, AmqpConsumer consumer) {
      this.contexts = new ArrayList<>(batchSizeHint);
      this.link = link;
      this.consumer = consumer;
    }

    @Override
    public void add(Consumer.Context context) {
      if (this.settled.get()) {
        throw new IllegalStateException("Batch is closed");
      }
      if (!(context instanceof DeliveryContext)) {
        throw new IllegalArgumentException("Context type not supported: " + context);
      }
      DeliveryContext dctx = (DeliveryContext) context;
      if (dctx.consumer != this.consumer) {
        // a foreign context would silently apply this batch's delivery IDs to its own link
        throw new IllegalArgumentException("Context does not belong to this batch's consumer");
      }
      if (dctx.link != this.link) {
        // a different generation than this batch's: it cannot be settled on this link, and the
        // broker will redeliver it, so this is a normal post-recovery condition, not an error
        LOGGER.debug("Skipping context from a stale link generation");
        return;
      }
      // marking the context as settled avoids operation on it and deduplicates as well
      if (dctx.settled.compareAndSet(false, true)) {
        this.contexts.add(dctx);
      } else {
        throw new IllegalStateException("Message already settled");
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
          new Modified(true, true, ClientConversionSupport.toSymbolKeyedMap(annotations));
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
    public void requeue(Map<String, Object> annotations, boolean deliveryFailed) {
      annotations = annotations == null ? Collections.emptyMap() : annotations;
      Utils.checkMessageAnnotations(annotations);
      Modified state =
          new Modified(
              deliveryFailed, false, ClientConversionSupport.toSymbolKeyedMap(annotations));
      this.settle(state, REQUEUED, "requeue (modified)");
    }

    @Override
    public void delayedRetry(Duration delay) {
      this.delayedRetry(delay, false);
    }

    @Override
    public void delayedRetry(Duration delay, boolean deliveryFailed) {
      this.delayedRetry(Instant.now().plus(delay), deliveryFailed);
    }

    @Override
    public void delayedRetry(Instant deliveryTime) {
      this.delayedRetry(deliveryTime, false);
    }

    @Override
    public void delayedRetry(Instant deliveryTime, boolean deliveryFailed) {
      notNull(deliveryTime, "Delivery time");
      Map<String, Object> annotations =
          Collections.singletonMap(AmqpUtils.ANN_DELIVERY_TIME, Date.from(deliveryTime));
      this.requeue(annotations, deliveryFailed);
    }

    @Override
    public void delayedRetry(Duration delay, boolean deliveryFailed, String deferralToken) {
      this.delayedRetry(Instant.now().plus(delay), deliveryFailed, deferralToken);
    }

    @Override
    public void delayedRetry(Instant deliveryTime, boolean deliveryFailed, String deferralToken) {
      notNull(deliveryTime, "Delivery time");
      Utils.checkDeferralToken(deferralToken);
      Utils.checkDeferralDeliveryTime(deliveryTime);
      Map<String, Object> annotations =
          Map.of(
              AmqpUtils.ANN_DELIVERY_TIME,
              Date.from(deliveryTime),
              AmqpUtils.ANN_DEFERRAL_TOKEN,
              deferralToken);
      this.requeue(annotations, deliveryFailed);
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
          long[][] ranges =
              SerialNumberUtils.ranges(this.contexts, ctx -> ctx.delivery.getDeliveryId());
          // Decrement unsettled synchronously, before replenish is even scheduled, so it can
          // never read a stale value (same reasoning as the single-delivery settle path).
          // I3: pendingWorkItems is not touched here, those decrements already happened when
          // each handler returned. Dispositions are queued before the replenish that follows.
          // add() guarantees every context in the batch shares this.link.
          link.unsettled.addAndGet(-batchSize);
          onExecutor(
              link,
              () -> {
                for (long[] range : ranges) {
                  link.protonReceiver.disposition(state, range);
                }
                consumer.replenish(link);
              });
          IntStream.range(0, batchSize)
              .forEach(
                  ignored -> {
                    consumer.metricsCollector.consumeDisposition(disposition);
                  });
        } catch (Exception e) {
          handleContextException(this.consumer, e, label);
        }
      }
    }
  }

  // for testing
  boolean protonHasUnsettled() {
    return this.link.protonReceiver.hasUnsettled();
  }

  // for testing
  int pendingWorkItems() {
    return readFromExecutor(link -> link.pendingWorkItems);
  }

  // for testing
  int credits() {
    return readFromExecutor(link -> link.protonReceiver.getCredit());
  }

  // pendingWorkItems is a plain int (I2), so it can only be read safely on the link's executor
  private int readFromExecutor(java.util.function.ToIntFunction<Link> read) {
    Link link = this.link;
    CompletableFuture<Integer> future = new CompletableFuture<>();
    link.executor.execute(() -> future.complete(read.applyAsInt(link)));
    try {
      return future.get(2, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
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
    public void requeue(Map<String, Object> annotations, boolean deliveryFailed) {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public void delayedRetry(Duration delay) {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public void delayedRetry(Duration delay, boolean deliveryFailed) {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public void delayedRetry(Instant deliveryTime, boolean deliveryFailed) {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public void delayedRetry(Instant deliveryTime) {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public void delayedRetry(Duration delay, boolean deliveryFailed, String deferralToken) {
      throw new UnsupportedOperationException("auto-settle on, message is already disposed");
    }

    @Override
    public void delayedRetry(Instant deliveryTime, boolean deliveryFailed, String deferralToken) {
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

  // for testing
  @SuppressFBWarnings("REC_CATCH_EXCEPTION")
  String diagnosticState() {
    int nativeCredits = -1;
    int pendingWorkItems = -1;
    Link link = this.link;
    Scheduler exec = link == null ? null : link.executor;
    ProtonReceiver receiver = link == null ? null : link.protonReceiver;

    if (exec != null && !exec.isShutdown() && receiver != null) {
      try {
        CompletableFuture<int[]> stateFuture = new CompletableFuture<>();
        exec.execute(
            () -> stateFuture.complete(new int[] {receiver.getCredit(), link.pendingWorkItems}));
        // Block briefly to retrieve the values from the proton thread
        int[] state = stateFuture.get(2, TimeUnit.SECONDS);
        nativeCredits = state[0];
        pendingWorkItems = state[1];
      } catch (Exception e) {
        nativeCredits = -2; // -2 indicates we failed to read the credits safely
      }
    }

    return String.format(
        "Consumer-%d | queue='%s' | state=%s | pauseStatus=%s | unsettledCount=%d | pendingWorkItems=%d | nativeCredits=%d",
        this.id,
        this.queue == null ? "<direct-reply-to>" : this.queue,
        this.state(),
        this.pauseStatus.get(),
        this.unsettledMessageCount(),
        pendingWorkItems,
        nativeCredits);
  }
}
