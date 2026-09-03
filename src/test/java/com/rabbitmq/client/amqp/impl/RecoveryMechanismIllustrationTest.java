// Copyright (c) 2026 Broadcom. All Rights Reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.jetbrains.jetCheck.Generator;
import org.jetbrains.jetCheck.PropertyChecker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Standalone illustration of the state-management and recovery mechanism used by {@link
 * AmqpConnection} / {@link ConnectionStateClient}, with everything AMQP-specific stripped away.
 *
 * <p>A {@code TestClient} holds a {@code Link} to a {@code FakeServer}. When the link dies, the
 * client recovers in two phases: it reconnects (phase 1) and then reinitializes (phase 2). There is
 * no broker, no queue, no publisher: the vocabulary is deliberately generic so the pattern can be
 * lifted into another system.
 *
 * <p>Five patterns make up the mechanism:
 *
 * <ul>
 *   <li><b>Single-thread loop as the only mutation gate.</b> {@code ClientState} is only ever
 *       touched from inside the {@link EventLoop} it is registered with, via {@link
 *       EventLoop.Client#submit(Consumer)} and {@link
 *       EventLoop.Client#query(java.util.function.Function)}
 *   <li><b>State owned exclusively by that loop.</b> No other thread reads or writes {@code
 *       internalState} or {@code epoch} directly; everything goes through the loop
 *   <li><b>Pure transition functions.</b> Each {@code onXxx} method takes the current state and
 *       returns a {@code TransitionResult} - the next state, the next epoch and an {@code effect} -
 *       without mutating anything or touching a thread
 *   <li><b>Epoch as a fencing token.</b> Every recovery attempt is stamped with the epoch that was
 *       current when it was dispatched. When it reports back, a mismatch means a newer attempt has
 *       since taken over and the report is stale
 *   <li><b>Multi-phase recovery dispatched off-loop.</b> Recovery work runs on a separate executor
 *       and reports its outcome back into the loop, which is what makes stale reports possible in
 *       the first place
 * </ul>
 *
 * <p>State diagram:
 *
 * <pre>
 *                  disconnect(recoverable)         reconnected
 *   INITIAL --+    epoch+1                         (same epoch)
 *             +--&gt; RECONNECTING ----------------&gt; REINITIALIZING
 *   CONNECTED-+         ^     ^                        |     |
 *       ^               |     | reinit failed          |     | disconnect
 *       |               |     | epoch+1                |     | epoch+1
 *       |               |     +------------------------+     |
 *       |               +--------------------------------------+
 *       +------------------ reinitialized (same epoch) --------+
 *
 *   any state -- markClosed --&gt; CLOSED (epoch+1, fences every in-flight task)
 * </pre>
 *
 * <p>The illustration exposes only this fine-grained {@code InternalState}; the real code also
 * maintains a coarser public {@code Resource.State} ({@code OPEN}, {@code RECOVERING}, ...) for the
 * user-facing API, which is cut here for clarity.
 *
 * <p>Mapping back to the real code, for readers who want to check this illustration is faithful:
 *
 * <pre>
 *   Illustration            Real code
 *   ------------            ------------------------------------------------
 *   TestClient              AmqpConnection
 *   Link                    org.apache.qpid.protonj2.client.Connection
 *   dispatchReconnect       AmqpConnection.dispatchNativeRecovery
 *   dispatchReinitialize    AmqpConnection.dispatchTopologyRecovery
 *   ClientState             ConnectionStateClient.ConnectionState
 * </pre>
 */
class RecoveryMechanismIllustrationTest {

  EventExecutorGroup eventExecutorGroup;
  EventLoop eventLoop;
  FakeServer server;
  ManualExecutor recoveryExecutor;
  TestClient client;

  @BeforeEach
  void beforeEach() {
    this.eventExecutorGroup = new DefaultEventExecutorGroup(1);
    this.eventLoop = new EventLoop(this.eventExecutorGroup);
    this.server = new FakeServer();
    this.recoveryExecutor = new ManualExecutor();
    this.client = new TestClient(this.eventLoop, this.server, this.recoveryExecutor);
  }

  @AfterEach
  void afterEach() {
    Link current = this.client.link();
    for (Link link : this.server.links()) {
      if (link != current) {
        assertThat(link.isClosed())
            .as("link %d should have been closed, it is not the current one", link.id())
            .isTrue();
      }
    }
    this.eventLoop.close();
    this.eventExecutorGroup.shutdownGracefully();
  }

  // ---------------------------------------------------------------------
  // A. Pure transition tests
  //
  // No thread involved: static decision methods are called directly, and the
  // returned effect is run against a mock to check what it would do.
  // ---------------------------------------------------------------------

  @Test
  void disconnectWhenConnectedStartsReconnectAndBumpsEpoch() {
    ClientState.TransitionResult result =
        ClientState.onDisconnect(
            ClientState.InternalState.CONNECTED, 1, 1, new RecoverableFailure("boom"));

    assertThat(result.state()).isEqualTo(ClientState.InternalState.RECONNECTING);
    assertThat(result.epoch()).isEqualTo(2);

    RecoverableClient mockClient = mock(RecoverableClient.class);
    result.effect().accept(new ClientState(mockClient));

    verify(mockClient).link(null);
    verify(mockClient).submitRecoveryTask(any());
  }

  @Test
  void staleDisconnectIsIgnored() {
    ClientState.TransitionResult result =
        ClientState.onDisconnect(
            ClientState.InternalState.CONNECTED, 5, 3, new RecoverableFailure("boom"));

    assertThat(result.state()).isEqualTo(ClientState.InternalState.CONNECTED);
    assertThat(result.epoch()).isEqualTo(5);

    RecoverableClient mockClient = mock(RecoverableClient.class);
    result.effect().accept(new ClientState(mockClient));
    verifyNoInteractions(mockClient);
  }

  @Test
  void disconnectWhenClosedIsIgnored() {
    ClientState.TransitionResult result =
        ClientState.onDisconnect(
            ClientState.InternalState.CLOSED, 5, 5, new RecoverableFailure("boom"));

    assertThat(result.state()).isEqualTo(ClientState.InternalState.CLOSED);
    assertThat(result.epoch()).isEqualTo(5);

    RecoverableClient mockClient = mock(RecoverableClient.class);
    result.effect().accept(new ClientState(mockClient));
    verifyNoInteractions(mockClient);
  }

  @Test
  void nonRecoverableDisconnectClosesTheClient() {
    RuntimeException cause = new RuntimeException("non-recoverable");
    ClientState.TransitionResult result =
        ClientState.onDisconnect(ClientState.InternalState.CONNECTED, 1, 1, cause);

    assertThat(result.state()).isEqualTo(ClientState.InternalState.CONNECTED);
    assertThat(result.epoch()).isEqualTo(1);

    RecoverableClient mockClient = mock(RecoverableClient.class);
    result.effect().accept(new ClientState(mockClient));
    verify(mockClient).close(cause);
  }

  @Test
  void disconnectWhileReconnectingDoesNotBumpEpoch() {
    RecoverableFailure cause = new RecoverableFailure("boom");
    ClientState.TransitionResult result =
        ClientState.onDisconnect(ClientState.InternalState.RECONNECTING, 3, 3, cause);

    assertThat(result.state()).isEqualTo(ClientState.InternalState.RECONNECTING);
    assertThat(result.epoch()).isEqualTo(3);

    RecoverableClient mockClient = mock(RecoverableClient.class);
    result.effect().accept(new ClientState(mockClient));

    verify(mockClient).abortPendingWork(cause);
    verifyNoMoreInteractions(mockClient);
  }

  @Test
  void disconnectWhileReinitializingRestartsFromReconnect() {
    ClientState.TransitionResult result =
        ClientState.onDisconnect(
            ClientState.InternalState.REINITIALIZING, 4, 4, new RecoverableFailure("boom"));

    assertThat(result.state()).isEqualTo(ClientState.InternalState.RECONNECTING);
    assertThat(result.epoch()).isEqualTo(5);

    RecoverableClient mockClient = mock(RecoverableClient.class);
    result.effect().accept(new ClientState(mockClient));

    verify(mockClient).link(null);
    verify(mockClient).submitRecoveryTask(any());
  }

  @Test
  void reconnectSuccessMovesToReinitializingKeepingEpoch() {
    Link link = new Link(1);
    ClientState.TransitionResult result =
        ClientState.onReconnectSuccess(ClientState.InternalState.RECONNECTING, 2, 2, link);

    assertThat(result.state()).isEqualTo(ClientState.InternalState.REINITIALIZING);
    assertThat(result.epoch()).isEqualTo(2);

    RecoverableClient mockClient = mock(RecoverableClient.class);
    result.effect().accept(new ClientState(mockClient));

    verify(mockClient).link(link);
    verify(mockClient).submitRecoveryTask(any());
  }

  @Test
  void staleReconnectSuccessClosesTheZombieLink() {
    Link zombie = new Link(99);
    ClientState.TransitionResult result =
        ClientState.onReconnectSuccess(ClientState.InternalState.RECONNECTING, 5, 3, zombie);

    assertThat(result.state()).isEqualTo(ClientState.InternalState.RECONNECTING);
    assertThat(result.epoch()).isEqualTo(5);

    RecoverableClient mockClient = mock(RecoverableClient.class);
    result.effect().accept(new ClientState(mockClient));

    assertThat(zombie.isClosed()).isTrue();
    verifyNoInteractions(mockClient);
  }

  @Test
  void reinitializeSuccessMovesToConnected() {
    ClientState.TransitionResult result =
        ClientState.onReinitializeSuccess(ClientState.InternalState.REINITIALIZING, 2, 2);

    assertThat(result.state()).isEqualTo(ClientState.InternalState.CONNECTED);
    assertThat(result.epoch()).isEqualTo(2);
  }

  @Test
  void reinitializeFailureBumpsEpochAndRestartsReconnect() {
    ClientState.TransitionResult result =
        ClientState.onReinitializeFailure(ClientState.InternalState.REINITIALIZING, 3, 3);

    assertThat(result.state()).isEqualTo(ClientState.InternalState.RECONNECTING);
    assertThat(result.epoch()).isEqualTo(4);

    Link halfInitializedLink = new Link(7);
    RecoverableClient mockClient = mock(RecoverableClient.class);
    when(mockClient.link()).thenReturn(halfInitializedLink);

    result.effect().accept(new ClientState(mockClient));

    assertThat(halfInitializedLink.isClosed()).isTrue();
    verify(mockClient).link(null);
    verify(mockClient).submitRecoveryTask(any());
  }

  @Test
  void markClosedBumpsEpochToFenceInFlightTasks() {
    ClientState.TransitionResult result =
        ClientState.onMarkClosed(6, new RuntimeException("shutting down"));

    assertThat(result.state()).isEqualTo(ClientState.InternalState.CLOSED);
    assertThat(result.epoch()).isEqualTo(7);
  }

  // ---------------------------------------------------------------------
  // B. jetCheck invariants
  //
  // Mirrors ConnectionStateTransitionsTest so both styles sit side by side.
  // ---------------------------------------------------------------------

  private static final Generator<ClientState.InternalState> STATES =
      Generator.sampledFrom(ClientState.InternalState.values());

  private static final Generator<Long> EPOCHS = Generator.integers(1, 1_000_000).map(i -> (long) i);

  private static final Generator<Throwable> CAUSES =
      Generator.booleans()
          .map(
              recoverable ->
                  recoverable
                      ? new RecoverableFailure("simulated failure")
                      : new RuntimeException("simulated failure"));

  @Test
  void disconnectNeverDecreasesEpoch() {
    Generator<DisconnectInput> inputs =
        Generator.from(
            data ->
                new DisconnectInput(
                    data.generate(STATES),
                    data.generate(EPOCHS),
                    data.generate(EPOCHS),
                    data.generate(CAUSES)));

    PropertyChecker.forAll(
        inputs,
        input -> {
          ClientState.TransitionResult result =
              ClientState.onDisconnect(input.state, input.epoch, input.eventEpoch, input.cause);
          return result.epoch() >= input.epoch;
        });
  }

  @Test
  void staleDisconnectIsANoOp() {
    Generator<DisconnectInput> staleInputs =
        Generator.from(
            data -> {
              long epoch = data.generate(EPOCHS);
              long delta = data.generate(Generator.integers(1, 1000).map(i -> (long) i));
              return new DisconnectInput(
                  data.generate(STATES), epoch, epoch - delta, data.generate(CAUSES));
            });

    PropertyChecker.forAll(
        staleInputs,
        input -> {
          ClientState.TransitionResult result =
              ClientState.onDisconnect(input.state, input.epoch, input.eventEpoch, input.cause);
          return result.state() == input.state && result.epoch() == input.epoch;
        });
  }

  private static final class DisconnectInput {
    final ClientState.InternalState state;
    final long epoch;
    final long eventEpoch;
    final Throwable cause;

    DisconnectInput(ClientState.InternalState state, long epoch, long eventEpoch, Throwable cause) {
      this.state = state;
      this.epoch = epoch;
      this.eventEpoch = eventEpoch;
      this.cause = cause;
    }
  }

  // ---------------------------------------------------------------------
  // C. Lifecycle tests
  //
  // Real EventLoop on a DefaultEventExecutorGroup(1), a ManualExecutor standing
  // in for the off-loop recovery executor, and a FakeServer. No sleeps: retries
  // and recovery phases are advanced explicitly via recoveryExecutor.runNext().
  // ---------------------------------------------------------------------

  @Test
  void disconnectTriggersTwoPhaseRecoveryAndReconnects() {
    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.CONNECTED);
    assertThat(this.client.epoch()).isEqualTo(1);
    Link oldLink = this.client.link();

    oldLink.fail(new RecoverableFailure("boom"));
    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.RECONNECTING);
    assertThat(this.client.epoch()).isEqualTo(2);

    this.recoveryExecutor.runNext(); // phase 1: reconnect
    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.REINITIALIZING);
    assertThat(this.client.epoch()).isEqualTo(2);

    this.recoveryExecutor.runNext(); // phase 2: reinitialize
    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.CONNECTED);
    assertThat(this.client.epoch()).isEqualTo(2);

    assertThat(this.client.link()).isNotSameAs(oldLink);
    assertThat(this.server.connectAttempts()).isEqualTo(2); // initial connect + one reconnect
    assertThat(this.recoveryExecutor.pending()).isZero();
  }

  @Test
  void reconnectRetriesWithoutBumpingEpoch() {
    this.server.failNextConnects(2);
    Link oldLink = this.client.link();

    oldLink.fail(new RecoverableFailure("boom"));
    assertThat(this.client.epoch()).isEqualTo(2);

    this.recoveryExecutor.runNext(); // two failed attempts, then a successful one

    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.REINITIALIZING);
    assertThat(this.client.epoch()).isEqualTo(2); // retries never bump the epoch
    assertThat(this.server.connectAttempts()).isEqualTo(4); // initial + 2 failures + 1 success

    this.recoveryExecutor.runNext(); // phase 2
    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.CONNECTED);
    assertThat(this.client.epoch()).isEqualTo(2);
  }

  @Test
  void reinitializeFailureRestartsRecoveryWithNewEpoch() {
    Link oldLink = this.client.link();
    oldLink.fail(new RecoverableFailure("boom"));

    this.recoveryExecutor.runNext(); // phase 1 succeeds
    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.REINITIALIZING);
    Link halfRecoveredLink = this.client.link();

    this.server.failNextInits(1);
    this.recoveryExecutor.runNext(); // phase 2 fails

    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.RECONNECTING);
    assertThat(this.client.epoch())
        .isEqualTo(3); // one bump for the disconnect, one for the failure
    assertThat(halfRecoveredLink.isClosed()).isTrue();

    this.recoveryExecutor.runNext(); // phase 1 again
    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.REINITIALIZING);

    this.recoveryExecutor.runNext(); // phase 2 again, succeeds this time
    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.CONNECTED);
    assertThat(this.client.epoch()).isEqualTo(3);
  }

  @Test
  void staleDisconnectFromOldLinkIsIgnored() {
    Link oldLink = this.client.link();
    oldLink.fail(new RecoverableFailure("boom"));
    this.recoveryExecutor.runNext(); // phase 1
    this.recoveryExecutor.runNext(); // phase 2
    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.CONNECTED);
    long epochAfterRecovery = this.client.epoch();

    // the old, already-dead link fires again; its handler still carries the epoch
    // it was created under, which is now stale
    oldLink.fail(new RecoverableFailure("late failure from the dead link"));

    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.CONNECTED);
    assertThat(this.client.epoch()).isEqualTo(epochAfterRecovery);
  }

  @Test
  void lateReconnectSuccessAfterCloseClosesTheZombieLink() {
    Link oldLink = this.client.link();
    oldLink.fail(new RecoverableFailure("boom"));
    assertThat(this.client.internalState()).isEqualTo(ClientState.InternalState.RECONNECTING);
    assertThat(this.recoveryExecutor.pending()).isEqualTo(1);

    this.client.close(new RuntimeException("closing while a reconnect is in flight"));

    // the pending dispatchReconnect task runs after the client (and its loop registration)
    // has already been closed
    assertThatCode(() -> this.recoveryExecutor.drain()).doesNotThrowAnyException();

    List<Link> links = this.server.links();
    Link zombie = links.get(links.size() - 1);
    assertThat(zombie).isNotSameAs(oldLink);
    assertThat(zombie.isClosed()).isTrue();
    assertThat(this.client.link()).isNotSameAs(zombie);
  }

  @Test
  void reconnectExhaustionClosesTheClient() {
    this.server.failNextConnects(10); // more failures than dispatchReconnect will ever retry
    Link oldLink = this.client.link();

    oldLink.fail(new RecoverableFailure("boom"));
    this.recoveryExecutor.runNext(); // every attempt fails, dispatchReconnect gives up

    assertThat(this.client.isClosed()).isTrue();
    assertThat(this.server.connectAttempts()).isEqualTo(1 + TestClient.MAX_RECONNECT_ATTEMPTS);
  }

  @Test
  void allMutationsHappenOnTheLoopThread() {
    RecoverableClient mockClient = mock(RecoverableClient.class);
    EventLoop.Client<ClientState> probe =
        this.eventLoop.register(() -> new ClientState(mockClient));

    AtomicReference<Thread> loopThread = new AtomicReference<>();
    probe.submit(state -> loopThread.set(Thread.currentThread()));

    AtomicReference<Thread> effectThread = new AtomicReference<>();
    doAnswer(
            invocation -> {
              effectThread.set(Thread.currentThread());
              return null;
            })
        .when(mockClient)
        .submitRecoveryTask(any());

    probe.submit(state -> state.handleDisconnect(1, new RecoverableFailure("boom")));

    assertThat(effectThread.get()).isEqualTo(loopThread.get());
    assertThat(effectThread.get()).isNotEqualTo(Thread.currentThread());

    probe.close();
  }

  // ---------------------------------------------------------------------
  // Nested types
  // ---------------------------------------------------------------------

  /** Hands out {@link Link}s, optionally simulating connect/initialize failures. */
  private static final class FakeServer {

    private final AtomicInteger linkSequence = new AtomicInteger();
    private final AtomicInteger connectAttempts = new AtomicInteger();
    private final AtomicInteger remainingConnectFailures = new AtomicInteger();
    private final AtomicInteger remainingInitFailures = new AtomicInteger();
    private final List<Link> links = new CopyOnWriteArrayList<>();

    Link connect() {
      this.connectAttempts.incrementAndGet();
      if (this.remainingConnectFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
        throw new RecoverableFailure("simulated connect failure");
      }
      Link link = new Link(this.linkSequence.incrementAndGet());
      this.links.add(link);
      return link;
    }

    void initialize() {
      if (this.remainingInitFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
        throw new RecoverableFailure("simulated initialize failure");
      }
    }

    void failNextConnects(int n) {
      this.remainingConnectFailures.set(n);
    }

    void failNextInits(int n) {
      this.remainingInitFailures.set(n);
    }

    int connectAttempts() {
      return this.connectAttempts.get();
    }

    List<Link> links() {
      return this.links;
    }
  }

  /**
   * The disposable resource a link stands for. {@link #fail(Throwable)} simulates the IO thread
   * firing the disconnect handler; the link is considered closed at the transport level from that
   * point on, exactly like a socket that just died, so nothing needs to close it again.
   */
  private static final class Link {

    private final int id;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile Consumer<Throwable> disconnectHandler = cause -> {};

    Link(int id) {
      this.id = id;
    }

    int id() {
      return this.id;
    }

    void onDisconnect(Consumer<Throwable> handler) {
      this.disconnectHandler = handler;
    }

    void close() {
      this.closed.set(true);
    }

    boolean isClosed() {
      return this.closed.get();
    }

    void fail(Throwable cause) {
      this.closed.set(true);
      this.disconnectHandler.accept(cause);
    }
  }

  /**
   * Narrow view of {@link TestClient} that {@link ClientState} manipulates. Mirrors {@code
   * ConnectionStateClient.RecoverableConnection}.
   */
  private interface RecoverableClient {

    /** The current link, or {@code null} once it has been reset pending a new one. */
    Link link();

    /** Sync on success, {@code link(null)} to reset. */
    void link(Link link);

    void abortPendingWork(Throwable cause);

    void dispatchReconnect(long epoch);

    void reinitialize(long epoch);

    void close(Throwable cause);

    void submitRecoveryTask(Runnable task);
  }

  /**
   * Owns {@code internalState}, {@code epoch} and the {@link RecoverableClient}. Same three-part
   * layout as {@code ConnectionStateClient.ConnectionState}: decision logic, then the imperative
   * code that applies it.
   */
  private static final class ClientState {

    private static final Predicate<Throwable> RECOVERY_PREDICATE =
        e -> e instanceof RecoverableFailure;

    private InternalState internalState = InternalState.INITIAL;
    private long epoch = 1;
    private final RecoverableClient client;

    private ClientState(RecoverableClient client) {
      this.client = client;
    }

    InternalState internalState() {
      return this.internalState;
    }

    long epoch() {
      return this.epoch;
    }

    private void markConnected() {
      this.internalState = InternalState.CONNECTED;
    }

    private void handleDisconnect(long eventEpoch, Throwable cause) {
      apply(onDisconnect(this.internalState, this.epoch, eventEpoch, cause));
    }

    private void handleReconnectSuccess(long attemptEpoch, Link link) {
      apply(onReconnectSuccess(this.internalState, this.epoch, attemptEpoch, link));
    }

    private void handleReinitializeSuccess(long attemptEpoch) {
      apply(onReinitializeSuccess(this.internalState, this.epoch, attemptEpoch));
    }

    private void handleReinitializeFailure(long attemptEpoch) {
      apply(onReinitializeFailure(this.internalState, this.epoch, attemptEpoch));
    }

    private void markClosed(Throwable cause) {
      apply(onMarkClosed(this.epoch, cause));
    }

    // --------------------------------------------------------------
    // Decision logic
    // --------------------------------------------------------------

    static TransitionResult onDisconnect(
        InternalState state, long epoch, long eventEpoch, Throwable cause) {
      if (eventEpoch < epoch) {
        return TransitionResult.noChange(state, epoch);
      }

      if (state == InternalState.CLOSED) {
        return TransitionResult.noChange(state, epoch);
      }

      if (!RECOVERY_PREDICATE.test(cause)) {
        return TransitionResult.of(state, epoch, s -> s.client.close(cause));
      }

      if (state == InternalState.RECONNECTING) {
        // the retry loop inside dispatchReconnect owns this failure; just unblock pending work
        return TransitionResult.of(state, epoch, s -> s.client.abortPendingWork(cause));
      }

      // CONNECTED or REINITIALIZING: a disconnect is a hard failure, start over
      long newEpoch = epoch + 1;
      return TransitionResult.of(
          InternalState.RECONNECTING,
          newEpoch,
          s -> {
            // the link already tore itself down at the transport level (see Link#fail), so it
            // only needs to be forgotten here, not closed again
            s.client.link(null);
            s.client.submitRecoveryTask(() -> s.client.dispatchReconnect(newEpoch));
          });
    }

    private static TransitionResult onReconnectSuccess(
        InternalState state, long epoch, long attemptEpoch, Link link) {
      if (isStale(state, epoch, attemptEpoch)) {
        // a zombie report: the link it created was never handed to the client, close it
        return TransitionResult.of(state, epoch, s -> s.closeZombieLink(link));
      }

      return TransitionResult.of(
          InternalState.REINITIALIZING,
          epoch,
          s -> {
            s.client.link(link);
            s.client.submitRecoveryTask(() -> s.client.reinitialize(attemptEpoch));
          });
    }

    private static TransitionResult onReinitializeSuccess(
        InternalState state, long epoch, long attemptEpoch) {
      if (isStale(state, epoch, attemptEpoch)) {
        return TransitionResult.noChange(state, epoch);
      }
      return TransitionResult.of(InternalState.CONNECTED, epoch, s -> {});
    }

    private static TransitionResult onReinitializeFailure(
        InternalState state, long epoch, long attemptEpoch) {
      if (isStale(state, epoch, attemptEpoch)) {
        return TransitionResult.noChange(state, epoch);
      }

      long newEpoch = epoch + 1;
      return TransitionResult.of(
          InternalState.RECONNECTING,
          newEpoch,
          s -> {
            // unlike a disconnect, the link itself is still alive here; only the higher-level
            // reinitialize call failed, so it must be closed explicitly
            s.tearDownFailedLink();
            s.client.submitRecoveryTask(() -> s.client.dispatchReconnect(newEpoch));
          });
    }

    private static TransitionResult onMarkClosed(long epoch, Throwable cause) {
      // invalidates every in-flight task instantly: none of them was stamped with this epoch.
      // The actual teardown (closing the link, closing the loop registration) is done by the
      // caller of close(), not here: calling back into it would recurse forever
      return TransitionResult.of(InternalState.CLOSED, epoch + 1, s -> {});
    }

    // --------------------------------------------------------------
    // End of decision logic
    // --------------------------------------------------------------

    private static boolean isStale(InternalState state, long epoch, long attemptEpoch) {
      return epoch != attemptEpoch || state == InternalState.CLOSED;
    }

    // --------------------------------------------------------------
    // Imperative code (apply decisions, running effects)
    // --------------------------------------------------------------

    private void apply(TransitionResult result) {
      this.internalState = result.state();
      this.epoch = result.epoch();
      result.effect().accept(this);
    }

    private void closeZombieLink(Link link) {
      link.close();
    }

    private void tearDownFailedLink() {
      Link link = this.client.link();
      if (link != null) {
        link.close();
      }
      this.client.link(null);
    }

    enum InternalState {
      INITIAL,
      CONNECTED,
      RECONNECTING,
      REINITIALIZING,
      CLOSED
    }

    /**
     * Outcome of a transition: next state, next epoch, side effect. Unlike production, {@link
     * #effect()} is exposed so the pure tests can run it against a mock; production effects are
     * currently untested, which is worth carrying back.
     */
    static final class TransitionResult {

      private static final Consumer<ClientState> NO_OP = s -> {};

      private final InternalState state;
      private final long epoch;
      private final Consumer<ClientState> effect;

      private TransitionResult(InternalState state, long epoch, Consumer<ClientState> effect) {
        this.state = state;
        this.epoch = epoch;
        this.effect = effect;
      }

      private static TransitionResult noChange(InternalState state, long epoch) {
        return new TransitionResult(state, epoch, NO_OP);
      }

      private static TransitionResult of(
          InternalState state, long epoch, Consumer<ClientState> effect) {
        return new TransitionResult(state, epoch, effect);
      }

      InternalState state() {
        return this.state;
      }

      long epoch() {
        return this.epoch;
      }

      Consumer<ClientState> effect() {
        return this.effect;
      }
    }
  }

  /**
   * The entity. Holds the {@link EventLoop.Client} directly (no separate facade), so the loop
   * boundary is visible at each call site.
   */
  private static final class TestClient implements RecoverableClient {

    private static final int MAX_RECONNECT_ATTEMPTS = 5;

    private final FakeServer server;
    private final Executor recoveryExecutor;
    private final EventLoop.Client<ClientState> loopClient;
    private volatile Link link;

    TestClient(EventLoop eventLoop, FakeServer server, Executor recoveryExecutor) {
      this.server = server;
      this.recoveryExecutor = recoveryExecutor;
      this.loopClient = eventLoop.register(() -> new ClientState(this));
      long connectEpoch = this.loopClient.query(ClientState::epoch);
      this.link = this.server.connect();
      this.link.onDisconnect(cause -> this.handleDisconnect(connectEpoch, cause));
      this.loopClient.submit(ClientState::markConnected);
    }

    ClientState.InternalState internalState() {
      return this.loopClient.query(ClientState::internalState);
    }

    long epoch() {
      return this.loopClient.query(ClientState::epoch);
    }

    boolean isClosed() {
      return this.loopClient.isClosed();
    }

    @Override
    public Link link() {
      return this.link;
    }

    @Override
    public void link(Link link) {
      this.link = link;
    }

    @Override
    public void abortPendingWork(Throwable cause) {
      // nothing pending to unblock in this illustration; the branch exists to show the
      // effect fires without also restarting recovery
    }

    @Override
    public void dispatchReconnect(long epoch) {
      // a real implementation would schedule the reconnection after a back-off period
      // (hence the "dispatch" in the method name
      RuntimeException lastFailure = null;
      for (int attempt = 0; attempt < MAX_RECONNECT_ATTEMPTS; attempt++) {
        try {
          Link newLink = this.server.connect();
          if (this.loopClient.isClosed()) {
            // the client was closed while this attempt was in flight; nobody will ever
            // read this link, so it must be closed here instead of through the FSM
            newLink.close();
            return;
          }
          newLink.onDisconnect(cause -> this.handleDisconnect(epoch, cause));
          this.loopClient.submit(state -> state.handleReconnectSuccess(epoch, newLink));
          return;
        } catch (RecoverableFailure e) {
          lastFailure = e;
        }
      }
      this.close(lastFailure);
    }

    @Override
    public void reinitialize(long epoch) {
      // simulate the entity recovery step (publishers, subscribers, etc)
      try {
        this.server.initialize();
        this.loopClient.submit(state -> state.handleReinitializeSuccess(epoch));
      } catch (RecoverableFailure e) {
        this.loopClient.submit(state -> state.handleReinitializeFailure(epoch));
      }
    }

    @Override
    public void close(Throwable cause) {
      this.loopClient.submit(state -> state.markClosed(cause));
      Link current = this.link;
      if (current != null) {
        current.close();
      }
      this.loopClient.close();
    }

    @Override
    public void submitRecoveryTask(Runnable task) {
      this.recoveryExecutor.execute(task);
    }

    private void handleDisconnect(long eventEpoch, Throwable cause) {
      this.loopClient.submit(state -> state.handleDisconnect(eventEpoch, cause));
    }
  }

  /**
   * Queues recovery tasks for the test to run explicitly with {@link #runNext()} or {@link
   * #drain()}. This is the testability move that makes the off-loop boundary controllable, so every
   * lifecycle test is deterministic with no sleeps. Tasks run on the calling (test) thread and hop
   * back onto the loop via {@code EventLoop.Client}, so there is no deadlock.
   */
  private static final class ManualExecutor implements Executor {

    private final Queue<Runnable> pending = new ConcurrentLinkedQueue<>();

    @Override
    public void execute(Runnable command) {
      this.pending.add(command);
    }

    void runNext() {
      Runnable task = this.pending.poll();
      if (task != null) {
        task.run();
      }
    }

    void drain() {
      Runnable task;
      while ((task = this.pending.poll()) != null) {
        task.run();
      }
    }

    int pending() {
      return this.pending.size();
    }
  }

  /** Not every failure is worth recovering from; only this one is. */
  private static final class RecoverableFailure extends RuntimeException {
    RecoverableFailure(String message) {
      super(message);
    }
  }
}
