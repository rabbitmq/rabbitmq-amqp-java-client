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

import static com.rabbitmq.client.amqp.Resource.State.CLOSING;
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.Resource.State.RECOVERING;
import static com.rabbitmq.client.amqp.impl.AmqpConnection.RECOVERY_PREDICATE;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Requester;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.Responder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.qpid.protonj2.client.DisconnectionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectionStateClient implements AutoCloseable {

  private final EventLoop.Client<ConnectionState> client;

  ConnectionStateClient(EventLoop eventLoop, RecoverableConnection connection) {
    this.client = eventLoop.register(() -> new ConnectionState(connection));
  }

  void executeInLoop(Runnable runnable) {
    this.client.submit(unused -> runnable.run());
  }

  <R> R executeInLoop(Supplier<R> runnable) {
    return this.client.query(unused -> runnable.get());
  }

  long epoch() {
    return this.client.query(ConnectionState::epoch);
  }

  List<AmqpConsumer> consumers() {
    return this.client.query(state -> new ArrayList<>(state.consumers));
  }

  List<AmqpPublisher> publishers() {
    return this.client.query(state -> new ArrayList<>(state.publishers));
  }

  List<Requester> requesters() {
    return this.client.query(state -> new ArrayList<>(state.requesters));
  }

  List<Responder> responders() {
    return this.client.query(state -> new ArrayList<>(state.responders));
  }

  // connection-state-related methods

  boolean registerPublisher(AmqpPublisher publisher) {
    return this.client.query(
        state -> {
          if (state.connection.state() != OPEN) {
            return false;
          }
          state.publishers.add(publisher);
          return true;
        });
  }

  void removePublisher(AmqpPublisher publisher) {
    this.client.submit(state -> state.publishers.remove(publisher));
  }

  boolean registerConsumer(AmqpConsumer consumer) {
    return this.client.query(
        state -> {
          if (state.connection.state() != OPEN) {
            return false;
          }
          state.consumers.add(consumer);
          return true;
        });
  }

  void removeConsumer(AmqpConsumer consumer) {
    this.client.submit(state -> state.consumers.remove(consumer));
  }

  boolean registerRequester(Requester requester) {
    return this.client.query(
        state -> {
          if (state.connection.state() != OPEN) {
            return false;
          }
          state.requesters.add(requester);
          return true;
        });
  }

  void removeRequester(Requester requester) {
    this.client.submit(state -> state.requesters.remove(requester));
  }

  boolean registerResponder(Responder responder) {
    return this.client.query(
        state -> {
          if (state.connection.state() != OPEN) {
            return false;
          }
          state.responders.add(responder);
          return true;
        });
  }

  void removeResponder(Responder responder) {
    this.client.submit(state -> state.responders.remove(responder));
  }

  void handleDisconnect(long attemptEpoch, DisconnectionEvent event) {
    this.client.submit(state -> state.handleDisconnect(attemptEpoch, event));
  }

  void handleNativeRecoverySuccess(AmqpConnection.NativeConnectionWrapper ncw, long attemptEpoch) {
    this.client.submit(state -> state.handleNativeRecoverySuccess(ncw, attemptEpoch));
  }

  void handleTopologyRecoverySuccess(long attemptEpoch) {
    this.client.submit(state -> state.handleTopologyRecoverySuccess(attemptEpoch));
  }

  void handleTopologyRecoveryFailure(long attemptEpoch) {
    this.client.submit(state -> state.handleTopologyRecoveryFailure(attemptEpoch));
  }

  void markClosed(Throwable cause) {
    this.client.submit(s -> s.markClosed(cause));
  }

  @Override
  public void close() throws Exception {
    this.client.close();
  }

  private static class ConnectionState {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionState.class);

    private InternalState internalState = InternalState.INITIAL;
    private long epoch = 1;
    private final RecoverableConnection connection;

    private final List<AmqpPublisher> publishers = new ArrayList<>();
    private final List<AmqpConsumer> consumers = new ArrayList<>();
    private final List<Requester> requesters = new ArrayList<>();
    private final List<Responder> responders = new ArrayList<>();

    private ConnectionState(RecoverableConnection connection) {
      this.connection = connection;
    }

    long epoch() {
      return this.epoch;
    }

    void handleDisconnect(long eventEpoch, DisconnectionEvent event) {
      AmqpException exception = ExceptionUtils.convert(event.failureCause());
      apply(
          onDisconnect(
              this.internalState, this.epoch, eventEpoch, exception, this.connection.name()));
    }

    void handleNativeRecoverySuccess(
        AmqpConnection.NativeConnectionWrapper ncw, long attemptEpoch) {
      apply(
          onNativeRecoverySuccess(
              this.internalState, this.epoch, attemptEpoch, ncw, this.connection.name()));
    }

    void handleTopologyRecoverySuccess(long attemptEpoch) {
      apply(
          onTopologyRecoverySuccess(
              this.internalState, this.epoch, attemptEpoch, this.connection.name()));
    }

    void handleTopologyRecoveryFailure(long attemptEpoch) {
      apply(onTopologyRecoveryFailure(this.internalState, this.epoch, attemptEpoch));
    }

    // A helper method for safely marking the internal state closed from AmqpConnection.close()
    void markClosed(Throwable cause) {
      apply(onMarkClosed(this.epoch, cause));
    }

    // Pure decision logic: no I/O, no access to `connection` or the resource lists, so it can be
    // unit-tested (including property-based) without an EventLoop, a fake, or a real connection.

    private static TransitionResult onDisconnect(
        InternalState state,
        long epoch,
        long eventEpoch,
        AmqpException exception,
        String connectionName) {

      if (eventEpoch < epoch) {
        LOGGER.debug(
            "Ignoring stale disconnect from epoch {} for connection {}",
            eventEpoch,
            connectionName);
        return TransitionResult.noChange(state, epoch);
      }

      if (state == InternalState.CLOSED) {
        return TransitionResult.noChange(state, epoch);
      }

      if (!RECOVERY_PREDICATE.test(exception)) {
        return TransitionResult.of(state, epoch, s -> s.connection.close(exception));
      }

      if (state == InternalState.RECOVERING_CONNECTION) {
        LOGGER.debug(
            "Mid-native-recovery disconnect for epoch {}. AsyncRetry will handle it.", eventEpoch);
        // this will unblock pending RPCs
        return TransitionResult.of(
            state, epoch, s -> s.connection.releaseManagementResources(exception));
      }

      // If we are in CONNECTED or RECOVERING_TOPOLOGY, a disconnect is a hard failure.
      // We must transition back to RECOVERING_CONNECTION and start over.
      LOGGER.debug("Valid disconnect detected, initiating recovery...");
      long newEpoch = epoch + 1;
      return TransitionResult.of(
          InternalState.RECOVERING_CONNECTION,
          newEpoch,
          // Synchronize public state safely inside the loop
          s -> s.connection.updateState(RECOVERING, exception),
          s -> s.updateStateOfResources(RECOVERING, exception),
          s -> s.connection.releaseManagementResources(exception),
          // Null out native resources
          s -> s.connection.resetNativeResources(),
          // Dispatch Phase 1: Native Recovery
          s ->
              s.connection.submitRecoveryTask(() -> s.connection.dispatchNativeRecovery(newEpoch)));
    }

    private static TransitionResult onNativeRecoverySuccess(
        InternalState state,
        long epoch,
        long attemptEpoch,
        AmqpConnection.NativeConnectionWrapper ncw,
        String connectionName) {

      if (isStale(state, epoch, attemptEpoch)) {
        // We are a zombie thread. Tear down the socket we just created to avoid leaks.
        return TransitionResult.of(state, epoch, s -> s.closeZombieConnection(ncw));
      }

      LOGGER.debug("Reconnected '{}' to {}", connectionName, ncw.address());
      return TransitionResult.of(
          InternalState.RECOVERING_TOPOLOGY,
          epoch,
          s -> s.connection.sync(ncw),
          // Dispatch Phase 2: Topology Recovery
          s ->
              s.connection.submitRecoveryTask(
                  () -> s.connection.dispatchTopologyRecovery(attemptEpoch)));
    }

    private static TransitionResult onTopologyRecoverySuccess(
        InternalState state, long epoch, long attemptEpoch, String connectionName) {
      if (isStale(state, epoch, attemptEpoch)) {
        return TransitionResult.noChange(state, epoch);
      }

      LOGGER.info("Recovered topology for connection '{}'", connectionName);
      return TransitionResult.of(
          InternalState.CONNECTED, epoch, s -> s.connection.updateState(OPEN, null));
    }

    private static TransitionResult onTopologyRecoveryFailure(
        InternalState state, long epoch, long attemptEpoch) {
      if (isStale(state, epoch, attemptEpoch)) {
        return TransitionResult.noChange(state, epoch);
      }

      LOGGER.debug(
          "Error during topology recovery, tearing down native connection to trigger clean retry.");
      long newEpoch = epoch + 1;
      return TransitionResult.of(
          InternalState.RECOVERING_CONNECTION,
          newEpoch,
          s -> s.tearDownFailedNativeConnection(),
          s -> s.connection.resetNativeResources(),
          s ->
              s.connection.submitRecoveryTask(() -> s.connection.dispatchNativeRecovery(newEpoch)));
    }

    private static TransitionResult onMarkClosed(long epoch, Throwable cause) {
      // Invalidate any pending IO tasks instantly
      return TransitionResult.of(
          InternalState.CLOSED, epoch + 1, s -> s.connection.updateState(CLOSING, cause));
    }

    private static boolean isStale(InternalState state, long epoch, long attemptEpoch) {
      return epoch != attemptEpoch || state == InternalState.CLOSED;
    }

    // Imperative shell: applies a decision, running its effects against the real connection.

    private void apply(TransitionResult result) {
      this.internalState = result.state;
      this.epoch = result.epoch;
      result.effects.forEach(effect -> effect.accept(this));
    }

    private void closeZombieConnection(AmqpConnection.NativeConnectionWrapper ncw) {
      try {
        ncw.connection().close();
      } catch (Exception e) {
        LOGGER.debug(
            "Error while closing native connection in 'handleNativeRecoverySuccess': {}",
            e.getMessage());
      }
    }

    private void tearDownFailedNativeConnection() {
      try {
        org.apache.qpid.protonj2.client.Connection nc = connection.nativeConnection();
        if (nc != null) {
          // best effort to close failed connection
          // using async variant to avoid blocking the event loop
          nc.closeAsync();
        }
      } catch (Exception e) {
        LOGGER.debug(
            "Error while closing native connection in 'handleTopologyRecoveryFailure': {}",
            e.getMessage());
      }
    }

    private void updateStateOfResources(Resource.State newState, Throwable failure) {
      this.publishers.forEach(r -> r.state(newState, failure));
      this.consumers.forEach(r -> r.state(newState, failure));
    }
  }

  private enum InternalState {
    INITIAL,
    CONNECTED,
    RECOVERING_CONNECTION,
    RECOVERING_TOPOLOGY,
    CLOSED
  }

  // The outcome of a pure transition: the next internal state/epoch, and the side effects (if
  // any) the imperative shell must run to make it so.
  private static final class TransitionResult {

    private final InternalState state;
    private final long epoch;
    private final List<Consumer<ConnectionState>> effects;

    private TransitionResult(
        InternalState state, long epoch, List<Consumer<ConnectionState>> effects) {
      this.state = state;
      this.epoch = epoch;
      this.effects = effects;
    }

    private static TransitionResult noChange(InternalState state, long epoch) {
      return new TransitionResult(state, epoch, Collections.emptyList());
    }

    @SafeVarargs
    private static TransitionResult of(
        InternalState state, long epoch, Consumer<ConnectionState>... effects) {
      return new TransitionResult(state, epoch, Arrays.asList(effects));
    }
  }

  // Narrow view of AmqpConnection driven by ConnectionStateClient, so the state machine can be
  // exercised with a test double instead of a real (network-opening) AmqpConnection.
  interface RecoverableConnection {

    Resource.State state();

    String name();

    void updateState(Resource.State state, Throwable cause);

    void releaseManagementResources(AmqpException e);

    void resetNativeResources();

    void sync(AmqpConnection.NativeConnectionWrapper wrapper);

    org.apache.qpid.protonj2.client.Connection nativeConnection();

    void dispatchNativeRecovery(long attemptEpoch);

    void dispatchTopologyRecovery(long attemptEpoch);

    void close(Throwable cause);

    void submitRecoveryTask(Runnable task);
  }
}
