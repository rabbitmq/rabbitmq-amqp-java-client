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

import com.rabbitmq.client.amqp.AmqpException;
import org.jetbrains.jetCheck.Generator;
import org.jetbrains.jetCheck.PropertyChecker;
import org.junit.jupiter.api.Test;

class ConnectionStateTransitionsTest {

  private static final Generator<ConnectionStateClient.InternalState> STATES =
      Generator.sampledFrom(ConnectionStateClient.InternalState.values());

  private static final Generator<Long> EPOCHS = Generator.integers(1, 1_000_000).map(i -> (long) i);

  private static final Generator<AmqpException> EXCEPTIONS =
      Generator.booleans()
          .map(recoverable ->
              recoverable
                  ? new AmqpException.AmqpConnectionException("simulated failure", null)
                  : new AmqpException("simulated failure"));

  @Test
  void disconnectNeverDecreasesEpoch() {
    Generator<DisconnectInput> inputs =
        Generator.from(data ->
            new DisconnectInput(
                data.generate(STATES),
                data.generate(EPOCHS),
                data.generate(EPOCHS),
                data.generate(EXCEPTIONS),
                data.generate(Generator.booleans())));

    PropertyChecker.forAll(
        inputs,
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onDisconnect(
                  input.state,
                  input.epoch,
                  input.eventEpoch,
                  true,
                  input.exception,
                  input.recoveryActivated,
                  "test-connection");
          return result.epoch() >= input.epoch;
        });
  }

  @Test
  void staleDisconnectIsANoOp() {
    Generator<DisconnectInput> staleInputs =
        Generator.from(data -> {
          long epoch = data.generate(EPOCHS);
          long delta = data.generate(Generator.integers(1, 1000).map(i -> (long) i));
          return new DisconnectInput(
              data.generate(STATES),
              epoch,
              epoch - delta,
              data.generate(EXCEPTIONS),
              data.generate(Generator.booleans()));
        });

    PropertyChecker.forAll(
        staleInputs,
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onDisconnect(
                  input.state,
                  input.epoch,
                  input.eventEpoch,
                  true,
                  input.exception,
                  input.recoveryActivated,
                  "test-connection");
          return result.state() == input.state && result.epoch() == input.epoch;
        });
  }

  @Test
  void sessionFailureNeverDecreasesEpoch() {
    Generator<DisconnectInput> inputs =
        Generator.from(data ->
            new DisconnectInput(
                data.generate(STATES),
                data.generate(EPOCHS),
                data.generate(EPOCHS),
                data.generate(EXCEPTIONS),
                data.generate(Generator.booleans())));

    PropertyChecker.forAll(
        inputs,
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onSessionFailure(
                  input.state, input.epoch, input.eventEpoch, input.exception, "test-connection");
          return result.epoch() >= input.epoch;
        });
  }

  @Test
  void sessionFailureFromOtherEpochIsANoOp() {
    Generator<DisconnectInput> inputs =
        Generator.from(data -> {
          long epoch = data.generate(EPOCHS);
          long delta = data.generate(Generator.integers(1, 1000).map(i -> (long) i));
          long eventEpoch = data.generate(Generator.booleans()) ? epoch - delta : epoch + delta;
          return new DisconnectInput(
              data.generate(STATES),
              epoch,
              eventEpoch,
              data.generate(EXCEPTIONS),
              data.generate(Generator.booleans()));
        });

    PropertyChecker.forAll(
        inputs,
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onSessionFailure(
                  input.state, input.epoch, input.eventEpoch, input.exception, "test-connection");
          return result.state() == input.state && result.epoch() == input.epoch;
        });
  }

  @Test
  void sessionFailureOfCurrentEpochStartsRecoveryUnlessClosedOrAlreadyRecovering() {
    Generator<DisconnectInput> inputs =
        Generator.from(data -> {
          long epoch = data.generate(EPOCHS);
          return new DisconnectInput(
              data.generate(STATES),
              epoch,
              epoch,
              data.generate(EXCEPTIONS),
              data.generate(Generator.booleans()));
        });

    PropertyChecker.forAll(
        inputs,
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onSessionFailure(
                  input.state, input.epoch, input.eventEpoch, input.exception, "test-connection");
          if (input.state == ConnectionStateClient.InternalState.CLOSED
              || input.state == ConnectionStateClient.InternalState.RECOVERING_CONNECTION) {
            return result.state() == input.state && result.epoch() == input.epoch;
          } else {
            return result.state() == ConnectionStateClient.InternalState.RECOVERING_CONNECTION
                && result.epoch() == input.epoch + 1;
          }
        });
  }

  @Test
  void disconnectOfCurrentEpochRecoversOnlyIfConnectedAndRecoverable() {
    Generator<DisconnectInput> inputs =
        Generator.from(data -> {
          long epoch = data.generate(EPOCHS);
          return new DisconnectInput(
              data.generate(STATES),
              epoch,
              epoch,
              data.generate(EXCEPTIONS),
              data.generate(Generator.booleans()));
        });

    PropertyChecker.forAll(
        inputs,
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onDisconnect(
                  input.state,
                  input.epoch,
                  input.eventEpoch,
                  true,
                  input.exception,
                  input.recoveryActivated,
                  "test-connection");
          boolean recovers =
              (input.state == ConnectionStateClient.InternalState.CONNECTED
                      || input.state == ConnectionStateClient.InternalState.RECOVERING_TOPOLOGY)
                  && input.recoveryActivated
                  && input.exception instanceof AmqpException.AmqpConnectionException;
          if (recovers) {
            return result.state() == ConnectionStateClient.InternalState.RECOVERING_CONNECTION
                && result.epoch() == input.epoch + 1;
          } else {
            return result.state() == input.state && result.epoch() == input.epoch;
          }
        });
  }

  @Test
  void disconnectOfOtherNativeConnectionIsANoOpUnlessRecoveringConnection() {
    Generator<DisconnectInput> inputs =
        Generator.from(data -> {
          long epoch = data.generate(EPOCHS);
          return new DisconnectInput(
              data.generate(STATES),
              epoch,
              epoch,
              data.generate(EXCEPTIONS),
              data.generate(Generator.booleans()));
        });

    PropertyChecker.forAll(
        inputs,
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onDisconnect(
                  input.state,
                  input.epoch,
                  input.eventEpoch,
                  false,
                  input.exception,
                  input.recoveryActivated,
                  "test-connection");
          return result.state() == input.state && result.epoch() == input.epoch;
        });
  }

  @Test
  void initialOpenWithoutDisconnectIsConnected() {
    PropertyChecker.forAll(
        Generator.from(data ->
            new DisconnectInput(
                data.generate(STATES),
                data.generate(EPOCHS),
                0,
                null,
                data.generate(Generator.booleans()))),
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onInitialOpen(
                  input.state, input.epoch, null, input.recoveryActivated, "test-connection");
          if (input.state == ConnectionStateClient.InternalState.INITIAL) {
            return result.state() == ConnectionStateClient.InternalState.CONNECTED
                && result.epoch() == input.epoch;
          } else {
            return result.state() == input.state && result.epoch() == input.epoch;
          }
        });
  }

  @Test
  void initialOpenWithDisconnectRecoversOnlyIfRecoverable() {
    PropertyChecker.forAll(
        Generator.from(data ->
            new DisconnectInput(
                ConnectionStateClient.InternalState.INITIAL,
                data.generate(EPOCHS),
                0,
                data.generate(EXCEPTIONS),
                data.generate(Generator.booleans()))),
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onInitialOpen(
                  input.state,
                  input.epoch,
                  input.exception,
                  input.recoveryActivated,
                  "test-connection");
          if (input.recoveryActivated
              && input.exception instanceof AmqpException.AmqpConnectionException) {
            return result.state() == ConnectionStateClient.InternalState.RECOVERING_CONNECTION
                && result.epoch() == input.epoch + 1;
          } else {
            // the connection is closed by the transition effect
            return result.state() == ConnectionStateClient.InternalState.CONNECTED
                && result.epoch() == input.epoch;
          }
        });
  }

  @Test
  void creationFailureClosesAndFencesLaterEvents() {
    PropertyChecker.forAll(
        Generator.from(data ->
            new DisconnectInput(
                ConnectionStateClient.InternalState.INITIAL,
                data.generate(EPOCHS),
                0,
                data.generate(EXCEPTIONS),
                data.generate(Generator.booleans()))),
        input -> {
          ConnectionStateClient.TransitionResult closed =
              ConnectionStateClient.ConnectionState.onCreationFailure(input.epoch);
          if (closed.state() != ConnectionStateClient.InternalState.CLOSED
              || closed.epoch() != input.epoch + 1) {
            return false;
          }
          for (long eventEpoch : new long[] {input.epoch, closed.epoch()}) {
            for (boolean fromCurrentNativeConnection : new boolean[] {true, false}) {
              ConnectionStateClient.TransitionResult disconnect =
                  ConnectionStateClient.ConnectionState.onDisconnect(
                      closed.state(),
                      closed.epoch(),
                      eventEpoch,
                      fromCurrentNativeConnection,
                      input.exception,
                      input.recoveryActivated,
                      "test-connection");
              if (disconnect.state() != closed.state() || disconnect.epoch() != closed.epoch()) {
                return false;
              }
            }
            ConnectionStateClient.TransitionResult sessionFailure =
                ConnectionStateClient.ConnectionState.onSessionFailure(
                    closed.state(), closed.epoch(), eventEpoch, input.exception, "test-connection");
            if (sessionFailure.state() != closed.state()
                || sessionFailure.epoch() != closed.epoch()) {
              return false;
            }
          }
          ConnectionStateClient.TransitionResult initialOpen =
              ConnectionStateClient.ConnectionState.onInitialOpen(
                  closed.state(),
                  closed.epoch(),
                  input.exception,
                  input.recoveryActivated,
                  "test-connection");
          return initialOpen.state() == closed.state() && initialOpen.epoch() == closed.epoch();
        });
  }

  private static final class DisconnectInput {
    final ConnectionStateClient.InternalState state;
    final long epoch;
    final long eventEpoch;
    final AmqpException exception;
    final boolean recoveryActivated;

    DisconnectInput(
        ConnectionStateClient.InternalState state,
        long epoch,
        long eventEpoch,
        AmqpException exception,
        boolean recoveryActivated) {
      this.state = state;
      this.epoch = epoch;
      this.eventEpoch = eventEpoch;
      this.exception = exception;
      this.recoveryActivated = recoveryActivated;
    }
  }
}
