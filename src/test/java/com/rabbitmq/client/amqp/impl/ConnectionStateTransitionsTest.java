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
          .map(
              recoverable ->
                  recoverable
                      ? new AmqpException.AmqpConnectionException("simulated failure", null)
                      : new AmqpException("simulated failure"));

  @Test
  void disconnectNeverDecreasesEpoch() {
    Generator<DisconnectInput> inputs =
        Generator.from(
            data ->
                new DisconnectInput(
                    data.generate(STATES),
                    data.generate(EPOCHS),
                    data.generate(EPOCHS),
                    data.generate(EXCEPTIONS)));

    PropertyChecker.forAll(
        inputs,
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onDisconnect(
                  input.state, input.epoch, input.eventEpoch, input.exception, "test-connection");
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
                  data.generate(STATES), epoch, epoch - delta, data.generate(EXCEPTIONS));
            });

    PropertyChecker.forAll(
        staleInputs,
        input -> {
          ConnectionStateClient.TransitionResult result =
              ConnectionStateClient.ConnectionState.onDisconnect(
                  input.state, input.epoch, input.eventEpoch, input.exception, "test-connection");
          return result.state() == input.state && result.epoch() == input.epoch;
        });
  }

  private static final class DisconnectInput {
    final ConnectionStateClient.InternalState state;
    final long epoch;
    final long eventEpoch;
    final AmqpException exception;

    DisconnectInput(
        ConnectionStateClient.InternalState state,
        long epoch,
        long eventEpoch,
        AmqpException exception) {
      this.state = state;
      this.epoch = epoch;
      this.eventEpoch = eventEpoch;
      this.exception = exception;
    }
  }
}
