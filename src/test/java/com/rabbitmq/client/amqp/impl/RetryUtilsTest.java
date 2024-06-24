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

import static com.rabbitmq.client.amqp.impl.RetryUtils.callAndMaybeRetry;
import static java.time.Duration.ofMillis;
import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import com.rabbitmq.client.amqp.AmqpException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RetryUtilsTest {

  private static final Predicate<Exception> EXCEPTION_PREDICATE =
      ex ->
          ex instanceof AmqpException.AmqpResourceInvalidStateException
              && !(ex instanceof AmqpException.AmqpResourceClosedException);
  @Mock Callable<String> operation;

  @Test
  void callAndMaybeRetryShouldReturnResultWhenAttemptsNotExhausted() throws Exception {
    when(operation.call())
        .thenThrow(new AmqpException.AmqpResourceInvalidStateException(""))
        .thenThrow(new AmqpException.AmqpResourceInvalidStateException(""))
        .thenReturn("foo");
    List<Duration> waitTimes = of(ofMillis(1), ofMillis(1), ofMillis(1));
    assertThat(callAndMaybeRetry(operation, EXCEPTION_PREDICATE, waitTimes, "operation %s", "test"))
        .isEqualTo("foo");
    verify(operation, times(3)).call();
  }

  @Test
  void callAndMaybeRetryShouldFailAfterTooManyAttempts() throws Exception {
    when(operation.call()).thenThrow(new AmqpException.AmqpResourceInvalidStateException(""));
    List<Duration> waitTimes = of(ofMillis(1), ofMillis(1), ofMillis(1));
    assertThatThrownBy(
            () ->
                callAndMaybeRetry(
                    operation, EXCEPTION_PREDICATE, waitTimes, "operation %s", "test"))
        .isInstanceOf(AmqpException.AmqpResourceInvalidStateException.class);
    verify(operation, times(waitTimes.size() + 1)).call();
  }

  @Test
  void callAndMaybeRetryShouldFailIfUnexpectedException() throws Exception {
    when(operation.call()).thenThrow(new AmqpException.AmqpResourceClosedException(""));
    List<Duration> waitTimes = of(ofMillis(1), ofMillis(1), ofMillis(1));
    assertThatThrownBy(
            () ->
                callAndMaybeRetry(
                    operation, EXCEPTION_PREDICATE, waitTimes, "operation %s", "test"))
        .isInstanceOf(AmqpException.AmqpResourceClosedException.class);
    verify(operation, times(1)).call();
  }
}
