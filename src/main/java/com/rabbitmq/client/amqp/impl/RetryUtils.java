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

import static java.lang.String.format;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class RetryUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryUtils.class);

  private RetryUtils() {}

  static <T> T callAndMaybeRetry(
      Callable<T> operation,
      Predicate<Exception> retryCondition,
      List<Duration> waitTimes,
      String format,
      Object... args) {
    return callAndMaybeRetry(
        operation,
        retryCondition,
        i -> i > waitTimes.size() ? BackOffDelayPolicy.TIMEOUT : waitTimes.get(i - 1),
        format,
        args);
  }

  static <T> T callAndMaybeRetry(
      Callable<T> operation,
      Predicate<Exception> retryCondition,
      BackOffDelayPolicy delayPolicy,
      String format,
      Object... args) {
    String description = format(format, args);
    int attempt = 0;
    Exception lastException = null;
    long startTime = System.nanoTime();
    boolean keepTrying = true;
    while (keepTrying) {
      try {
        attempt++;
        LOGGER.debug("Starting attempt #{} for operation '{}'", attempt, description);
        T result = operation.call();
        Duration operationDuration = Duration.ofNanos(System.nanoTime() - startTime);
        LOGGER.debug(
            "Operation '{}' completed in {} ms after {} attempt(s)",
            description,
            operationDuration.toMillis(),
            attempt);
        return result;
      } catch (Exception e) {
        lastException = e;
        if (retryCondition.test(e)) {
          LOGGER.debug("Operation '{}' failed, retrying...", description);
          Duration delay = delayPolicy.delay(attempt);
          if (BackOffDelayPolicy.TIMEOUT.equals(delay)) {
            keepTrying = false;
          } else if (!delay.isZero()) {
            try {
              Thread.sleep(delay.toMillis());
            } catch (InterruptedException ex) {
              Thread.interrupted();
              lastException = ex;
              keepTrying = false;
            }
          }
        } else {
          keepTrying = false;
        }
      }
    }
    String message =
        format(
            "Could not complete task '%s' after %d attempt(s) (reason: %s)",
            description, attempt, exceptionMessage(lastException));
    LOGGER.debug(message);
    if (lastException instanceof RuntimeException) {
      throw (RuntimeException) lastException;
    } else {
      throw new AmqpException(message, lastException);
    }
  }

  static String exceptionMessage(Exception e) {
    if (e == null) {
      return "unknown";
    } else if (e.getMessage() == null) {
      return e.getClass().getSimpleName();
    } else {
      return e.getMessage() + " [" + e.getClass().getSimpleName() + "]";
    }
  }
}
