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
package com.rabbitmq.client.amqp;

import java.time.Duration;

public interface BackOffDelayPolicy {

  Duration TIMEOUT = Duration.ofMillis(Long.MAX_VALUE);

  Duration delay(int recoveryAttempt);

  static BackOffDelayPolicy fixedWithInitialDelay(Duration initialDelay, Duration delay) {
    return new FixedWithInitialDelayBackOffPolicy(initialDelay, delay);
  }

  static BackOffDelayPolicy fixedWithInitialDelay(
      Duration initialDelay, Duration delay, Duration timeout) {
    return new FixedWithInitialDelayAndTimeoutBackOffPolicy(initialDelay, delay, timeout);
  }

  static BackOffDelayPolicy fixed(Duration delay) {
    return new FixedWithInitialDelayBackOffPolicy(delay, delay);
  }

  final class FixedWithInitialDelayBackOffPolicy implements BackOffDelayPolicy {

    private final Duration initialDelay;
    private final Duration delay;

    private FixedWithInitialDelayBackOffPolicy(Duration initialDelay, Duration delay) {
      this.initialDelay = initialDelay;
      this.delay = delay;
    }

    @Override
    public Duration delay(int recoveryAttempt) {
      return recoveryAttempt == 0 ? initialDelay : delay;
    }

    @Override
    public String toString() {
      return "FixedWithInitialDelayBackOffPolicy{"
          + "initialDelay="
          + initialDelay
          + ", delay="
          + delay
          + '}';
    }
  }

  final class FixedWithInitialDelayAndTimeoutBackOffPolicy implements BackOffDelayPolicy {

    private final int attemptLimitBeforeTimeout;
    private final BackOffDelayPolicy delegate;

    private FixedWithInitialDelayAndTimeoutBackOffPolicy(
        Duration initialDelay, Duration delay, Duration timeout) {
      this(fixedWithInitialDelay(initialDelay, delay), timeout);
    }

    private FixedWithInitialDelayAndTimeoutBackOffPolicy(
        BackOffDelayPolicy policy, Duration timeout) {
      if (timeout.toMillis() < policy.delay(0).toMillis()) {
        throw new IllegalArgumentException("Timeout must be longer than initial delay");
      }
      this.delegate = policy;
      // best effort, assume FixedWithInitialDelay-ish policy
      Duration initialDelay = policy.delay(0);
      Duration delay = policy.delay(1);
      long timeoutWithInitialDelay = timeout.toMillis() - initialDelay.toMillis();
      this.attemptLimitBeforeTimeout = (int) (timeoutWithInitialDelay / delay.toMillis()) + 1;
    }

    @Override
    public Duration delay(int recoveryAttempt) {
      if (recoveryAttempt >= attemptLimitBeforeTimeout) {
        return TIMEOUT;
      } else {
        return delegate.delay(recoveryAttempt);
      }
    }

    @Override
    public String toString() {
      return "FixedWithInitialDelayAndTimeoutBackOffPolicy{"
          + "attemptLimitBeforeTimeout="
          + attemptLimitBeforeTimeout
          + ", delegate="
          + delegate
          + '}';
    }
  }
}
