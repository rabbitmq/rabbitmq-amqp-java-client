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

import com.rabbitmq.client.amqp.oauth.Token;
import com.rabbitmq.client.amqp.oauth.TokenRequester;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class TokenCredentials implements Credentials {

  private final TokenRequester requester;
  private final ScheduledExecutorService scheduledExecutorService;
  private volatile Token token;
  private final Lock lock = new ReentrantLock();
  private final Map<Long, RegistrationImpl> registrations = new ConcurrentHashMap<>();
  private final AtomicLong registrationSequence = new AtomicLong(0);
  private final AtomicBoolean schedulingRenewal = new AtomicBoolean(false);
  private volatile ScheduledFuture<?> renewalTask;

  TokenCredentials(TokenRequester requester, ScheduledExecutorService scheduledExecutorService) {
    this.requester = requester;
    this.scheduledExecutorService = scheduledExecutorService;
  }

  private void lock() {
    this.lock.lock();
  }

  private void unlock() {
    this.lock.unlock();
  }

  private boolean expiresSoon(Token t) {
    return t.expirationTime() < System.currentTimeMillis() - 20_000;
  }

  private Duration delayBeforeTokenRenewal(Token token) {
    long expiresIn = token.expirationTime() - System.currentTimeMillis();
    long delay = (long) (expiresIn * 0.8);
    return Duration.ofMillis(delay);
  }

  private Token getToken() {
    return requester.request();
  }

  @Override
  public Registration register(RefreshCallback refreshCallback) {
    Long id = this.registrationSequence.getAndIncrement();
    RegistrationImpl registration = new RegistrationImpl(id, refreshCallback);
    this.registrations.put(id, registration);
    return registration;
  }

  private void refresh() {
    this.scheduledExecutorService.execute(
        () -> {
          for (RegistrationImpl registration : this.registrations.values()) {
            if (!registration.isClosed() && !this.token.equals(registration.registrationToken)) {
              // the registration does not have the new token yet
              registration.refreshCallback.refresh("", this.token.value());
              registration.registrationToken = this.token;
            }
          }
        });
  }

  private void token(Token t) {
    if (!t.equals(this.token)) {
      this.token = t;
      if (this.schedulingRenewal.compareAndSet(false, true)) {
        if (this.renewalTask != null) {
          this.renewalTask.cancel(false);
        }
        Duration delay = delayBeforeTokenRenewal(t);
        if (delay.isZero() || delay.isNegative()) {
          delay = Duration.ofSeconds(1);
        }
        // TODO check delay is > 0, schedule 1 second later at least
        this.renewalTask =
            this.scheduledExecutorService.schedule(
                () -> {
                  Token previousToken = this.token;
                  this.lock();
                  try {
                    if (this.token.equals(previousToken)) {
                      Token newToken = getToken();
                      token(newToken);
                      refresh();
                    }
                  } finally {
                    unlock();
                  }
                },
                delay.toMillis(),
                TimeUnit.MILLISECONDS);
        this.schedulingRenewal.set(false);
      }
    }
  }

  private final class RegistrationImpl implements Registration {

    private final Long id;
    private final RefreshCallback refreshCallback;
    private volatile Token registrationToken;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private RegistrationImpl(Long id, RefreshCallback refreshCallback) {
      this.id = id;
      this.refreshCallback = refreshCallback;
    }

    @Override
    public void connect(ConnectionCallback callback) {
      boolean shouldRefresh = false;
      lock();
      try {
        if (token == null) {
          token(getToken());
        } else if (expiresSoon(token)) {
          shouldRefresh = true;
          token(getToken());
        }
        this.registrationToken = token;
      } finally {
        unlock();
      }

      callback.username("").password(this.registrationToken.value());
      if (shouldRefresh) {
        refresh();
      }
    }

    @Override
    public void unregister() {
      if (this.closed.compareAndSet(false, true)) {
        registrations.remove(this.id);
      }
    }

    private boolean isClosed() {
      return this.closed.get();
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      RegistrationImpl that = (RegistrationImpl) o;
      return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id);
    }
  }
}
