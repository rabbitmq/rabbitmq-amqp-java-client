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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TokenCredentials implements Credentials {

  private static final Logger LOGGER = LoggerFactory.getLogger(TokenCredentials.class);

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
    // TODO use strategy to tell if the token expires soon
    return t.expirationTime() < System.currentTimeMillis() - 20_000;
  }

  private Duration delayBeforeTokenRenewal(Token token) {
    long expiresIn = token.expirationTime() - System.currentTimeMillis();
    // TODO use strategy to decide when to renew token
    long delay = (long) (expiresIn * 0.8);
    return Duration.ofMillis(delay);
  }

  private Token getToken() {
    LOGGER.debug("Requesting new token...");
    Utils.StopWatch stopWatch = new Utils.StopWatch();
    Token token = requester.request();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Got new token in {} ms, token expires on {}",
          stopWatch.stop().toMillis(),
          format(token.expirationTime()));
    }
    return token;
  }

  @Override
  public Registration register(RefreshCallback refreshCallback) {
    Long id = this.registrationSequence.getAndIncrement();
    RegistrationImpl registration = new RegistrationImpl(id, refreshCallback);
    this.registrations.put(id, registration);
    return registration;
  }

  private void refreshRegistrations(Token t) {
    this.scheduledExecutorService.execute(
        () -> {
          LOGGER.debug("Refreshing {} registration(s)", this.registrations.size());
          int refreshedCount = 0;
          for (RegistrationImpl registration : this.registrations.values()) {
            if (t.equals(this.token)) {
              if (!registration.isClosed() && !registration.hasSameToken(t)) {
                // the registration does not have the new token yet
                registration.refreshCallback.refresh("", this.token.value());
                registration.registrationToken = this.token;
                refreshedCount++;
              }
            }
          }
          LOGGER.debug("Refreshed {} registration(s)", refreshedCount);
        });
  }

  private void token(Token t) {
    lock();
    try {
      if (!t.equals(this.token)) {
        this.token = t;
        scheduleRenewal(t);
      }
    } finally {
      unlock();
    }
  }

  private void scheduleRenewal(Token t) {
    if (this.schedulingRenewal.compareAndSet(false, true)) {
      if (this.renewalTask != null) {
        this.renewalTask.cancel(false);
      }
      Duration delay = delayBeforeTokenRenewal(t);
      if (delay.isZero() || delay.isNegative()) {
        delay = Duration.ofSeconds(1);
      }
      if (!this.registrations.isEmpty()) {
        LOGGER.debug("Scheduling token retrieval in {}", delay);
        this.renewalTask =
            this.scheduledExecutorService.schedule(
                () -> {
                  Token previousToken = this.token;
                  this.lock();
                  try {
                    if (this.token.equals(previousToken)) {
                      Token newToken = getToken();
                      token(newToken);
                      refreshRegistrations(newToken);
                    }
                  } finally {
                    unlock();
                  }
                },
                delay.toMillis(),
                TimeUnit.MILLISECONDS);
      } else {
        this.renewalTask = null;
      }
      this.schedulingRenewal.set(false);
    }
  }

  private static String format(long timestampMs) {
    return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(timestampMs));
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
      Token tokenToUse;
      lock();
      try {
        if (token == null) {
          token(getToken());
        } else if (expiresSoon(token)) {
          shouldRefresh = true;
          token(getToken());
        }
        this.registrationToken = token;
        tokenToUse = this.registrationToken;
        if (renewalTask == null) {
          scheduleRenewal(tokenToUse);
        }
      } finally {
        unlock();
      }

      callback.username("").password(tokenToUse.value());
      if (shouldRefresh) {
        refreshRegistrations(tokenToUse);
      }
    }

    @Override
    public void unregister() {
      if (this.closed.compareAndSet(false, true)) {
        registrations.remove(this.id);
        ScheduledFuture<?> task = renewalTask;
        if (registrations.isEmpty() && task != null) {
          lock();
          try {
            if (renewalTask != null) {
              renewalTask.cancel(false);
            }
          } finally {
            unlock();
          }
        }
      }
    }

    private boolean hasSameToken(Token t) {
      return t.equals(this.registrationToken);
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
