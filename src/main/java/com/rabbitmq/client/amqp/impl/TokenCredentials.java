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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TokenCredentials implements Credentials {

  static Function<Instant, Duration> DEFAULT_REFRESH_DELAY_STRATEGY =
      ratioRefreshDelayStrategy(0.8f);
  private static final Logger LOGGER = LoggerFactory.getLogger(TokenCredentials.class);

  private final TokenRequester requester;
  private final ScheduledExecutorService scheduledExecutorService;
  private volatile Token token;
  private final Lock lock = new ReentrantLock();
  private final Map<Long, RegistrationImpl> registrations = new ConcurrentHashMap<>();
  private final AtomicLong registrationSequence = new AtomicLong(0);
  private final AtomicBoolean schedulingRefresh = new AtomicBoolean(false);
  private final Function<Instant, Duration> refreshDelayStrategy;
  private volatile ScheduledFuture<?> refreshTask;

  TokenCredentials(
      TokenRequester requester,
      ScheduledExecutorService scheduledExecutorService,
      Function<Instant, Duration> refreshDelayStrategy) {
    this.requester = requester;
    this.scheduledExecutorService = scheduledExecutorService;
    this.refreshDelayStrategy = refreshDelayStrategy;
  }

  private void lock() {
    this.lock.lock();
  }

  private void unlock() {
    this.lock.unlock();
  }

  private boolean expiresSoon(Token ignores) {
    return false;
  }

  private Token getToken() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Requesting new token ({})...", registrationSummary(this.registrations.values()));
    }
    Utils.StopWatch stopWatch = new Utils.StopWatch();
    Token token = requester.request();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Got new token in {} ms, token expires on {} ({})",
          stopWatch.stop().toMillis(),
          format(token.expirationTime()),
          registrationSummary(this.registrations.values()));
    }
    return token;
  }

  @Override
  public Registration register(String name, AuthenticationCallback updateCallback) {
    Long id = this.registrationSequence.getAndIncrement();
    name = name == null ? id.toString() : name;
    RegistrationImpl registration = new RegistrationImpl(id, name, updateCallback);
    this.registrations.put(id, registration);
    return registration;
  }

  private void updateRegistrations(Token t) {
    this.scheduledExecutorService.execute(
        () -> {
          LOGGER.debug("Updating {} registration(s)", this.registrations.size());
          int refreshedCount = 0;
          for (RegistrationImpl registration : this.registrations.values()) {
            if (t.equals(this.token)) {
              if (!registration.isClosed() && !registration.hasSameToken(t)) {
                // the registration does not have the new token yet
                try {
                  registration.updateCallback().authenticate("", this.token.value());
                } catch (Exception e) {
                  LOGGER.warn(
                      "Error while updating token for registration '{}': {}",
                      registration.name(),
                      e.getMessage());
                }
                registration.registrationToken = this.token;
                refreshedCount++;
              } else {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(
                      "Not updating registration {} (closed or already has the new token)",
                      registration.name());
                }
              }
            } else {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "Not updating registration {} (the token has changed)", registration.name());
              }
            }
          }
          LOGGER.debug("Updated {} registration(s)", refreshedCount);
        });
  }

  private void token(Token t) {
    lock();
    try {
      if (!t.equals(this.token)) {
        this.token = t;
        scheduleTokenRefresh(t);
      }
    } finally {
      unlock();
    }
  }

  private void scheduleTokenRefresh(Token t) {
    if (this.schedulingRefresh.compareAndSet(false, true)) {
      if (this.refreshTask != null) {
        this.refreshTask.cancel(false);
      }
      Duration delay = this.refreshDelayStrategy.apply(t.expirationTime());
      if (!this.registrations.isEmpty()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Scheduling token retrieval in {} ({})",
              delay,
              registrationSummary(this.registrations.values()));
        }
        this.refreshTask =
            this.scheduledExecutorService.schedule(
                () -> {
                  Token previousToken = this.token;
                  this.lock();
                  try {
                    if (this.token.equals(previousToken)) {
                      Token newToken = getToken();
                      token(newToken);
                      updateRegistrations(newToken);
                    }
                  } finally {
                    unlock();
                  }
                },
                delay.toMillis(),
                TimeUnit.MILLISECONDS);
      } else {
        this.refreshTask = null;
      }
      this.schedulingRefresh.set(false);
    }
  }

  private static String format(Instant instant) {
    return DateTimeFormatter.ISO_INSTANT.format(instant);
  }

  private final class RegistrationImpl implements Registration {

    private final Long id;
    private final String name;
    private final AuthenticationCallback updateCallback;
    private volatile Token registrationToken;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private RegistrationImpl(Long id, String name, AuthenticationCallback updateCallback) {
      this.id = id;
      this.name = name;
      this.updateCallback = updateCallback;
    }

    @Override
    public void connect(AuthenticationCallback callback) {
      boolean shouldRefresh = false;
      Token tokenToUse;
      lock();
      try {
        Token globalToken = token;
        if (globalToken == null) {
          token(getToken());
        } else if (expiresSoon(globalToken)) {
          shouldRefresh = true;
          token(getToken());
        }
        if (!token.equals(this.registrationToken)) {
          this.registrationToken = token;
        }
        tokenToUse = this.registrationToken;
        if (refreshTask == null) {
          scheduleTokenRefresh(tokenToUse);
        }
      } finally {
        unlock();
      }
      callback.authenticate("", tokenToUse.value());
      if (shouldRefresh) {
        updateRegistrations(tokenToUse);
      }
    }

    @Override
    public void unregister() {
      if (this.closed.compareAndSet(false, true)) {
        registrations.remove(this.id);
        ScheduledFuture<?> task = refreshTask;
        if (registrations.isEmpty() && task != null) {
          lock();
          try {
            if (refreshTask != null) {
              refreshTask.cancel(false);
            }
          } finally {
            unlock();
          }
        }
      }
    }

    private AuthenticationCallback updateCallback() {
      return this.updateCallback;
    }

    private String name() {
      return this.name;
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

    @Override
    public String toString() {
      return this.name();
    }
  }

  static Function<Instant, Duration> ratioRefreshDelayStrategy(float ratio) {
    return new RatioRefreshDelayStrategy(ratio);
  }

  private static class RatioRefreshDelayStrategy implements Function<Instant, Duration> {

    private final float ratio;

    @SuppressFBWarnings("CT_CONSTRUCTOR_THROW")
    private RatioRefreshDelayStrategy(float ratio) {
      if (ratio < 0 || ratio > 1) {
        throw new IllegalArgumentException("Ratio should be > 0 and <= 1: " + ratio);
      }
      this.ratio = ratio;
    }

    @Override
    public Duration apply(Instant expirationTime) {
      Duration expiresIn = Duration.between(Instant.now(), expirationTime);
      Duration delay;
      if (expiresIn.isZero() || expiresIn.isNegative()) {
        delay = Duration.ofSeconds(1);
      } else {
        delay = Duration.ofMillis((long) (expiresIn.toMillis() * ratio));
      }
      return delay;
    }
  }

  private static String registrationSummary(Collection<? extends Registration> registrations) {
    return registrations.stream().map(Registration::toString).collect(Collectors.joining(", "));
  }
}
