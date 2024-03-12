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
package com.rabbitmq.model.amqp;

import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.annotation.*;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

  private static final Duration DEFAULT_CONDITION_TIMEOUT = Duration.ofSeconds(10);

  private TestUtils() {}

  @FunctionalInterface
  public interface CallableBooleanSupplier {
    boolean getAsBoolean() throws Exception;
  }

  public static Duration waitAtMost(CallableBooleanSupplier condition) throws Exception {
    return waitAtMost(DEFAULT_CONDITION_TIMEOUT, condition, null);
  }

  public static Duration waitAtMost(CallableBooleanSupplier condition, Supplier<String> message)
      throws Exception {
    return waitAtMost(DEFAULT_CONDITION_TIMEOUT, condition, message);
  }

  public static Duration waitAtMost(Duration timeout, CallableBooleanSupplier condition)
      throws Exception {
    return waitAtMost(timeout, condition, null);
  }

  public static Duration waitAtMost(int timeoutInSeconds, CallableBooleanSupplier condition)
      throws Exception {
    return waitAtMost(timeoutInSeconds, condition, null);
  }

  public static Duration waitAtMost(
      int timeoutInSeconds, CallableBooleanSupplier condition, Supplier<String> message)
      throws Exception {
    return waitAtMost(Duration.ofSeconds(timeoutInSeconds), condition, message);
  }

  public static Duration waitAtMost(
      Duration timeout, CallableBooleanSupplier condition, Supplier<String> message)
      throws Exception {
    if (condition.getAsBoolean()) {
      return Duration.ZERO;
    }
    int waitTime = 100;
    int waitedTime = 0;
    int timeoutInMs = (int) timeout.toMillis();
    Exception exception = null;
    while (waitedTime <= timeoutInMs) {
      Thread.sleep(waitTime);
      waitedTime += waitTime;
      try {
        if (condition.getAsBoolean()) {
          return Duration.ofMillis(waitedTime);
        }
        exception = null;
      } catch (Exception e) {
        exception = e;
      }
    }
    String msg;
    if (message == null) {
      msg = "Waited " + timeout.getSeconds() + " second(s), condition never got true";
    } else {
      msg = "Waited " + timeout.getSeconds() + " second(s), " + message.get();
    }
    if (exception == null) {
      fail(msg);
    } else {
      fail(msg, exception);
    }
    return Duration.ofMillis(waitedTime);
  }

  public static class CountDownLatchConditions {

    public static Condition<CountDownLatch> completed() {
      return completed(Duration.ofSeconds(10));
    }

    static Condition<CountDownLatch> completed(int timeoutInSeconds) {
      return completed(Duration.ofSeconds(timeoutInSeconds));
    }

    static Condition<CountDownLatch> completed(Duration timeout) {
      return new Condition<>(
          latch -> latchCondition(latch, timeout), "completed in %d ms", timeout.toMillis());
    }

    private static boolean latchCondition(CountDownLatch latch, Duration timeout) {
      try {
        return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException(e);
      }
    }
  }

  public static String name(TestInfo info) {
    return name(info.getTestClass().get(), info.getTestMethod().get());
  }

  private static String name(ExtensionContext context) {
    return name(context.getTestInstance().get().getClass(), context.getTestMethod().get());
  }

  private static String name(Class<?> testClass, Method testMethod) {
    return name(testClass, testMethod.getName());
  }

  public static String name(Class<?> testClass, String testMethod) {
    String uuid = UUID.randomUUID().toString();
    return format(
        "%s_%s%s", testClass.getSimpleName(), testMethod, uuid.substring(uuid.length() / 2));
  }

  static Client client() {
    return Client.create();
  }

  static Connection connection(Client client) {
    return connection(client, o -> {});
  }

  static Connection connection(Client client, Consumer<ConnectionOptions> optionsCallback) {
    return connection(null, client, optionsCallback);
  }

  static Connection connection(
      String name, Client client, Consumer<ConnectionOptions> optionsCallback) {
    ConnectionOptions connectionOptions = new ConnectionOptions();
    connectionOptions
        .user("guest")
        .password("guest")
        .virtualHost("vhost:/")
        // only the mechanisms supported in RabbitMQ
        .saslOptions()
        .addAllowedMechanism("PLAIN")
        .addAllowedMechanism("EXTERNAL");
    if (name != null) {
      connectionOptions.properties(singletonMap("connection_name", name));
    }
    optionsCallback.accept(connectionOptions);
    try {
      return client.connect("localhost", 5672, connectionOptions);
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }
  }

  static UnsignedLong ulong(long value) {
    return UnsignedLong.valueOf(value);
  }

  public static AmqpEnvironmentBuilder environmentBuilder() {
    return new AmqpEnvironmentBuilder();
  }

  static class DisabledIfRabbitMqCtlNotSetCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      try {
        Cli.rabbitmqctlCommand();
        return ConditionEvaluationResult.enabled("rabbitmqctl.bin system property is set");
      } catch (IllegalStateException e) {
        return ConditionEvaluationResult.disabled("rabbitmqctl.bin system property not set");
      }
    }
  }

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfRabbitMqCtlNotSetCondition.class)
  @interface DisabledIfRabbitMqCtlNotSet {}
}
