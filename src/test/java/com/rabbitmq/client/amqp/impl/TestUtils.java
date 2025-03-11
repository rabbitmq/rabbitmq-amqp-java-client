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
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.fail;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Resource;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import java.io.IOException;
import java.lang.annotation.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

public abstract class TestUtils {

  static final Duration DEFAULT_CONDITION_TIMEOUT = Duration.ofSeconds(10);
  static final Duration DEFAULT_WAIT_TIME = Duration.ofMillis(100);
  private static final boolean USE_WEB_SOCKET =
      Boolean.parseBoolean(System.getProperty("rabbitmq.use.web.socket", "false"));

  private TestUtils() {}

  static void submitTask(Runnable task) {
    Utils.defaultThreadFactory().newThread(task).start();
  }

  static <T> T waitUntilStable(Supplier<T> call) {
    return waitUntilStable(call, Duration.ofMillis(200));
  }

  static <T> T waitUntilStable(Supplier<T> call, Duration waitTime) {
    Duration timeout = Duration.ofSeconds(10);
    Duration waitedTime = Duration.ZERO;
    T newValue = null;
    while (waitedTime.compareTo(timeout) <= 0) {
      T previousValue = call.get();
      try {
        Thread.sleep(waitTime.toMillis());
        waitedTime = waitedTime.plus(waitTime);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      newValue = call.get();
      if (newValue.equals(previousValue)) {
        return newValue;
      }
    }
    fail("Value did not stabilize in %s, last value was %s", timeout, newValue);
    return null;
  }

  static Resource.StateListener closedOnSecurityExceptionListener(Sync sync) {
    return context -> {
      if (context.currentState() == Resource.State.CLOSED
          && context.failureCause() instanceof AmqpException.AmqpSecurityException) {
        sync.down();
      }
    };
  }

  @FunctionalInterface
  public interface CallableBooleanSupplier {
    boolean getAsBoolean() throws Exception;
  }

  @FunctionalInterface
  interface RunnableWithException {

    void run() throws Exception;
  }

  public static Duration waitAtMostNoException(RunnableWithException condition) {
    return waitAtMostNoException(DEFAULT_CONDITION_TIMEOUT, condition);
  }

  public static Duration waitAtMostNoException(Duration timeout, RunnableWithException condition) {
    return waitAtMost(
        timeout,
        DEFAULT_WAIT_TIME,
        () -> {
          try {
            condition.run();
            return true;
          } catch (Exception e) {
            return false;
          }
        },
        null);
  }

  public static Duration waitAtMost(CallableBooleanSupplier condition) {
    return waitAtMost(DEFAULT_CONDITION_TIMEOUT, DEFAULT_WAIT_TIME, condition, null);
  }

  public static Duration waitAtMost(CallableBooleanSupplier condition, Supplier<String> message) {
    return waitAtMost(DEFAULT_CONDITION_TIMEOUT, DEFAULT_WAIT_TIME, condition, message);
  }

  public static Duration waitAtMost(
      Duration timeout, Duration waitTime, CallableBooleanSupplier condition) {
    return waitAtMost(timeout, waitTime, condition, null);
  }

  public static Duration waitAtMost(Duration timeout, CallableBooleanSupplier condition) {
    return waitAtMost(timeout, DEFAULT_WAIT_TIME, condition, null);
  }

  public static Duration waitAtMost(int timeoutInSeconds, CallableBooleanSupplier condition) {
    return waitAtMost(timeoutInSeconds, condition, null);
  }

  public static Duration waitAtMost(
      int timeoutInSeconds, CallableBooleanSupplier condition, Supplier<String> message) {
    return waitAtMost(Duration.ofSeconds(timeoutInSeconds), DEFAULT_WAIT_TIME, condition, message);
  }

  public static Duration waitAtMost(
      Duration timeout, CallableBooleanSupplier condition, Supplier<String> message) {
    return waitAtMost(timeout, DEFAULT_WAIT_TIME, condition, message);
  }

  public static Duration waitAtMost(
      Duration timeout,
      Duration waitTime,
      CallableBooleanSupplier condition,
      Supplier<String> message) {
    long start = System.nanoTime();
    try {
      if (condition.getAsBoolean()) {
        return Duration.ZERO;
      }
      Duration waitedTime = Duration.ofNanos(System.nanoTime() - start);
      Exception exception = null;
      while (waitedTime.compareTo(timeout) <= 0) {
        Thread.sleep(waitTime.toMillis());
        waitedTime = waitedTime.plus(waitTime);
        start = System.nanoTime();
        try {
          if (condition.getAsBoolean()) {
            return waitedTime;
          }
          exception = null;
        } catch (Exception e) {
          exception = e;
        }
        waitedTime = waitedTime.plus(Duration.ofNanos(System.nanoTime() - start));
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
      return waitedTime;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void simulateActivity(long timeInMs) {
    try {
      Thread.sleep(timeInMs);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static String name(TestInfo info) {
    return name(info.getTestClass().get(), info.getTestMethod().get());
  }

  static String name(ExtensionContext context) {
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
    if (useWebSocket()) {
      connectionOptions.transportOptions().useWebSockets(true).webSocketPath("/ws");
    }
    try {
      return client.connect("localhost", TestUtils.defaultPort(), connectionOptions);
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }
  }

  static Proxy toxiproxy(ToxiproxyClient client, String name, int proxyPort) throws IOException {
    Proxy proxy = client.getProxyOrNull(name);
    if (proxy != null) {
      proxy.delete();
    }
    proxy =
        client.createProxy(
            name,
            "localhost:" + proxyPort,
            DefaultConnectionSettings.DEFAULT_HOST + ":" + defaultPort());
    return proxy;
  }

  public static int randomNetworkPort() throws IOException {
    ServerSocket socket = new ServerSocket();
    socket.bind(null);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  static UnsignedLong ulong(long value) {
    return UnsignedLong.valueOf(value);
  }

  public static AmqpEnvironmentBuilder environmentBuilder() {
    AmqpEnvironmentBuilder builder = new AmqpEnvironmentBuilder();
    maybeConfigureWebSocket((DefaultConnectionSettings<?>) builder.connectionSettings());
    return builder;
  }

  static void maybeConfigureWebSocket(DefaultConnectionSettings<?> connectionSettings) {
    if (useWebSocket()) {
      connectionSettings.useWebSocket(true);
      connectionSettings.port(DefaultConnectionSettings.DEFAULT_WEB_SOCKET_PORT);
    }
  }

  @SuppressWarnings("unchecked")
  static ToxiproxyClient toxiproxyClient(ExtensionContext context) {
    CloseableResourceWrapper<ToxiproxyClient> wrapper =
        (CloseableResourceWrapper<ToxiproxyClient>)
            context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL).get("toxiproxy");
    return wrapper == null ? null : wrapper.resource();
  }

  static void storeToxiproxyClient(ToxiproxyClient client, ExtensionContext context) {
    context
        .getRoot()
        .getStore(ExtensionContext.Namespace.GLOBAL)
        .put("toxiproxy", new CloseableResourceWrapper<>(client, c -> {}));
  }

  static boolean tlsAvailable() {
    String status = Cli.rabbitmqctl("status").output();
    String shouldContain = useWebSocket() ? "https/web-amqp" : "amqp/ssl";
    return status.contains(shouldContain);
  }

  static boolean addressV1Permitted() {
    return Cli.rabbitmqctl("eval 'rabbit_deprecated_features:is_permitted(amqp_address_v1).'")
        .output()
        .trim()
        .endsWith("true");
  }

  static boolean isCluster() {
    return !Cli.rabbitmqctl("eval 'nodes().'").output().replace("[", "").replace("]", "").isBlank();
  }

  static boolean useWebSocket() {
    return USE_WEB_SOCKET;
  }

  static int defaultPort() {
    return useWebSocket()
        ? DefaultConnectionSettings.DEFAULT_WEB_SOCKET_PORT
        : DefaultConnectionSettings.DEFAULT_PORT;
  }

  static class DisabledIfTlsNotEnabledCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      if (tlsAvailable()) {
        return ConditionEvaluationResult.enabled("TLS is enabled");
      } else {
        return ConditionEvaluationResult.disabled("TLS is disabled");
      }
    }
  }

  static class DisabledIfWebSocketCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      if (useWebSocket()) {
        return ConditionEvaluationResult.disabled("Testing against WebSocket");
      } else {
        return ConditionEvaluationResult.enabled("Not testing against WebSocket");
      }
    }
  }

  static class DisabledIfAddressV1PermittedCondition implements ExecutionCondition {

    private static final String KEY = "addressV1Permitted";

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      boolean addressV1Permitted =
          context
              .getRoot()
              .getStore(ExtensionContext.Namespace.GLOBAL)
              .getOrComputeIfAbsent(KEY, k -> addressV1Permitted(), Boolean.class);
      if (addressV1Permitted) {
        return ConditionEvaluationResult.disabled("AMQP address format v1 is permitted");
      } else {
        return ConditionEvaluationResult.enabled("AMQP address format v1 is not permitted");
      }
    }
  }

  static class DisabledIfToxiproxyNotAvailableCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      try {
        ToxiproxyClient client = toxiproxyClient(context);
        if (client == null) {
          client = new ToxiproxyClient("localhost", 8474);
          client.version();
          storeToxiproxyClient(client, context);
        }
        if (context.getTestInstance().isPresent()) {
          Object test = context.getTestInstance().get();
          for (Field field : test.getClass().getDeclaredFields()) {
            if (ToxiproxyClient.class.isAssignableFrom(field.getType())) {
              field.setAccessible(true);
              if (field.get(test) == null) {
                field.set(test, client);
              }
            }
          }
        }
        return ConditionEvaluationResult.enabled("toxiproxy is available");
      } catch (Exception e) {
        return ConditionEvaluationResult.disabled("toxiproxy is not available");
      }
    }
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

  private abstract static class DisabledIfPluginNotEnabledCondition implements ExecutionCondition {

    private final String pluginLabel;
    private final Predicate<String> condition;

    DisabledIfPluginNotEnabledCondition(String pluginLabel, Predicate<String> condition) {
      this.pluginLabel = pluginLabel;
      this.condition = condition;
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      try {
        String output = Cli.rabbitmqctl("status").output();
        if (condition.test(output)) {
          return ConditionEvaluationResult.enabled(format("%s plugin enabled", pluginLabel));
        } else {
          return ConditionEvaluationResult.disabled(format("%s plugin disabled", pluginLabel));
        }
      } catch (Exception e) {
        return ConditionEvaluationResult.disabled(
            format("Error while trying to detect %s plugin: " + e.getMessage(), pluginLabel));
      }
    }
  }

  private static class DisabledIfAuthMechanismSslNotEnabledCondition
      extends DisabledIfPluginNotEnabledCondition {

    DisabledIfAuthMechanismSslNotEnabledCondition() {
      super(
          "X509 authentication mechanism",
          output -> output.contains("rabbitmq_auth_mechanism_ssl"));
    }
  }

  private static class DisabledIfOauth2AuthBackendNotEnabledCondition
      extends DisabledIfPluginNotEnabledCondition {

    DisabledIfOauth2AuthBackendNotEnabledCondition() {
      super(
          "OAuth2 authentication backend",
          output -> output.contains("rabbitmq_auth_backend_oauth2"));
    }
  }

  static class DisabledIfNotClusterCondition implements ExecutionCondition {

    private static final String KEY = "isCluster";

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      ExtensionContext.Store store = context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL);
      boolean isCluster = store.getOrComputeIfAbsent(KEY, k -> isCluster(), Boolean.class);
      if (isCluster) {
        return ConditionEvaluationResult.enabled("Multi-node cluster");
      } else {
        return ConditionEvaluationResult.disabled("Not a multi-node cluster");
      }
    }
  }

  private static class DisabledOnSemeruCondition
      implements org.junit.jupiter.api.extension.ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      String javaRuntimeName = System.getProperty("java.runtime.name");
      return javaRuntimeName.toLowerCase(Locale.ENGLISH).contains("semeru")
          ? ConditionEvaluationResult.disabled("Test fails on Semeru")
          : ConditionEvaluationResult.enabled("OK");
    }
  }

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfTlsNotEnabledCondition.class)
  public @interface DisabledIfTlsNotEnabled {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfWebSocketCondition.class)
  public @interface DisabledIfWebSocket {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfAddressV1PermittedCondition.class)
  public @interface DisabledIfAddressV1Permitted {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfRabbitMqCtlNotSetCondition.class)
  @interface DisabledIfRabbitMqCtlNotSet {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfToxiproxyNotAvailableCondition.class)
  @interface DisabledIfToxiproxyNotAvailable {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfAuthMechanismSslNotEnabledCondition.class)
  @interface DisabledIfAuthMechanismSslNotEnabled {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfOauth2AuthBackendNotEnabledCondition.class)
  @interface DisabledIfOauth2AuthBackendNotEnabled {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfNotClusterCondition.class)
  @interface DisabledIfNotCluster {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledOnSemeruCondition.class)
  public @interface DisabledOnJavaSemeru {}

  static Sync sync() {
    return sync(1);
  }

  public static Sync sync(int count) {
    return new Sync(count, () -> {}, null);
  }

  static Sync sync(int count, Runnable doneCallback, String format, Object... args) {
    return new Sync(count, doneCallback, format, args);
  }

  private static class CloseableResourceWrapper<T>
      implements ExtensionContext.Store.CloseableResource {

    private final T resource;
    private final Consumer<T> closing;

    private CloseableResourceWrapper(T resource, Consumer<T> closing) {
      this.resource = resource;
      this.closing = closing;
    }

    T resource() {
      return this.resource;
    }

    @Override
    public void close() {
      this.closing.accept(this.resource);
    }
  }

  public static class Sync {

    private final String description;
    private final AtomicReference<CountDownLatch> latch = new AtomicReference<>();
    private final AtomicReference<Runnable> doneCallback = new AtomicReference<>();

    private Sync(int count, Runnable doneCallback, String description, Object... args) {
      this.latch.set(new CountDownLatch(count));
      if (description == null) {
        this.description = "N/A";
      } else {
        this.description = String.format(description, args);
      }
      this.doneCallback.set(doneCallback);
    }

    public void down() {
      this.latch.get().countDown();
    }

    boolean await(Duration timeout) {
      try {
        return this.latch.get().await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      } finally {
        this.doneCallback.get().run();
      }
    }

    void reset(int count) {
      this.latch.set(new CountDownLatch(count));
    }

    void reset() {
      this.reset(1);
    }

    boolean hasCompleted() {
      return this.latch.get().getCount() == 0;
    }

    @Override
    public String toString() {
      return this.description;
    }
  }
}
