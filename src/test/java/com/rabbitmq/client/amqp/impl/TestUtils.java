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

import com.rabbitmq.client.amqp.Management;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import java.io.IOException;
import java.lang.annotation.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.time.Duration;
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
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

public abstract class TestUtils {

  private static final Duration DEFAULT_CONDITION_TIMEOUT = Duration.ofSeconds(10);

  private TestUtils() {}

  static void submitTask(Runnable task) {
    Utils.defaultThreadFactory().newThread(task).start();
  }

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

  public static class CountDownLatchReferenceConditions {

    public static Condition<AtomicReference<CountDownLatch>> completed() {
      return new Condition<>(
          latch -> CountDownLatchConditions.latchCondition(latch.get(), DEFAULT_CONDITION_TIMEOUT),
          "completed in %d ms",
          DEFAULT_CONDITION_TIMEOUT.toMillis());
    }
  }

  static QueueInfoAssert assertThat(Management.QueueInfo queueInfo) {
    return new QueueInfoAssert(queueInfo);
  }

  static SyncAssert assertThat(Sync sync) {
    return new SyncAssert(sync);
  }

  static class SyncAssert extends AbstractObjectAssert<SyncAssert, Sync> {

    private SyncAssert(Sync sync) {
      super(sync, SyncAssert.class);
    }

    SyncAssert completes() {
      return this.completes(DEFAULT_CONDITION_TIMEOUT);
    }

    SyncAssert completes(Duration timeout) {
      try {
        boolean completed = actual.await(timeout);
        if (!completed) {
          fail("Sync timed out after %d ms", timeout.toMillis());
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException(e);
      }
      return this;
    }
  }

  static CountDownLatchAssert assertThat(AtomicReference<CountDownLatch> reference) {
    return new CountDownLatchAssert(reference);
  }

  static CountDownLatchAssert assertThat(CountDownLatch latch) {
    return new CountDownLatchAssert(latch);
  }

  static class CountDownLatchAssert
      extends AbstractObjectAssert<CountDownLatchAssert, CountDownLatch> {

    private CountDownLatchAssert(CountDownLatch latch) {
      super(latch, CountDownLatchAssert.class);
    }

    private CountDownLatchAssert(AtomicReference<CountDownLatch> reference) {
      super(reference.get(), CountDownLatchAssert.class);
    }

    CountDownLatchAssert completes() {
      return this.completes(DEFAULT_CONDITION_TIMEOUT);
    }

    CountDownLatchAssert completes(Duration timeout) {
      try {
        boolean completed = actual.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!completed) {
          fail("Latch timed out after %d ms", timeout.toMillis());
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException(e);
      }
      return this;
    }
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

  static Proxy toxiproxy(ToxiproxyClient client, String name, int proxyPort) throws IOException {
    Proxy proxy = client.getProxyOrNull(name);
    if (proxy != null) {
      proxy.delete();
    }
    proxy =
        client.createProxy(
            name,
            "localhost:" + proxyPort,
            DefaultConnectionSettings.DEFAULT_HOST + ":" + DefaultConnectionSettings.DEFAULT_PORT);
    return proxy;
  }

  static int randomNetworkPort() throws IOException {
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
    return new AmqpEnvironmentBuilder();
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
    return Cli.rabbitmqctl("status").output().contains("amqp/ssl");
  }

  static boolean addressV1Permitted() {
    return Cli.rabbitmqctl("eval 'rabbit_deprecated_features:is_permitted(amqp_address_v1).'")
        .output()
        .trim()
        .endsWith("true");
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

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfTlsNotEnabledCondition.class)
  public @interface DisabledIfTlsNotEnabled {}

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

  static class QueueInfoAssert extends AbstractObjectAssert<QueueInfoAssert, Management.QueueInfo> {

    private QueueInfoAssert(Management.QueueInfo queueInfo) {
      super(queueInfo, QueueInfoAssert.class);
    }

    QueueInfoAssert hasName(String name) {
      isNotNull();
      if (!actual.name().equals(name)) {
        fail("Queue should be named '%s' but is '%s'", name, actual.name());
      }
      return this;
    }

    QueueInfoAssert hasConsumerCount(int consumerCount) {
      isNotNull();
      if (Integer.compareUnsigned(actual.consumerCount(), consumerCount) != 0) {
        fail(
            "Queue should have %s consumer(s) but has %s",
            Integer.toUnsignedString(consumerCount),
            Integer.toUnsignedString(actual.consumerCount()));
      }
      return this;
    }

    QueueInfoAssert hasNoConsumers() {
      return this.hasConsumerCount(0);
    }

    QueueInfoAssert hasMessageCount(long messageCount) {
      isNotNull();
      if (Long.compareUnsigned(actual.messageCount(), messageCount) != 0) {
        fail(
            "Queue should contains %s messages(s) but contains %s",
            Long.toUnsignedString(messageCount), Long.toUnsignedString(actual.messageCount()));
      }
      return this;
    }

    QueueInfoAssert isEmpty() {
      return this.hasMessageCount(0);
    }

    QueueInfoAssert is(Management.QueueType type) {
      isNotNull();
      if (actual.type() != type) {
        fail("Queue should be of type %s but is %s", type.name(), actual.type().name());
      }
      return this;
    }

    QueueInfoAssert hasArgument(String key, Object value) {
      isNotNull();
      if (!actual.arguments().containsKey(key) || !actual.arguments().get(key).equals(value)) {
        fail(
            "Queue should have argument %s = %s, but does not (arguments: %s)",
            key, value.toString(), actual.arguments().toString());
      }
      return this;
    }

    QueueInfoAssert isDurable() {
      return this.flag("durable", actual.durable(), true);
    }

    QueueInfoAssert isNotDurable() {
      return this.flag("durable", actual.durable(), false);
    }

    QueueInfoAssert isAutoDelete() {
      return this.flag("auto-delete", actual.autoDelete(), true);
    }

    QueueInfoAssert isNotAutoDelete() {
      return this.flag("auto-delete", actual.autoDelete(), false);
    }

    QueueInfoAssert isExclusive() {
      return this.flag("exclusive", actual.exclusive(), true);
    }

    QueueInfoAssert isNotExclusive() {
      return this.flag("exclusive", actual.exclusive(), false);
    }

    private QueueInfoAssert flag(String label, boolean expected, boolean actual) {
      isNotNull();
      if (expected != actual) {
        fail("Queue should have %s = %b but does not", label, actual);
      }
      return this;
    }
  }

  static Sync sync() {
    return sync(1);
  }

  static Sync sync(int count) {
    return new Sync(count);
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

  static class Sync {

    private final AtomicReference<CountDownLatch> latch = new AtomicReference<>();

    private Sync(int count) {
      this.latch.set(new CountDownLatch(count));
    }

    void down() {
      this.latch.get().countDown();
    }

    private boolean await(Duration timeout) throws InterruptedException {
      return this.latch.get().await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    void reset(int count) {
      this.latch.set(new CountDownLatch(count));
    }

    void reset() {
      this.reset(1);
    }
  }
}
