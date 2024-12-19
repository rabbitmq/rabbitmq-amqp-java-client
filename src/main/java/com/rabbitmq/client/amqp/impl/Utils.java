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

import com.rabbitmq.client.amqp.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Utils {

  static final Supplier<String> NAME_SUPPLIER = new NameSupplier("client.gen-");

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  private static final ThreadFactory THREAD_FACTORY;
  private static final Function<String, ExecutorService> EXECUTOR_SERVICE_FACTORY;

  static {
    if (isJava21OrMore()) {
      LOGGER.debug("Running Java 21 or more, using virtual threads");
      Class<?> builderClass =
          Arrays.stream(Thread.class.getDeclaredClasses())
              .filter(c -> "Builder".equals(c.getSimpleName()))
              .findFirst()
              .get();
      // Reflection code is the same as:
      // Thread.ofVirtual().factory();
      try {
        Object builder = Thread.class.getDeclaredMethod("ofVirtual").invoke(null);
        THREAD_FACTORY = (ThreadFactory) builderClass.getDeclaredMethod("factory").invoke(builder);
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
      EXECUTOR_SERVICE_FACTORY =
          prefix -> {
            try {
              // Reflection code is the same as the 2 following lines:
              // ThreadFactory factory = Thread.ofVirtual().name(prefix, 0).factory();
              // Executors.newThreadPerTaskExecutor(factory);
              Object builder = Thread.class.getDeclaredMethod("ofVirtual").invoke(null);
              if (prefix != null) {
                builder =
                    builderClass
                        .getDeclaredMethod("name", String.class, Long.TYPE)
                        .invoke(builder, prefix, 0L);
              }
              ThreadFactory factory =
                  (ThreadFactory) builderClass.getDeclaredMethod("factory").invoke(builder);
              return (ExecutorService)
                  Executors.class
                      .getDeclaredMethod("newThreadPerTaskExecutor", ThreadFactory.class)
                      .invoke(null, factory);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
              throw new RuntimeException(e);
            }
          };
    } else {
      THREAD_FACTORY = Executors.defaultThreadFactory();
      EXECUTOR_SERVICE_FACTORY = prefix -> Executors.newCachedThreadPool(threadFactory(prefix));
    }
  }

  private Utils() {}

  static ExecutorService executorService(String prefixFormat, Object... args) {
    return EXECUTOR_SERVICE_FACTORY.apply(String.format(prefixFormat, args));
  }

  private static boolean isJava21OrMore() {
    return Runtime.version().compareTo(Runtime.Version.parse("21")) >= 0;
  }

  static void checkMessageAnnotations(Map<String, Object> annotations) {
    annotations.forEach((k, v) -> validateMessageAnnotationKey(k));
  }

  static void validateMessageAnnotationKey(String key) {
    if (!key.startsWith("x-")) {
      throw new IllegalArgumentException("Message annotation keys must start with 'x-': " + key);
    }
  }

  private static class NamedThreadFactory implements ThreadFactory {

    private final ThreadFactory backingThreadFactory;

    private final String prefix;

    private final AtomicLong count = new AtomicLong(0);

    private NamedThreadFactory(String prefix) {
      this(Executors.defaultThreadFactory(), prefix);
    }

    private NamedThreadFactory(ThreadFactory backingThreadFactory, String prefix) {
      this.backingThreadFactory = backingThreadFactory;
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = this.backingThreadFactory.newThread(r);
      thread.setName(prefix + count.getAndIncrement());
      return thread;
    }
  }

  static ThreadFactory threadFactory(String prefix) {
    if (prefix == null) {
      return Executors.defaultThreadFactory();
    } else {
      return new NamedThreadFactory(prefix);
    }
  }

  static ThreadFactory defaultThreadFactory() {
    return THREAD_FACTORY;
  }

  static void throwIfInterrupted() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }
  }

  static DefaultAddressBuilder<?> addressBuilder() {
    return new DefaultAddressBuilder<>(null) {};
  }

  private static class NameSupplier implements Supplier<String> {

    private final String prefix;

    private NameSupplier(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public String get() {
      String uuid = UUID.randomUUID().toString();
      MessageDigest md = null;
      try {
        md = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new AmqpException(e);
      }
      byte[] digest = md.digest(uuid.getBytes(StandardCharsets.UTF_8));
      return prefix
          + Base64.getEncoder()
              .encodeToString(digest)
              .replace('+', '-')
              .replace('/', '_')
              .replace("=", "");
    }
  }

  private static String currentVersion(String currentVersion) {
    // versions built from source: 3.7.0+rc.1.4.gedc5d96
    if (currentVersion.contains("+")) {
      currentVersion = currentVersion.substring(0, currentVersion.indexOf("+"));
    }
    // alpha (snapshot) versions: 3.7.0~alpha.449-1
    if (currentVersion.contains("~")) {
      currentVersion = currentVersion.substring(0, currentVersion.indexOf("~"));
    }
    // alpha (snapshot) versions: 3.7.1-alpha.40
    if (currentVersion.contains("-")) {
      currentVersion = currentVersion.substring(0, currentVersion.indexOf("-"));
    }
    return currentVersion;
  }

  /**
   * https://stackoverflow.com/questions/6701948/efficient-way-to-compare-version-strings-in-java
   */
  static int versionCompare(String str1, String str2) {
    String[] vals1 = str1.split("\\.");
    String[] vals2 = str2.split("\\.");
    int i = 0;
    // set index to first non-equal ordinal or length of shortest version string
    while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
      i++;
    }
    // compare first non-equal ordinal number
    if (i < vals1.length && i < vals2.length) {
      Integer val1 = Integer.valueOf(vals1[i]);
      Integer val2 = Integer.valueOf(vals2[i]);
      return val1.compareTo(val2);
    }
    // the strings are equal or one string is a substring of the other
    // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
    return Integer.signum(vals1.length - vals2.length);
  }

  static boolean is4_0_OrMore(String brokerVersion) {
    return versionCompare(currentVersion(brokerVersion), "4.0.0") >= 0;
  }

  static boolean is4_1_OrMore(String brokerVersion) {
    return versionCompare(currentVersion(brokerVersion), "4.1.0") >= 0;
  }

  static boolean supportFilterExpressions(String brokerVersion) {
    return is4_1_OrMore(brokerVersion);
  }

  static boolean supportSetToken(String brokerVersion) {
    return is4_1_OrMore(brokerVersion);
  }

  static final class ObservationConnectionInfo implements ObservationCollector.ConnectionInfo {

    private final String address;
    private final int port;

    ObservationConnectionInfo(Address address) {
      this.address = address == null ? "" : address.host();
      this.port = address == null ? 0 : address.port();
    }

    @Override
    public String peerAddress() {
      return this.address;
    }

    @Override
    public int peerPort() {
      return this.port;
    }
  }

  static void maybeClose(
      AutoCloseable closeable, java.util.function.Consumer<Exception> exceptionCallback) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {
        exceptionCallback.accept(e);
      }
    }
  }

  static final ObservationCollector NO_OP_OBSERVATION_COLLECTOR = new NoOpObservationCollector();

  private static final class NoOpObservationCollector implements ObservationCollector {

    private NoOpObservationCollector() {}

    @Override
    public <T> T publish(
        String exchange,
        String routingKey,
        Message message,
        ConnectionInfo connectionInfo,
        Function<Message, T> publishCall) {
      return publishCall.apply(message);
    }

    @Override
    public Consumer.MessageHandler subscribe(String queue, Consumer.MessageHandler handler) {
      return handler;
    }
  }

  private static final Pattern MAX_AGE_PATTERN = Pattern.compile("^[0-9]+[YMDhms]$");

  static boolean validateMaxAge(String input) {
    return MAX_AGE_PATTERN.matcher(input).find();
  }

  static class StopWatch {

    private final long start = System.nanoTime();
    private Duration duration;

    Duration stop() {
      this.duration = Duration.ofNanos(System.nanoTime() - start);
      return this.duration;
    }
  }

  static Runnable namedRunnable(Runnable task, String format, Object... args) {
    return new NamedRunnable(String.format(format, args), task);
  }

  private static class NamedRunnable implements Runnable {

    private final String name;
    private final Runnable delegate;

    private NamedRunnable(String name, Runnable delegate) {
      this.name = name;
      this.delegate = delegate;
    }

    @Override
    public void run() {
      this.delegate.run();
    }

    @Override
    public String toString() {
      return this.name;
    }
  }
}
