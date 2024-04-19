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

import com.rabbitmq.model.ModelException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class Utils {

  static final Supplier<String> NAME_SUPPLIER = new NameSupplier("client.gen-");

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  private static final Function<String, ExecutorService> EXECUTOR_SERVICE_FACTORY;

  static {
    if (isJava21OrMore()) {
      LOGGER.debug("Running Java 21 or more, using virtual threads");
      Class<?> builderClass =
          Arrays.stream(Thread.class.getDeclaredClasses())
              .filter(c -> "Builder".equals(c.getSimpleName()))
              .findFirst()
              .get();
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

  static class ConnectionParameters {

    private final String username, password, host, virtualHost;
    private final int port;

    ConnectionParameters(
        String username, String password, String host, String virtualHost, int port) {
      this.username = username;
      this.password = password;
      this.host = host;
      this.virtualHost = virtualHost;
      this.port = port;
    }

    String username() {
      return username;
    }

    String password() {
      return password;
    }

    String host() {
      return host;
    }

    String virtualHost() {
      return virtualHost;
    }

    int port() {
      return port;
    }

    String label() {
      return this.host + ":" + this.port;
    }

    @Override
    public String toString() {
      return "ConnectionParameters{"
          + "username='"
          + username
          + '\''
          + ", password='********'"
          + ", host='"
          + host
          + '\''
          + ", virtualHost='"
          + virtualHost
          + '\''
          + ", port="
          + port
          + '}';
    }
  }

  static String extractQueueFromSourceAddress(String address) {
    if (address == null) {
      return null;
    } else if (address.startsWith("/queue/")) {
      return address.replace("/queue/", "");
    } else if (address.startsWith("/exchange/") || address.startsWith("/topic/")) {
      return null;
    } else {
      return null;
    }
  }

  static void throwIfInterrupted() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }
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
        throw new ModelException(e);
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
}
