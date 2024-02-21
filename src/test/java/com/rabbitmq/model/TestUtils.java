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
package com.rabbitmq.model;

import static java.lang.String.format;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;

public abstract class TestUtils {

  private TestUtils() {}

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

  static String name(Class<?> testClass, String testMethod) {
    String uuid = UUID.randomUUID().toString();
    return format(
        "%s_%s%s", testClass.getSimpleName(), testMethod, uuid.substring(uuid.length() / 2));
  }
}
