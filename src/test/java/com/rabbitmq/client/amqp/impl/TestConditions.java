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

import com.rabbitmq.client.amqp.Environment;
import java.lang.annotation.*;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestConditions {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestConditions.class);

  private TestConditions() {}

  public enum BrokerVersion {
    RABBITMQ_4_0_3("4.0.3"),
    RABBITMQ_4_1_0("4.1.0"),
    RABBITMQ_4_2_0("4.2.0");

    final String value;

    BrokerVersion(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }
  }

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(BrokerVersionAtLeastCondition.class)
  public @interface BrokerVersionAtLeast {

    BrokerVersion value();
  }

  private static class BrokerVersionAtLeastCondition implements ExecutionCondition {

    private final Function<ExtensionContext, String> versionProvider;

    private BrokerVersionAtLeastCondition() {
      this.versionProvider =
          context -> {
            BrokerVersionAtLeast annotation =
                context.getElement().get().getAnnotation(BrokerVersionAtLeast.class);
            return annotation == null ? null : annotation.value().toString();
          };
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      if (context.getTestMethod().isEmpty()) {
        return ConditionEvaluationResult.enabled("Apply only to methods");
      }
      String expectedVersion = versionProvider.apply(context);
      if (expectedVersion == null) {
        return ConditionEvaluationResult.enabled("No broker version requirement");
      } else {
        String brokerVersion =
            context
                .getRoot()
                .getStore(ExtensionContext.Namespace.GLOBAL)
                .getOrComputeIfAbsent(
                    "brokerVersion",
                    k -> {
                      try (Environment env = TestUtils.environmentBuilder().build()) {
                        return ((AmqpConnection) env.connectionBuilder().build()).brokerVersion();
                      }
                    },
                    String.class);

        if (atLeastVersion(expectedVersion, brokerVersion)) {
          return ConditionEvaluationResult.enabled(
              "Broker version requirement met, expected "
                  + expectedVersion
                  + ", actual "
                  + brokerVersion);
        } else {
          return ConditionEvaluationResult.disabled(
              "Broker version requirement not met, expected "
                  + expectedVersion
                  + ", actual "
                  + brokerVersion);
        }
      }
    }
  }

  private static boolean atLeastVersion(String expectedVersion, String currentVersion) {
    try {
      currentVersion = currentVersion(currentVersion);
      return "0.0.0".equals(currentVersion)
          || Utils.versionCompare(currentVersion, expectedVersion) >= 0;
    } catch (RuntimeException e) {
      LOGGER.warn("Unable to parse broker version {}", currentVersion, e);
      throw e;
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
}
