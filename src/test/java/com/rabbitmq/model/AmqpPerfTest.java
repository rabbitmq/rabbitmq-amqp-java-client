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

import static com.rabbitmq.model.Management.ExchangeType.DIRECT;
import static com.rabbitmq.model.Management.QueueType.QUORUM;
import static com.rabbitmq.model.TestUtils.environmentBuilder;

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class AmqpPerfTest {

  /*
  ./mvnw -q clean test-compile exec:java \
    -Dexec.mainClass=com.rabbitmq.model.AmqpPerfTest \
    -Dexec.classpathScope="test"
     */
  public static void main(String[] args) {
    MeterRegistry registry = dropwizardMeterRegistry();
    Counter published = registry.counter("published");
    Counter confirmed = registry.counter("confirmed");
    Counter consumed = registry.counter("consumed");
    List<Counter> counters =
        registry.getMeters().stream()
            .filter(m -> m.getId().getType() == Meter.Type.COUNTER)
            .map(m -> (Counter) m)
            .collect(Collectors.toList());
    Map<String, Long> lastMetersValues = new ConcurrentHashMap<>(counters.size());
    counters.forEach(m -> lastMetersValues.put(m.getId().getName(), 0L));

    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10);
    AtomicLong lastTick = new AtomicLong(System.nanoTime());
    AtomicInteger reportCount = new AtomicInteger(1);
    executorService.scheduleAtFixedRate(
        () -> {
          long now = System.nanoTime();
          Duration elapsed = Duration.ofNanos(now - lastTick.get());
          lastTick.set(now);

          StringBuilder builder = new StringBuilder().append(reportCount.get()).append(", ");
          String counterReport =
              counters.stream()
                  .map(
                      counter -> {
                        String meterName = counter.getId().getName();
                        long lastValue = lastMetersValues.get(meterName);
                        long currentValue = (long) counter.count();
                        long rate = 1000 * (currentValue - lastValue) / elapsed.toMillis();
                        lastMetersValues.put(meterName, currentValue);
                        return String.format("%s %d msg/s", meterName, rate);
                      })
                  .collect(Collectors.joining(", "));
          builder.append(counterReport);
          System.out.println(builder);

          reportCount.incrementAndGet();
        },
        1,
        1,
        TimeUnit.SECONDS);

    String e = TestUtils.name(AmqpPerfTest.class, "main");
    String q = TestUtils.name(AmqpPerfTest.class, "main");
    String rk = "foo";
    Environment environment = environmentBuilder().build();
    Connection connection = environment.connectionBuilder().build();
    Management management = connection.management();

    CountDownLatch shutdownLatch = new CountDownLatch(1);

    AtomicBoolean hasShutDown = new AtomicBoolean(false);
    Runnable shutdownSequence =
        () -> {
          executorService.shutdownNow();
          shutdownLatch.countDown();
          if (hasShutDown.compareAndSet(false, true)) {
            management.queueDeletion().delete(q);
            management.exchangeDeletion().delete(e);
            management.close();
          }
        };

    Runtime.getRuntime().addShutdownHook(new Thread(shutdownSequence::run));
    try {
      management.exchange().name(e).type(DIRECT).declare();
      management.queue().name(q).type(QUORUM).declare();
      management.binding().sourceExchange(e).destinationQueue(q).key(rk).bind();

      connection
          .consumerBuilder()
          .address(q)
          .initialCredits(1000)
          .messageHandler(
              (context, message) -> {
                consumed.increment();
                context.accept();
              })
          .build();

      executorService.submit(
          () -> {
            Publisher publisher = connection.publisherBuilder().address("/exchange/" + e).build();
            Publisher.Callback callback =
                context -> {
                  confirmed.increment();
                };
            while (!Thread.currentThread().isInterrupted()) {
              Message message = publisher.message().subject(rk);
              publisher.publish(message, callback);
              published.increment();
            }
          });
      shutdownLatch.await();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    } finally {
      shutdownSequence.run();
    }
  }

  static MeterRegistry dropwizardMeterRegistry() {
    DropwizardConfig dropwizardConfig =
        new DropwizardConfig() {
          @Override
          public String prefix() {
            return "";
          }

          @Override
          public String get(String key) {
            return null;
          }
        };
    MetricRegistry metricRegistry = new MetricRegistry();
    MeterRegistry dropwizardMeterRegistry =
        new DropwizardMeterRegistry(
            dropwizardConfig,
            metricRegistry,
            HierarchicalNameMapper.DEFAULT,
            io.micrometer.core.instrument.Clock.SYSTEM) {
          @Override
          protected Double nullGaugeValue() {
            return null;
          }
        };
    return dropwizardMeterRegistry;
  }
}
