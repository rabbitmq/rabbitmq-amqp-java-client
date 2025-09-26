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
package com.rabbitmq.client.amqp.perf;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.HistogramSupport;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

class PerformanceMetrics implements AutoCloseable {

  private final MeterRegistry registry;
  private final ScheduledExecutorService executorService;
  private final PrintWriter out;
  private final Timer latency, publishedAcceptedLatency;
  private volatile Runnable closingSequence = () -> {};
  private volatile long lastPublishedCount = 0;
  private volatile long lastConsumedCount = 0;

  PerformanceMetrics(
      MeterRegistry registry, ScheduledExecutorService executorService, PrintWriter out) {
    this.registry = registry;
    this.executorService = executorService;
    this.out = out;

    String metricsPrefix = "rabbitmq.amqp";
    latency =
        Timer.builder(metricsPrefix + ".latency")
            .description("message latency")
            .publishPercentiles(0.5, 0.75, 0.95, 0.99)
            .distributionStatisticExpiry(Duration.ofSeconds(1))
            .serviceLevelObjectives()
            .register(registry);
    publishedAcceptedLatency =
        Timer.builder(metricsPrefix + ".published_accepted_latency")
            .description("published message disposition latency")
            .publishPercentiles(0.5, 0.75, 0.95, 0.99)
            .distributionStatisticExpiry(Duration.ofSeconds(1))
            .serviceLevelObjectives()
            .register(registry);
  }

  void start() {
    Map<String, String> counterLabels = new LinkedHashMap<>();
    counterLabels.put("rabbitmq.amqp.published", "published");
    counterLabels.put("rabbitmq.amqp.published_accepted", "p. accepted");
    counterLabels.put("rabbitmq.amqp.consumed", "consumed");
    counterLabels.put("rabbitmq.amqp.consumed_accepted", "c. accepted");
    List<Counter> counters =
        counterLabels.keySet().stream()
            .map(n -> registry.get(n).counter())
            .collect(Collectors.toList());
    Map<String, Long> lastMetersValues = new ConcurrentHashMap<>(counters.size());
    counters.forEach(m -> lastMetersValues.put(m.getId().getName(), 0L));
    long startTime = System.nanoTime();
    AtomicLong lastTick = new AtomicLong(startTime);
    AtomicInteger reportCount = new AtomicInteger(1);
    ScheduledFuture<?> consoleReportingTask =
        executorService.scheduleAtFixedRate(
            () -> {
              if (checkActivity()) {
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
                              return String.format(
                                  "%s %d msg/s", counterLabels.get(meterName), rate);
                            })
                        .collect(Collectors.joining(", "));
                builder
                    .append(counterReport)
                    .append(", ")
                    .append(formatLatency("p. accepted latency", publishedAcceptedLatency))
                    .append(formatLatency("latency", latency));
                this.out.println(builder);
                reportCount.incrementAndGet();
              }
            },
            1,
            1,
            TimeUnit.SECONDS);

    this.closingSequence =
        () -> {
          consoleReportingTask.cancel(true);

          Duration d = Duration.ofNanos(System.nanoTime() - startTime);
          Duration duration = d.getSeconds() <= 0 ? Duration.ofSeconds(1) : d;

          StringBuilder builder = new StringBuilder("Summary: ");
          counterLabels.forEach(
              (name, label) ->
                  builder.append(
                      formatMeterSummary(label, registry.get(name).counter(), duration)));
          builder
              .append(formatLatencySummary("p. accepted latency", publishedAcceptedLatency))
              .append(", ");
          builder.append(formatLatencySummary("latency", latency));
          this.out.println();
          this.out.println(builder);
        };
  }

  @Override
  public void close() {
    this.closingSequence.run();
  }

  void latency(long latency, TimeUnit unit) {
    this.latency.record(latency, unit);
  }

  void publishedAcceptedlatency(long latency, TimeUnit unit) {
    this.publishedAcceptedLatency.record(latency, unit);
  }

  boolean checkActivity() {
    long currentPublishedCount = getPublishedCount();
    long currentConsumedCount = getConsumedCount();
    boolean activity =
        this.lastPublishedCount != currentPublishedCount
            || this.lastConsumedCount != currentConsumedCount;
    if (activity) {
      this.lastPublishedCount = currentPublishedCount;
      this.lastConsumedCount = currentConsumedCount;
    }
    return activity;
  }

  private long getPublishedCount() {
    return (long) this.registry.get("rabbitmq.amqp.published").counter().count();
  }

  private long getConsumedCount() {
    return (long) this.registry.get("rabbitmq.amqp.consumed").counter().count();
  }

  private static String formatLatency(String name, Timer timer) {
    HistogramSnapshot snapshot = timer.takeSnapshot();

    return String.format(
        name + " median/75th/95th/99th %.0f/%.0f/%.0f/%.0f ms",
        convertDuration(percentile(snapshot, 0.5).value()),
        convertDuration(percentile(snapshot, 0.75).value()),
        convertDuration(percentile(snapshot, 0.95).value()),
        convertDuration(percentile(snapshot, 0.99).value()));
  }

  private static String formatMeterSummary(String label, Counter counter, Duration duration) {
    return String.format(
        "%s %d msg/s, ", label, 1000 * (long) counter.count() / duration.toMillis());
  }

  private static String formatLatencySummary(String name, HistogramSupport histogram) {
    return String.format(
        name + " 95th %.0f ms",
        convertDuration(percentile(histogram.takeSnapshot(), 0.95).value()));
  }

  private static Number convertDuration(Number in) {
    return in instanceof Long ? in.longValue() / 1_000_000 : in.doubleValue() / 1_000_000;
  }

  private static ValueAtPercentile percentile(HistogramSnapshot snapshot, double expected) {
    for (ValueAtPercentile percentile : snapshot.percentileValues()) {
      if (percentile.percentile() == expected) {
        return percentile;
      }
    }
    return null;
  }
}
