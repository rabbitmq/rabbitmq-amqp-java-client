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
package com.rabbitmq.model.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

public class MicrometerMetricsCollector implements MetricsCollector {

  private final AtomicLong connections;
  private final AtomicLong publishers;
  private final AtomicLong consumers;
  private final Counter publish, publishAccepted, publishFailed;
  private final Counter consume, consumeAccepted, consumeRequeued, consumeDiscarded;

  public MicrometerMetricsCollector(MeterRegistry registry) {
    this(registry, "rabbitmq.amqp");
  }

  public MicrometerMetricsCollector(final MeterRegistry registry, final String prefix) {
    this(registry, prefix, Collections.emptyList());
  }

  public MicrometerMetricsCollector(
      final MeterRegistry registry, final String prefix, final String... tags) {
    this(registry, prefix, Tags.of(tags));
  }

  public MicrometerMetricsCollector(
      final MeterRegistry registry, final String prefix, final Iterable<Tag> tags) {
    this.connections = registry.gauge(prefix + ".connections", tags, new AtomicLong(0));
    this.publishers = registry.gauge(prefix + ".publishers", tags, new AtomicLong(0));
    this.consumers = registry.gauge(prefix + ".consumers", tags, new AtomicLong(0));
    this.publish = registry.counter(prefix + ".published", tags);
    this.publishAccepted = registry.counter(prefix + ".published_accepted", tags);
    this.publishFailed = registry.counter(prefix + ".published_failed", tags);
    this.consume = registry.counter(prefix + ".consumed", tags);
    this.consumeAccepted = registry.counter(prefix + ".consumed_accepted", tags);
    this.consumeRequeued = registry.counter(prefix + ".consumed_requeued", tags);
    this.consumeDiscarded = registry.counter(prefix + ".consumed_discarded", tags);
  }

  @Override
  public void openConnection() {
    this.connections.incrementAndGet();
  }

  @Override
  public void closeConnection() {
    this.connections.decrementAndGet();
  }

  @Override
  public void openPublisher() {
    this.publishers.incrementAndGet();
  }

  @Override
  public void closePublisher() {
    this.publishers.decrementAndGet();
  }

  @Override
  public void openConsumer() {
    this.consumers.incrementAndGet();
  }

  @Override
  public void closeConsumer() {
    this.consumers.decrementAndGet();
  }

  @Override
  public void publish() {
    this.publish.increment();
  }

  @Override
  public void publishDisposition(PublishDisposition disposition) {
    switch (disposition) {
      case ACCEPTED:
        this.publishAccepted.increment();
        break;
      case FAILED:
        this.publishFailed.increment();
        break;
      default:
        break;
    }
  }

  @Override
  public void consume() {
    this.consume.increment();
  }

  @Override
  public void consumeDisposition(ConsumeDisposition disposition) {
    switch (disposition) {
      case ACCEPTED:
        this.consumeAccepted.increment();
        break;
      case REQUEUED:
        this.consumeRequeued.increment();
        break;
      case DISCARDED:
        this.consumeDiscarded.increment();
        break;
      default:
        break;
    }
  }
}
