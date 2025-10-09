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
package com.rabbitmq.client.amqp.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link MetricsCollector} implementation using <a href="https://micrometer.io/">Micrometer</a>.
 */
public class MicrometerMetricsCollector implements MetricsCollector {

  private final AtomicLong connections;
  private final AtomicLong publishers;
  private final AtomicLong consumers;
  private final Counter publish, publishAccepted, publishRejected, publishReleased;
  private final Counter consume, consumeAccepted, consumeRequeued, consumeDiscarded;
  private final Counter writtenBytes;
  private final Counter readBytes;

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
    this.publishRejected = registry.counter(prefix + ".published_rejected", tags);
    this.publishReleased = registry.counter(prefix + ".published_released", tags);
    this.consume = registry.counter(prefix + ".consumed", tags);
    this.consumeAccepted = registry.counter(prefix + ".consumed_accepted", tags);
    this.consumeRequeued = registry.counter(prefix + ".consumed_requeued", tags);
    this.consumeDiscarded = registry.counter(prefix + ".consumed_discarded", tags);
    this.writtenBytes = registry.counter(prefix + ".written_bytes", tags);
    this.readBytes = registry.counter(prefix + ".read_bytes", tags);
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
      case REJECTED:
        this.publishRejected.increment();
        break;
      case RELEASED:
        this.publishReleased.increment();
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

  @Override
  public void writtenBytes(int writtenBytes) {
    this.writtenBytes.increment(writtenBytes);
  }

  @Override
  public void readBytes(int readBytes) {
    this.readBytes.increment(readBytes);
  }
}
