// Copyright (c) 2025 Broadcom. All Rights Reserved.
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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Message;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class BatchDispositionTest {

  static final int failureLimit = 1;
  static int batchSize = 100;
  static int messageCount = 100_000;
  static final Random random = new Random();
  static final MetricRegistry metrics = new MetricRegistry();
  static final Counter successfulCount = metrics.counter("successful");
  static final Counter failedCount = metrics.counter("failed");
  static final Counter dispositionFrameCount = metrics.counter("disposition");
  static final Counter dispositionFirstOnlyFrameCount = metrics.counter("disposition.first.only");
  static final Counter dispositionFirstLastFrameCount = metrics.counter("disposition.first.last");
  static final Histogram dispositionRangeSize = metrics.histogram("disposition.range.size");
  static final int DISPOSITION_FIRST_ONLY_SIZE = 94;
  static final int DISPOSITION_FIRST_LAST_SIZE = 98;

  @Test
  @Disabled
  void test() {
    AtomicReference<Consumer.BatchContext> batchReference = new AtomicReference<>();
    Consumer.MessageHandler handler =
        new Consumer.MessageHandler() {
          Consumer.BatchContext batch;

          @Override
          public void handle(Consumer.Context context, Message message) {
            if (batch == null) {
              batch = context.batch();
            }

            boolean success = processMessage(message);
            if (success) {
              successfulCount.inc();
              batch.add(context);
            } else {
              failedCount.inc();
              context.discard();
            }

            if (batch.size() == batchSize) {
              batch.accept();
              batch = null;
            }
            batchReference.set(batch);
          }
        };

    IntStream.range(0, messageCount)
        .forEach(
            i -> {
              handler.handle(new TestContext(i), null);
            });

    if (batchReference.get() == null) {
      batchReference.get().accept();
    }

    final ConsoleReporter reporter =
        ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    reporter.start(1, TimeUnit.SECONDS);
    reporter.report();

    long firstOnlyBytes = dispositionFirstOnlyFrameCount.getCount() * DISPOSITION_FIRST_ONLY_SIZE;
    long firstLastBytes = dispositionFirstLastFrameCount.getCount() * DISPOSITION_FIRST_LAST_SIZE;
    System.out.printf(
        "Number of first-only disposition frames: %d (bytes: %d)%n",
        dispositionFirstOnlyFrameCount.getCount(), firstOnlyBytes);
    System.out.printf(
        "Number of first-last disposition frames: %d (bytes: %d)%n",
        dispositionFirstLastFrameCount.getCount(), firstLastBytes);

    long monoAckBytes = (long) messageCount * DISPOSITION_FIRST_ONLY_SIZE;
    System.out.printf("Traffic with message-by-message disposition: %d%n", monoAckBytes);
    System.out.printf(
        "Gain: %s%n",
        (double) (monoAckBytes - (firstOnlyBytes + firstLastBytes)) / (double) monoAckBytes);
  }

  static class TestContext implements Consumer.Context {

    private final long deliveryId;

    TestContext(long deliveryId) {
      this.deliveryId = deliveryId;
    }

    @Override
    public void accept() {
      dispose();
    }

    @Override
    public void discard() {
      dispose();
    }

    @Override
    public void discard(Map<String, Object> annotations) {
      dispose();
    }

    @Override
    public void requeue() {
      dispose();
    }

    @Override
    public void requeue(Map<String, Object> annotations) {
      dispose();
    }

    private void dispose() {
      dispositionFrameCount.inc();
      dispositionRangeSize.update(1);
      dispositionFirstOnlyFrameCount.inc();
    }

    @Override
    public Consumer.BatchContext batch() {
      return new TestBatchContext();
    }
  }

  static class TestBatchContext implements Consumer.BatchContext {

    private final List<TestContext> contexts = new ArrayList<>(batchSize);
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    @Override
    public void add(Consumer.Context context) {
      this.contexts.add((TestContext) context);
    }

    @Override
    public int size() {
      return this.contexts.size();
    }

    @Override
    public void accept() {
      dispose();
    }

    @Override
    public void discard() {
      dispose();
    }

    @Override
    public void discard(Map<String, Object> annotations) {
      dispose();
    }

    @Override
    public void requeue() {
      dispose();
    }

    @Override
    public void requeue(Map<String, Object> annotations) {
      dispose();
    }

    private void dispose() {
      if (this.disposed.compareAndSet(false, true)) {
        List<Long> deliveryIds =
            this.contexts.stream().map(c -> c.deliveryId).collect(Collectors.toList());
        long[][] ranges = SerialNumberUtils.ranges(deliveryIds, Long::longValue);
        for (long[] range : ranges) {
          dispositionFrameCount.inc();
          long rangeSize = range[1] - range[0] + 1;
          dispositionRangeSize.update(rangeSize);
          if (rangeSize > 1) {
            dispositionFirstLastFrameCount.inc();
          } else {
            dispositionFirstOnlyFrameCount.inc();
          }
        }
      }
    }

    @Override
    public Consumer.BatchContext batch() {
      return this;
    }
  }

  static boolean processMessage(Message message) {
    int v = random.nextInt(100);
    return v >= failureLimit;
  }
}
