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
package com.rabbitmq.amqp.client.impl;

import static com.rabbitmq.amqp.client.metrics.MetricsCollector.ConsumeDisposition.*;
import static com.rabbitmq.amqp.client.metrics.MetricsCollector.PublishDisposition.FAILED;
import static java.lang.String.format;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.rabbitmq.amqp.client.*;
import com.rabbitmq.amqp.client.metrics.MetricsCollector;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MetricsCollectorTest {

  @Mock MetricsCollector metricsCollector;

  @Test
  void metricsShouldBeCollected() throws Exception {
    try (Environment environment =
        TestUtils.environmentBuilder().metricsCollector(metricsCollector).build()) {
      verify(metricsCollector, never()).openConnection();
      String c1Name = UUID.randomUUID().toString();
      CountDownLatch recoveredLatch = new CountDownLatch(1);
      Connection c1 =
          ((AmqpConnectionBuilder) environment.connectionBuilder())
              .name(c1Name)
              .listeners(recoveredListener(recoveredLatch))
              .recovery()
              .backOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofMillis(100)))
              .connectionBuilder()
              .build();
      verify(metricsCollector, times(1)).openConnection();

      Cli.closeConnection(c1Name);
      TestUtils.assertThat(recoveredLatch).completes();
      // a recovered connection is not closed
      verify(metricsCollector, never()).closeConnection();

      AtomicBoolean c2Closed = new AtomicBoolean(false);
      String c2Name = UUID.randomUUID().toString();
      Connection c2 =
          ((AmqpConnectionBuilder) environment.connectionBuilder())
              .name(c2Name)
              .recovery()
              .activated(false)
              .connectionBuilder()
              .listeners(
                  context -> {
                    if (context.currentState() == Resource.State.CLOSED) {
                      c2Closed.set(true);
                    }
                  })
              .build();
      verify(metricsCollector, times(2)).openConnection();

      CountDownLatch c2ClosedLatch = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                c2ClosedLatch.countDown();
                return null;
              })
          .when(metricsCollector)
          .closeConnection();
      Cli.closeConnection(c2Name);
      TestUtils.assertThat(c2ClosedLatch).completes();
      // the connection is closed because automatic recovery was not activated
      verify(metricsCollector, times(1)).closeConnection();

      String q = c1.management().queue().exclusive(true).declare().name();
      verify(metricsCollector, never()).openPublisher();
      Publisher publisher = c1.publisherBuilder().build();
      verify(metricsCollector, times(1)).openPublisher();

      verify(metricsCollector, never()).publish();
      verify(metricsCollector, never()).publishDisposition(any());
      CountDownLatch disposed = new CountDownLatch(1);
      publisher.publish(publisher.message().toAddress().queue(q).message(), disposed(disposed));
      verify(metricsCollector, times(1)).publish();
      TestUtils.assertThat(disposed).completes();
      verify(metricsCollector, times(1))
          .publishDisposition(MetricsCollector.PublishDisposition.ACCEPTED);

      disposed = new CountDownLatch(1);
      // we published to a non-existing queue
      publisher.publish(
          publisher.message().toAddress().queue(UUID.randomUUID().toString()).message(),
          disposed(disposed));
      verify(metricsCollector, times(2)).publish();
      TestUtils.assertThat(disposed).completes();
      // the last message could not be routed, so its disposition state is failed
      verify(metricsCollector, times(1)).publishDisposition(FAILED);
      verify(metricsCollector, times(2)).publishDisposition(any());

      verify(metricsCollector, never()).consume();
      verify(metricsCollector, never()).consumeDisposition(any());
      AtomicInteger consumedCount = new AtomicInteger(0);
      @SuppressWarnings("unchecked")
      java.util.function.Consumer<Consumer.Context> handler =
          mock(java.util.function.Consumer.class);
      // our handler will accept, requeue, and discard
      doAnswer(
              invocation -> {
                invocation.getArgument(0, Consumer.Context.class).accept();
                return null;
              })
          .doAnswer(
              invocation -> {
                invocation.getArgument(0, Consumer.Context.class).requeue();
                return null;
              })
          .doAnswer(
              invocation -> {
                invocation.getArgument(0, Consumer.Context.class).discard();
                return null;
              })
          .when(handler)
          .accept(any());
      Consumer consumer =
          c1.consumerBuilder()
              .queue(q)
              .messageHandler(
                  (ctx, msg) -> {
                    handler.accept(ctx);
                    consumedCount.incrementAndGet();
                  })
              .build();
      TestUtils.waitAtMost(
          () -> consumedCount.get() == 1,
          () -> format("Expected 1 message, but got %d.", consumedCount.get()));
      // the first message is accepted
      verify(metricsCollector, times(1)).consume();
      verify(metricsCollector, times(1))
          .consumeDisposition(MetricsCollector.ConsumeDisposition.ACCEPTED);

      // publishing another message
      publisher.publish(publisher.message().toAddress().queue(q).message(), ctx -> {});

      // the message is requeued, so it comes back, and it's then discarded
      TestUtils.waitAtMost(() -> consumedCount.get() == 1 + 2);
      verify(metricsCollector, times(1 + 2)).consume();
      verify(metricsCollector, times(1)).consumeDisposition(REQUEUED);
      verify(metricsCollector, times(1)).consumeDisposition(DISCARDED);

      verify(metricsCollector, never()).closePublisher();
      publisher.close();
      verify(metricsCollector, times(1)).closePublisher();

      verify(metricsCollector, never()).closeConsumer();
      consumer.close();
      verify(metricsCollector, times(1)).closeConsumer();

      // we create a publisher and a consumer to make sure they are recorded as closed
      // when the connection is closed
      c1.publisherBuilder().build();
      c1.consumerBuilder().queue(q).messageHandler((ctx, msg) -> {}).build();

      c1.close();
    }
    verify(metricsCollector, times(2)).closePublisher();
    verify(metricsCollector, times(2)).closeConsumer();
    verify(metricsCollector, times(2)).closeConnection();
  }

  private static Resource.StateListener recoveredListener(CountDownLatch latch) {
    return context -> {
      if (context.previousState() == Resource.State.RECOVERING
          && context.currentState() == Resource.State.OPEN) {
        latch.countDown();
      }
    };
  }

  private static Publisher.Callback disposed(CountDownLatch latch) {
    return ctx -> latch.countDown();
  }
}
