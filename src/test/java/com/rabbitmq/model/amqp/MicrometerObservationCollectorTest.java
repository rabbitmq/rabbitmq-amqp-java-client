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

import static com.rabbitmq.model.amqp.TestUtils.assertThat;
import static com.rabbitmq.model.amqp.TestUtils.waitAtMost;

import com.rabbitmq.model.Connection;
import com.rabbitmq.model.Environment;
import com.rabbitmq.model.Management;
import com.rabbitmq.model.Publisher;
import com.rabbitmq.model.observation.micrometer.MicrometerObservationCollectorBuilder;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.SampleTestRunner;
import io.micrometer.tracing.test.reporter.BuildingBlocks;
import io.micrometer.tracing.test.simple.SpanAssert;
import io.micrometer.tracing.test.simple.SpansAssert;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Nested;

public class MicrometerObservationCollectorTest {

  private static final byte[] PAYLOAD = "msg".getBytes(StandardCharsets.UTF_8);

  private abstract static class IntegrationTest extends SampleTestRunner {

    @Override
    public TracingSetup[] getTracingSetup() {
      return new TracingSetup[] {TracingSetup.IN_MEMORY_BRAVE, TracingSetup.ZIPKIN_BRAVE};
    }
  }

  @Nested
  class PublishConsume extends IntegrationTest {

    @Override
    public SampleTestRunner.SampleTestRunnerConsumer yourCode() {
      return (buildingBlocks, meterRegistry) -> {
        try (Environment env =
            new AmqpEnvironmentBuilder()
                .observationCollector(
                    new MicrometerObservationCollectorBuilder()
                        .registry(getObservationRegistry())
                        .build())
                .build()) {
          Connection publisherConnection = env.connectionBuilder().build();
          Connection consumerConnection = env.connectionBuilder().build();

          String e = TestUtils.name(MicrometerObservationCollectorTest.class, "PublishConsume");
          publisherConnection
              .management()
              .exchange(e)
              .autoDelete(true)
              .type(Management.ExchangeType.FANOUT)
              .declare();

          String q = consumerConnection.management().queue().exclusive(true).declare().name();
          consumerConnection
              .management()
              .binding()
              .sourceExchange(e)
              .destinationQueue(q)
              .key("foo")
              .bind();

          AtomicReference<CountDownLatch> consumeLatch =
              new AtomicReference<>(new CountDownLatch(1));
          consumerConnection
              .consumerBuilder()
              .queue(q)
              .messageHandler(
                  (ctx, msg) -> {
                    ctx.accept();
                    consumeLatch.get().countDown();
                  })
              .build();

          Publisher publisher =
              publisherConnection.publisherBuilder().exchange(e).key("foo").build();

          UUID messageId = UUID.randomUUID();
          long correlationId = 42;
          publisher.publish(
              publisher.message(PAYLOAD).messageId(messageId).correlationId(correlationId),
              ctx -> {});

          TestUtils.assertThat(consumeLatch).completes();

          waitAtMost(() -> buildingBlocks.getFinishedSpans().size() == 2);
          SpansAssert.assertThat(buildingBlocks.getFinishedSpans()).haveSameTraceId().hasSize(2);
          SpanAssert.assertThat(lastPublish(buildingBlocks))
              .hasNameEqualTo(e + " publish")
              .hasTag("messaging.rabbitmq.destination.routing_key", "foo")
              .hasTag("messaging.destination.name", e)
              .hasTag("messaging.message.payload_size_bytes", String.valueOf(PAYLOAD.length))
              .hasTag("messaging.message.id", messageId.toString())
              .hasTag("messaging.message.conversation_id", String.valueOf(correlationId))
              .hasTagWithKey("net.sock.peer.addr")
              .hasTag("net.sock.peer.port", "5672")
              .hasTag("net.protocol.name", "amqp")
              .hasTag("net.protocol.version", "1.0");

          SpanAssert.assertThat(lastProcess(buildingBlocks))
              .hasNameEqualTo(q + " process")
              .hasTag("messaging.rabbitmq.destination.routing_key", "foo")
              .hasTag("messaging.destination.name", e)
              .hasTag("messaging.source.name", q)
              .hasTag("messaging.message.payload_size_bytes", String.valueOf(PAYLOAD.length))
              .hasTag("messaging.message.id", messageId.toString())
              .hasTag("messaging.message.conversation_id", String.valueOf(correlationId))
              .hasTag("net.protocol.name", "amqp")
              .hasTag("net.protocol.version", "1.0");
          waitAtMost(
              () ->
                  getMeterRegistry().find("rabbitmq.amqp.publish").timer() != null
                      && getMeterRegistry().find("rabbitmq.amqp.process").timer() != null);
          getMeterRegistry()
              .get("rabbitmq.amqp.publish")
              .tag("messaging.operation", "publish")
              .tag("messaging.system", "rabbitmq")
              .timer();
          getMeterRegistry()
              .get("rabbitmq.amqp.process")
              .tag("messaging.operation", "process")
              .tag("messaging.system", "rabbitmq")
              .timer();

          publisher.close();
          publisher = publisherConnection.publisherBuilder().exchange(e).build();

          consumeLatch.set(new CountDownLatch(1));
          messageId = UUID.randomUUID();
          publisher.publish(publisher.message(PAYLOAD).messageId(messageId), ctx -> {});
          assertThat(consumeLatch).completes();
          waitAtMost(() -> buildingBlocks.getFinishedSpans().size() == 4);
          SpansAssert.assertThat(buildingBlocks.getFinishedSpans()).haveSameTraceId().hasSize(4);
          SpanAssert.assertThat(lastPublish(buildingBlocks))
              .hasNameEqualTo(e + " publish")
              .hasTag("messaging.message.id", messageId.toString())
              .hasTag("messaging.rabbitmq.destination.routing_key", "")
              .hasTag("messaging.destination.name", e);
          SpanAssert.assertThat(lastProcess(buildingBlocks))
              .hasNameEqualTo(q + " process")
              .hasTag("messaging.message.id", messageId.toString())
              .hasTag("messaging.rabbitmq.destination.routing_key", "")
              .hasTag("messaging.destination.name", e)
              .hasTag("messaging.source.name", q);

          publisher.close();
          publisher = publisherConnection.publisherBuilder().queue(q).build();

          consumeLatch.set(new CountDownLatch(1));
          messageId = UUID.randomUUID();
          publisher.publish(publisher.message(PAYLOAD).messageId(messageId), ctx -> {});
          assertThat(consumeLatch).completes();
          waitAtMost(() -> buildingBlocks.getFinishedSpans().size() == 6);
          SpansAssert.assertThat(buildingBlocks.getFinishedSpans()).haveSameTraceId().hasSize(6);
          SpanAssert.assertThat(lastPublish(buildingBlocks))
              .hasNameEqualTo("amq.default publish")
              .hasTag("messaging.message.id", messageId.toString())
              .hasTag("messaging.rabbitmq.destination.routing_key", q)
              .hasTag("messaging.destination.name", "");
          SpanAssert.assertThat(lastProcess(buildingBlocks))
              .hasNameEqualTo(q + " process")
              .hasTag("messaging.message.id", messageId.toString())
              .hasTag("messaging.rabbitmq.destination.routing_key", q)
              .hasTag("messaging.destination.name", "")
              .hasTag("messaging.source.name", q);

          publisher.close();
          publisher = publisherConnection.publisherBuilder().build();

          consumeLatch.set(new CountDownLatch(1));
          messageId = UUID.randomUUID();
          publisher.publish(
              publisher
                  .message(PAYLOAD)
                  .messageId(messageId)
                  .toAddress()
                  .exchange(e)
                  .key("foo")
                  .message(),
              ctx -> {});
          assertThat(consumeLatch).completes();
          waitAtMost(() -> buildingBlocks.getFinishedSpans().size() == 8);
          SpansAssert.assertThat(buildingBlocks.getFinishedSpans()).haveSameTraceId().hasSize(8);
          SpanAssert.assertThat(lastPublish(buildingBlocks))
              .hasNameEqualTo(e + " publish")
              .hasTag("messaging.message.id", messageId.toString())
              .hasTag("messaging.rabbitmq.destination.routing_key", "foo")
              .hasTag("messaging.destination.name", e);
          SpanAssert.assertThat(lastProcess(buildingBlocks))
              .hasNameEqualTo(q + " process")
              .hasTag("messaging.message.id", messageId.toString())
              .hasTag("messaging.rabbitmq.destination.routing_key", "foo")
              .hasTag("messaging.destination.name", e)
              .hasTag("messaging.source.name", q);
        }
      };
    }
  }

  private static FinishedSpan lastPublish(BuildingBlocks blocks) {
    return lastWithNameEnding(blocks, "publish");
  }

  private static FinishedSpan lastProcess(BuildingBlocks blocks) {
    return lastWithNameEnding(blocks, "process");
  }

  private static FinishedSpan lastWithNameEnding(BuildingBlocks blocks, String nameEnding) {
    List<FinishedSpan> spans = blocks.getFinishedSpans();
    for (int i = spans.size() - 1; i >= 0; i--) {
      FinishedSpan span = spans.get(i);
      if (span.getName().endsWith(nameEnding)) {
        return span;
      }
    }
    throw new IllegalStateException();
  }
}
