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

import static com.rabbitmq.client.amqp.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class MicrometerMetricsCollectorTest {

  @Test
  void simple() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsCollector collector = new MicrometerMetricsCollector(registry);

    assertThat(registry.get("rabbitmq.amqp.connections").gauge().value()).isZero();
    collector.openConnection();
    assertThat(registry.get("rabbitmq.amqp.connections").gauge().value()).isEqualTo(1);
    collector.openConnection();
    assertThat(registry.get("rabbitmq.amqp.connections").gauge().value()).isEqualTo(2);
    collector.closeConnection();
    assertThat(registry.get("rabbitmq.amqp.connections").gauge().value()).isEqualTo(1);

    assertThat(registry.get("rabbitmq.amqp.publishers").gauge().value()).isZero();
    collector.openPublisher();
    assertThat(registry.get("rabbitmq.amqp.publishers").gauge().value()).isEqualTo(1);
    collector.openPublisher();
    assertThat(registry.get("rabbitmq.amqp.publishers").gauge().value()).isEqualTo(2);
    collector.closePublisher();
    assertThat(registry.get("rabbitmq.amqp.publishers").gauge().value()).isEqualTo(1);

    assertThat(registry.get("rabbitmq.amqp.consumers").gauge().value()).isZero();
    collector.openConsumer();
    assertThat(registry.get("rabbitmq.amqp.consumers").gauge().value()).isEqualTo(1);
    collector.openConsumer();
    assertThat(registry.get("rabbitmq.amqp.consumers").gauge().value()).isEqualTo(2);
    collector.closeConsumer();
    assertThat(registry.get("rabbitmq.amqp.consumers").gauge().value()).isEqualTo(1);

    assertThat(registry.get("rabbitmq.amqp.published").counter().count()).isZero();
    collector.publish();
    assertThat(registry.get("rabbitmq.amqp.published").counter().count()).isEqualTo(1.0);
    collector.publish();
    assertThat(registry.get("rabbitmq.amqp.published").counter().count()).isEqualTo(2.0);

    assertThat(registry.get("rabbitmq.amqp.published_accepted").counter().count()).isZero();
    collector.publishDisposition(MetricsCollector.PublishDisposition.ACCEPTED);
    assertThat(registry.get("rabbitmq.amqp.published_accepted").counter().count()).isEqualTo(1.0);

    assertThat(registry.get("rabbitmq.amqp.published_rejected").counter().count()).isZero();
    collector.publishDisposition(MetricsCollector.PublishDisposition.REJECTED);
    assertThat(registry.get("rabbitmq.amqp.published_rejected").counter().count()).isEqualTo(1.0);

    assertThat(registry.get("rabbitmq.amqp.published_released").counter().count()).isZero();
    collector.publishDisposition(MetricsCollector.PublishDisposition.RELEASED);
    assertThat(registry.get("rabbitmq.amqp.published_released").counter().count()).isEqualTo(1.0);

    assertThat(registry.get("rabbitmq.amqp.consumed").counter().count()).isZero();
    collector.consume();
    assertThat(registry.get("rabbitmq.amqp.consumed").counter().count()).isEqualTo(1.0);
    collector.consume();
    collector.consume();
    assertThat(registry.get("rabbitmq.amqp.consumed").counter().count()).isEqualTo(3.0);

    assertThat(registry.get("rabbitmq.amqp.consumed_accepted").counter().count()).isZero();
    collector.consumeDisposition(MetricsCollector.ConsumeDisposition.ACCEPTED);
    assertThat(registry.get("rabbitmq.amqp.consumed_accepted").counter().count()).isEqualTo(1.0);

    assertThat(registry.get("rabbitmq.amqp.consumed_requeued").counter().count()).isZero();
    collector.consumeDisposition(MetricsCollector.ConsumeDisposition.REQUEUED);
    assertThat(registry.get("rabbitmq.amqp.consumed_requeued").counter().count()).isEqualTo(1.0);

    assertThat(registry.get("rabbitmq.amqp.consumed_discarded").counter().count()).isZero();
    collector.consumeDisposition(MetricsCollector.ConsumeDisposition.DISCARDED);
    assertThat(registry.get("rabbitmq.amqp.consumed_discarded").counter().count()).isEqualTo(1.0);
  }

  @Test
  void prometheus() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    MetricsCollector collector = new MicrometerMetricsCollector(registry);

    collector.openConnection();
    collector.openConnection();
    collector.closeConnection();

    collector.openPublisher();
    collector.openPublisher();
    collector.closePublisher();

    collector.openConsumer();
    collector.openConsumer();
    collector.closeConsumer();

    collector.publish();
    collector.publish();

    collector.publishDisposition(MetricsCollector.PublishDisposition.ACCEPTED);
    collector.publishDisposition(MetricsCollector.PublishDisposition.REJECTED);
    collector.publishDisposition(MetricsCollector.PublishDisposition.RELEASED);

    collector.consume();
    collector.consume();
    collector.consume();

    collector.consumeDisposition(MetricsCollector.ConsumeDisposition.ACCEPTED);
    collector.consumeDisposition(MetricsCollector.ConsumeDisposition.REQUEUED);
    collector.consumeDisposition(MetricsCollector.ConsumeDisposition.DISCARDED);

    Stream.of(
            "# TYPE rabbitmq_amqp_connections gauge",
            "rabbitmq_amqp_connections 1.0",
            "# TYPE rabbitmq_amqp_consumers gauge",
            "rabbitmq_amqp_consumers 1.0",
            "TYPE rabbitmq_amqp_publishers gauge",
            "rabbitmq_amqp_publishers 1.0",
            "# TYPE rabbitmq_amqp_published_total counter",
            "rabbitmq_amqp_published_total 2.0",
            "# TYPE rabbitmq_amqp_published_accepted_total counter",
            "rabbitmq_amqp_published_accepted_total 1.0",
            "# TYPE rabbitmq_amqp_published_rejected_total counter",
            "rabbitmq_amqp_published_rejected_total 1.0",
            "# TYPE rabbitmq_amqp_published_released_total counter",
            "rabbitmq_amqp_published_released_total 1.0",
            "# TYPE rabbitmq_amqp_consumed_total counter",
            "rabbitmq_amqp_consumed_total 3.0",
            "# TYPE rabbitmq_amqp_consumed_accepted_total counter",
            "rabbitmq_amqp_consumed_accepted_total 1.0",
            "# TYPE rabbitmq_amqp_consumed_discarded_total counter",
            "rabbitmq_amqp_consumed_discarded_total 1.0",
            "# TYPE rabbitmq_amqp_consumed_requeued_total counter",
            "rabbitmq_amqp_consumed_requeued_total 1.0")
        .forEach(expected -> waitAtMost(() -> registry.scrape().contains(expected)));
  }
}
