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
package com.rabbitmq.client.amqp.docs;

import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import com.rabbitmq.client.amqp.metrics.MicrometerMetricsCollector;
import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.observation.micrometer.MicrometerObservationCollectorBuilder;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import javax.net.ssl.SSLContext;

class Api {

  void connectionSettings() {
    // tag::connection-settings[]
    Environment environment = new AmqpEnvironmentBuilder()
        .connectionSettings()
        .uri("amqp://guest:guest@localhost:5672/%2f") // <1>
        .environmentBuilder().build();

    Connection connection = environment.connectionBuilder()
        .uri("amqp://admin:admin@localhost:5672/%2f") // <2>
        .build();
    // end::connection-settings[]
  }

  void subscriptionListener() {
    Connection connection = null;
    // tag::subscription-listener[]
    connection.consumerBuilder()
        .queue("some-stream")
        .subscriptionListener(ctx -> {  // <1>
          long offset = getOffsetFromExternalStore();  // <2>
          ctx.streamOptions().offset(offset + 1);  // <3>
        })
        .messageHandler((ctx, msg) -> {
          // message handling code...

          long offset = (long) msg.annotation("x-stream-offset");  // <4>
          storeOffsetInExternalStore(offset);  // <5>
        })
        .build();
    // end::subscription-listener[]
  }

  long getOffsetFromExternalStore() {
    return 0L;
  }

  void storeOffsetInExternalStore(long offset) {

  }

  void metricsCollectorMicrometerPrometheus() {
    String queue = null;
    // tag::metrics-micrometer-prometheus[]
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry( // <1>
        PrometheusConfig.DEFAULT
    );
    MetricsCollector collector = new MicrometerMetricsCollector(registry); // <2>

    Environment environment = new AmqpEnvironmentBuilder()
        .metricsCollector(collector) // <3>
        .build();

    Connection connection = environment.connectionBuilder().build(); // <4>
    // end::metrics-micrometer-prometheus[]
  }

  void micrometerObservation() {
    ObservationRegistry observationRegistry = ObservationRegistry.NOOP;
    // tag::micrometer-observation[]
    Environment environment = new AmqpEnvironmentBuilder()
        .observationCollector(new MicrometerObservationCollectorBuilder()  // <1>
            .registry(observationRegistry).build())  // <2>
        .build();
    // end::micrometer-observation[]
  }

  void oauth2() {
    SSLContext sslContext = null;
    // tag::oauth2[]
    Environment environment = new AmqpEnvironmentBuilder()
        .connectionSettings().oauth2()  // <1>
        .tokenEndpointUri("https://localhost:8443/uaa/oauth/token/")  // <2>
        .clientId("rabbitmq").clientSecret("rabbitmq")  // <3>
        .grantType("password")  // <4>
        .parameter("username", "rabbit_super")  // <5>
        .parameter("password", "rabbit_super")  // <5>
        .tls().sslContext(sslContext).oauth2()  // <6>
        .shared(true)  // <7>
        .connection()
        .environmentBuilder().build();
    // end::oauth2[]
  }

}
