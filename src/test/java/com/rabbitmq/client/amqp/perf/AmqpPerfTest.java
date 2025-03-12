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

import static com.rabbitmq.client.amqp.Management.ExchangeType.DIRECT;
import static com.rabbitmq.client.amqp.Management.QueueType.QUORUM;
import static com.rabbitmq.client.amqp.impl.TestUtils.environmentBuilder;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.Resource;
import com.rabbitmq.client.amqp.impl.TestUtils;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import com.rabbitmq.client.amqp.metrics.MicrometerMetricsCollector;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpPerfTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpPerfTest.class);

  /*
  ./mvnw -q clean test-compile exec:java \
    -Dexec.mainClass=com.rabbitmq.client.amqp.perf.AmqpPerfTest \
    -Dexec.classpathScope="test"
     */
  public static void main(String[] args) throws IOException {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    MetricsCollector collector = new MicrometerMetricsCollector(registry);

    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10);

    PrintWriter out = new PrintWriter(System.out, true);
    PerformanceMetrics metrics = new PerformanceMetrics(registry, executorService, out);

    String e = TestUtils.name(AmqpPerfTest.class, "main");
    String q = TestUtils.name(AmqpPerfTest.class, "main");
    String rk = "foo";
    Environment environment = environmentBuilder().metricsCollector(collector).build();
    Connection connection = environment.connectionBuilder().build();
    Management management = connection.management();

    int monitoringPort = 8080;
    HttpServer monitoringServer = startMonitoringServer(monitoringPort, registry);

    CountDownLatch shutdownLatch = new CountDownLatch(1);

    AtomicBoolean hasShutDown = new AtomicBoolean(false);
    Runnable shutdownSequence =
        () -> {
          if (hasShutDown.compareAndSet(false, true)) {
            monitoringServer.stop(0);
            metrics.close();
            executorService.shutdownNow();
            shutdownLatch.countDown();
            management.queueDelete(q);
            management.exchangeDelete(e);
            management.close();
          }
        };

    Runtime.getRuntime().addShutdownHook(new Thread(shutdownSequence::run));
    try {
      management.exchange().name(e).type(DIRECT).declare();
      management.queue().name(q).type(QUORUM).declare();
      management.binding().sourceExchange(e).destinationQueue(q).key(rk).bind();

      java.util.function.Consumer<Message> recordMessage =
          msg -> {
            try {
              long time = readLong(msg.body());
              metrics.latency(System.currentTimeMillis() - time, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
              // not able to read the body, maybe not a message from the
              // tool
            }
          };

      int initialCredits = 1000;
      Consumer.MessageHandler handler;
      int disposeEvery = 1;

      if (disposeEvery <= 1) {
        handler =
            (context, message) -> {
              recordMessage.accept(message);
              context.accept();
            };
      } else {
        AtomicReference<Consumer.BatchContext> batch = new AtomicReference<>();
        handler =
            (context, message) -> {
              recordMessage.accept(message);
              if (batch.get() == null) {
                batch.set(context.batch(disposeEvery));
              }
              batch.get().add(context);
              if (batch.get().size() == disposeEvery) {
                batch.get().accept();
                batch.set(null);
              }
            };
      }

      connection
          .consumerBuilder()
          .listeners(
              context -> {
                if (context.currentState() == Resource.State.RECOVERING) {
                  LOGGER.info("Consumer is recovering...");
                }
              })
          .queue(q)
          .initialCredits(initialCredits)
          .messageHandler(handler)
          .build();

      executorService.submit(
          () -> {
            AtomicBoolean shouldPublish = new AtomicBoolean(false);
            Publisher publisher =
                connection
                    .publisherBuilder()
                    .exchange(e)
                    .key(rk)
                    .listeners(
                        context -> {
                          if (context.currentState() == Resource.State.OPEN) {
                            shouldPublish.set(true);
                          } else {
                            if (context.currentState() == Resource.State.RECOVERING) {
                              LOGGER.info("Publisher is recovering...");
                            }
                            shouldPublish.set(false);
                          }
                        })
                    .build();
            Publisher.Callback callback =
                context -> {
                  try {
                    long time = readLong(context.message().body());
                    metrics.publishedAcceptedlatency(
                        System.currentTimeMillis() - time, TimeUnit.MILLISECONDS);
                  } catch (Exception ex) {
                    // not able to read the body, should not happen
                  }
                };
            int msgSize = 10;
            while (!Thread.currentThread().isInterrupted()) {
              if (shouldPublish.get()) {
                long creationTime = System.currentTimeMillis();
                byte[] payload = new byte[msgSize];
                writeLong(payload, creationTime);
                try {
                  Message message = publisher.message(payload);
                  publisher.publish(message, callback);
                } catch (Exception ex) {
                  LOGGER.info("Error while trying to publish: {}", ex.getMessage());
                }
              } else {
                try {
                  Thread.sleep(1000L);
                } catch (InterruptedException ex) {
                  Thread.interrupted();
                }
              }
            }
          });
      out.println("Prometheus endpoint started on http://localhost:" + monitoringPort + "/metrics");
      metrics.start();
      shutdownLatch.await();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    } finally {
      shutdownSequence.run();
    }
  }

  static void writeLong(byte[] array, long value) {
    // from Guava Longs
    for (int i = 7; i >= 0; i--) {
      array[i] = (byte) (value & 0xffL);
      value >>= 8;
    }
  }

  static long readLong(byte[] array) {
    // from Guava Longs
    return (array[0] & 0xFFL) << 56
        | (array[1] & 0xFFL) << 48
        | (array[2] & 0xFFL) << 40
        | (array[3] & 0xFFL) << 32
        | (array[4] & 0xFFL) << 24
        | (array[5] & 0xFFL) << 16
        | (array[6] & 0xFFL) << 8
        | (array[7] & 0xFFL);
  }

  private static HttpServer startMonitoringServer(
      int monitoringPort, PrometheusMeterRegistry registry) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(monitoringPort), 0);

    server.createContext(
        "/metrics",
        exchange -> {
          exchange.getResponseHeaders().set("Content-Type", "text/plain");
          byte[] content = registry.scrape().getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, content.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(content);
          }
        });
    server.start();
    return server;
  }
}
