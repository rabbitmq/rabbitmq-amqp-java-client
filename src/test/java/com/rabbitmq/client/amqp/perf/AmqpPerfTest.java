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
import static com.rabbitmq.client.amqp.Management.QueueType.*;
import static com.rabbitmq.client.amqp.impl.TestUtils.environmentBuilder;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.TestUtils;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import com.rabbitmq.client.amqp.metrics.MicrometerMetricsCollector;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.jboss.forge.roaster._shade.org.apache.felix.resolver.util.ArrayMap;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AmqpPerfTest {

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
        Connection connection = environment.
                connectionBuilder().
                listeners(context -> {
                    context.previousState();
                    context.currentState();
                    context.failureCause();
                    context.resource();
                }).
                recovery().
                activated(true).connectionBuilder().build();

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
                        management.queueDeletion().delete(q);
                        management.exchangeDeletion().delete(e);
                        management.close();
                    }
                };

        Runtime.getRuntime().addShutdownHook(new Thread(shutdownSequence::run));
//        management.queue().name("stream").type(STREAM).deadLetterExchange("aaaa").declare();
        try {
//            management.queueDeletion().delete("my-first-queue-j");
//            management.queue().name("my-first-queue-j").type(QUORUM).declare();
//
//            Publisher publisher1 = connection.publisherBuilder().queue("my-first-queue-j").build();
//
//            long startTime = System.currentTimeMillis();
//            AtomicInteger confirmed = new AtomicInteger(0);
//            int total = 5_000_000;
//            for (int i = 0; i < total ; i++) {
//
//                byte[] payload1 = new byte[10];
//                Message message1 = publisher1.message(payload1);
//                publisher1.publish(message1, context -> {
//                    if (confirmed.incrementAndGet() % 200_000 == 0) {
//                        long stopTime = System.currentTimeMillis();
//                        long elapsedTime = (stopTime - startTime)/ 1000;
//                        System.out.println("confirmed time:" + elapsedTime + " confirmed: " + confirmed.get() + " total: " + total);
//                    }
//                });
//            }

//            long stopTime = System.currentTimeMillis();
//            long elapsedTime = (stopTime - startTime)/ 1000;
//            System.out.println("sent time: " + elapsedTime);

//        management.queueDeletion().delete("alone");
//        publisher1.close();
//            Thread.sleep(300000000);

//            try {

//            try {
//                management.queue().name("e1").type(QUORUM).declare();
//                management.queue().name("e1").type(STREAM).declare();
//            } catch (Exception e1) {
//                e1.printStackTrace();
//            }


            try {

                management.queue().name("stream").type(STREAM).deadLetterExchange("aaaa").declare();
            } catch (Exception e1) {
                e1.printStackTrace();
            }

            management.queue().name("stream-1").type(STREAM).declare();


            management.queue().name("aaaaa").type(QUORUM).declare();
            List list = new ArrayList<>();
            list.add(1);
            list.add(2);
            list.add("aaaa");
            list.add(0.33);

            Map s = new LinkedHashMap();
            s.put("v_8", "p_8");
            s.put("v_1", 1);
            s.put("list", list);

            management.exchange().name("e").type(DIRECT).declare();
            management.binding().sourceExchange("e").destinationQueue("q").
                    key("k").arguments(s).bind();


            management.unbind().sourceExchange("e")
                    .destinationQueue("q").key("k").arguments(s).unbind();

            try {
                management.queue().name("q_是英国数学家").type(CLASSIC).declare();
            } catch (Exception e1) {
                e1.printStackTrace();
            }


            management.exchange().name("是英国数学家").type(DIRECT).declare();
            management.queue().name(q).type(QUORUM).declare();
            management.binding().sourceExchange(e).destinationQueue(q).key(rk).bind();
            /// { $"v_8", $"p_8" }, { $"v_1", 1 }, { $"v_r", 1000L },
            connection
                    .consumerBuilder()
                    .queue(q)
                    .initialCredits(1000)
                    .messageHandler(
                            (context, message) -> {
                                context.accept();
                                try {
                                    long time = readLong(message.body());
                                    metrics.latency(System.currentTimeMillis() - time, TimeUnit.MILLISECONDS);
                                } catch (Exception ex) {
                                    // not able to read the body, maybe not a message from the
                                    // tool
                                }
                            })
                    .build();

            executorService.submit(
                    () -> {
                        Publisher publisher = connection.publisherBuilder().exchange(e).key(rk).build();
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
                            long creationTime = System.currentTimeMillis();
                            byte[] payload = new byte[msgSize];
                            writeLong(payload, creationTime);
                            Message message = publisher.message(payload);
                            publisher.publish(message, callback);
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
