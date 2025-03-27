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
package com.rabbitmq.client.amqp.impl;

import static com.rabbitmq.client.amqp.Management.ExchangeType.FANOUT;
import static com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersion.RABBITMQ_4_1_0;
import static com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersion.RABBITMQ_4_2_0;
import static com.rabbitmq.client.amqp.impl.TestUtils.*;
import static java.nio.charset.StandardCharsets.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.qpid.protonj2.client.DeliveryMode.AT_LEAST_ONCE;
import static org.apache.qpid.protonj2.client.DeliveryMode.AT_MOST_ONCE;
import static org.apache.qpid.protonj2.client.DeliveryState.released;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.*;

import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.impl.TestConditions.BrokerVersionAtLeast;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.apache.qpid.protonj2.client.impl.ClientReceiver;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.junit.jupiter.api.*;

public class ClientTest {

  static Environment environment;
  static Management management;
  static com.rabbitmq.client.amqp.Connection connection;
  String q;

  @BeforeAll
  static void initAll() {
    environment = environmentBuilder().build();
    connection = environment.connectionBuilder().build();
    management = connection.management();
  }

  @BeforeEach
  void init(TestInfo info) {
    q = TestUtils.name(info);
    management.queue().name(q).declare();
  }

  @AfterEach
  void tearDown() {
    management.queueDelete(q);
  }

  @AfterAll
  static void tearDownAll() {
    connection.close();
    environment.close();
  }

  @Test
  void deliveryCount() throws Exception {
    try (Client client = client();
        Publisher publisher = connection.publisherBuilder().queue(q).build()) {
      int messageCount = 10;
      CountDownLatch publishLatch = new CountDownLatch(5);
      IntStream.range(0, messageCount)
          .forEach(
              ignored ->
                  publisher.publish(
                      publisher.message("".getBytes(UTF_8)), context -> publishLatch.countDown()));

      org.apache.qpid.protonj2.client.Connection protonConnection = connection(client);
      Receiver receiver = protonConnection.openReceiver("/queues/" + q, new ReceiverOptions());
      int receivedMessages = 0;
      while (receiver.receive(100, TimeUnit.MILLISECONDS) != null) {
        receivedMessages++;
      }
      assertThat(receivedMessages).isEqualTo(messageCount);
    }
  }

  @Test
  void largeMessage() throws Exception {
    try (Client client = client()) {
      int maxFrameSize = 1000;
      org.apache.qpid.protonj2.client.Connection connection =
          connection(client, o -> o.traceFrames(false).maxFrameSize(maxFrameSize));

      Sender sender =
          connection.openSender("/queues/" + q, new SenderOptions().deliveryMode(AT_LEAST_ONCE));
      byte[] body = new byte[maxFrameSize * 4];
      Arrays.fill(body, (byte) 'A');
      Tracker tracker = sender.send(Message.create(body));
      tracker.awaitSettlement();

      Receiver receiver =
          connection.openReceiver(
              "/queues/" + q,
              new ReceiverOptions()
                  .deliveryMode(AT_LEAST_ONCE)
                  .autoSettle(false)
                  .autoAccept(false));
      Delivery delivery = receiver.receive(100, SECONDS);
      assertThat(delivery).isNotNull();
      assertThat(delivery.message().body()).isEqualTo(body);
      delivery.disposition(DeliveryState.accepted(), true);
    }
  }

  @Test
  void largeMessageStreamSupport() throws Exception {
    int maxFrameSize = 1000;
    int chunkSize = 10;
    try (Client client = client()) {
      org.apache.qpid.protonj2.client.Connection connection =
          connection(client, o -> o.traceFrames(false).maxFrameSize(maxFrameSize));

      StreamSender sender =
          connection.openStreamSender(
              "/queues/" + q, new StreamSenderOptions().deliveryMode(AT_LEAST_ONCE));
      StreamSenderMessage message = sender.beginMessage();
      byte[] body = new byte[maxFrameSize * 4];
      Arrays.fill(body, (byte) 'A');

      OutputStreamOptions streamOptions = new OutputStreamOptions().bodyLength(body.length);
      OutputStream output = message.body(streamOptions);

      for (int i = 0; i < body.length; i += chunkSize) {
        output.write(body, i, chunkSize);
      }

      output.close();
      message.tracker().awaitSettlement();

      StreamReceiver receiver =
          connection.openStreamReceiver(
              "/queues/" + q,
              new StreamReceiverOptions()
                  .deliveryMode(AT_LEAST_ONCE)
                  .autoAccept(false)
                  .autoSettle(false));

      StreamDelivery delivery = receiver.receive();
      InputStream inputStream = delivery.message().body();

      byte[] chunk = new byte[chunkSize];

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream(body.length);
      int read;
      while ((read = inputStream.read(chunk)) != -1) {
        outputStream.write(chunk, 0, read);
      }

      inputStream.close();

      assertThat(outputStream.toByteArray()).isEqualTo(body);
      delivery.disposition(DeliveryState.accepted(), true);
    }
  }

  @Test
  void management(TestInfo info) throws Exception {
    String q = name(info);
    AtomicLong requestIdSequence = new AtomicLong(0);
    try (Client client = client()) {
      org.apache.qpid.protonj2.client.Connection connection =
          connection(client, o -> o.traceFrames(false));

      String linkPairName = "my-link-pair";
      String managementNodeAddress = "/management";
      String replyTo = "$me";
      Session session = connection.openSession();
      Sender sender =
          session.openSender(
              managementNodeAddress,
              new SenderOptions()
                  .deliveryMode(DeliveryMode.AT_MOST_ONCE)
                  .linkName(linkPairName)
                  .properties(Collections.singletonMap("paired", Boolean.TRUE)));

      Receiver receiver =
          session.openReceiver(
              managementNodeAddress,
              new ReceiverOptions()
                  .deliveryMode(DeliveryMode.AT_MOST_ONCE)
                  .linkName(linkPairName)
                  .properties(Collections.singletonMap("paired", Boolean.TRUE)));

      sender.openFuture().get(1, SECONDS);
      receiver.openFuture().get(1, SECONDS);

      Map<String, Object> body = new HashMap<>();
      body.put("durable", true);
      body.put("exclusive", false);
      body.put("auto_delete", false);
      body.put("arguments", Collections.emptyMap());
      UnsignedLong requestId = ulong(requestIdSequence.incrementAndGet());
      Message<Map<String, Object>> request =
          Message.create(body)
              .messageId(requestId)
              .to("/queues/" + q)
              .subject("PUT")
              .replyTo(replyTo);

      sender.send(request);

      Delivery delivery = receiver.receive(1, SECONDS);
      assertThat(delivery).isNotNull();
      Message<Map<String, Object>> response = delivery.message();
      assertThat(response.correlationId()).isEqualTo(requestId);
      assertThat(response.subject()).isEqualTo("201");
      assertThat(response.property("http:response")).isEqualTo("1.1");

      assertThat(response.body())
          .isNotNull()
          .isNotEmpty()
          .containsEntry("message_count", UnsignedLong.valueOf(0));

      requestId = ulong(requestIdSequence.incrementAndGet());
      request =
          Message.create((Map<String, Object>) null)
              .messageId(requestId)
              .to("/queues/" + q)
              .subject("DELETE")
              .replyTo(replyTo);

      sender.send(request);

      delivery = receiver.receive(1, SECONDS);
      assertThat(delivery).isNotNull();
      response = delivery.message();
      assertThat(response.correlationId()).isEqualTo(requestId);
      assertThat(response.subject()).isEqualTo("200");
      assertThat(response.property("http:response")).isEqualTo("1.1");

      Map<String, Object> responseBody = response.body();
      assertThat(responseBody).containsEntry("message_count", ulong(0));
    }
  }

  @Test
  void queueDeletionImpactOnReceiver(TestInfo info) throws Exception {
    String queue = name(info);
    try (Environment env = TestUtils.environmentBuilder().build();
        com.rabbitmq.client.amqp.Connection connection = env.connectionBuilder().build();
        Client client = client()) {
      connection.management().queue().name(queue).declare();

      org.apache.qpid.protonj2.client.Connection protonConnection = connection(client);
      Session session = protonConnection.openSession();
      Receiver receiver = session.openReceiver("/queues/" + queue);
      receiver.openFuture().get();
      Delivery delivery = receiver.tryReceive();
      assertThat(delivery).isNull();
      connection.management().queueDelete(queue);
      try {
        receiver.receive(10, SECONDS);
        fail("Receiver should have been closed after queue deletion");
      } catch (ClientLinkRemotelyClosedException e) {
        assertThat(e.getErrorCondition().condition()).isEqualTo("amqp:resource-deleted");
      }
    }
  }

  @Test
  void exchangeDeletionImpactOnSender(TestInfo info) throws Exception {
    String exchange = name(info);
    try (Environment env = TestUtils.environmentBuilder().build();
        com.rabbitmq.client.amqp.Connection connection = env.connectionBuilder().build();
        Client client = client()) {
      connection.management().exchange().name(exchange).type(FANOUT).declare();

      byte[] body = new byte[0];
      org.apache.qpid.protonj2.client.Connection protonConnection = connection(client);
      Session session = protonConnection.openSession();
      Sender sender =
          session.openSender(
              "/exchanges/" + exchange, new SenderOptions().deliveryMode(AT_LEAST_ONCE));
      Tracker tracker = sender.send(Message.create(body));
      tracker.awaitSettlement(10, SECONDS);
      assertThat(tracker.remoteState()).isEqualTo(released());

      connection.management().binding().sourceExchange(exchange).destinationQueue(q).bind();

      tracker = sender.send(Message.create(body));
      tracker.awaitSettlement(10, SECONDS);
      assertThat(tracker.remoteState()).isEqualTo(DeliveryState.accepted());

      connection.management().exchangeDelete(exchange);
      try {
        int count = 0;
        while (count++ < 10) {
          tracker = sender.send(Message.create(body));
          tracker.awaitSettlement(10, SECONDS);
          assertThat(tracker.remoteState()).isEqualTo(released());
          Thread.sleep(100);
        }
        fail("The sender link should have been closed after exchange deletion");
      } catch (ClientLinkRemotelyClosedException e) {
        assertThat(e.getErrorCondition().condition()).isEqualTo("amqp:not-found");
      }
    }
  }

  @Test
  @DisabledIfRabbitMqCtlNotSet
  void connectionClosing() {
    try (Client client = client()) {
      CountDownLatch connectedLatch = new CountDownLatch(1);
      AtomicBoolean called = new AtomicBoolean(false);
      org.apache.qpid.protonj2.client.Connection c =
          connection(
              client,
              o ->
                  o.connectedHandler(
                          (conn, connectionEvent) -> {
                            // called when connected for the first time
                            connectedLatch.countDown();
                          })
                      .disconnectedHandler(
                          (conn, disconnectionEvent) -> {
                            // called when the connection fails
                            called.set(true);
                          })
                      .interruptedHandler(
                          (conn, disconnectionEvent) -> {
                            // called when the connection fails and recovery is activated
                            called.set(true);
                          })
                      .reconnectedHandler(
                          (conn, connectionEvent) -> {
                            // called when the connection reconnects after a failure
                            called.set(true);
                          }));

      Assertions.assertThat(connectedLatch).completes();
      c.close();
      assertThat(called).isFalse();

      CountDownLatch disconnectedLatch = new CountDownLatch(1);
      String name = UUID.randomUUID().toString();
      c =
          connection(
              name,
              client,
              o ->
                  o.disconnectedHandler((conn, disconnectedEvent) -> disconnectedLatch.countDown())
                      .interruptedHandler((conn, disconnectionEvent) -> called.set(true))
                      .reconnectedHandler((conn, connectionEvent) -> called.set(true)));
      Cli.closeConnection(name);
      Assertions.assertThat(disconnectedLatch).completes();
      assertThat(called).isFalse();

      AtomicReference<org.apache.qpid.protonj2.client.Connection> cRef = new AtomicReference<>(c);
      assertThatThrownBy(() -> cRef.get().openReceiver("does-not-matter"))
          .isInstanceOf(ClientConnectionRemotelyClosedException.class);

      CountDownLatch interruptedLatch = new CountDownLatch(1);
      CountDownLatch reconnectedLatch = new CountDownLatch(1);
      name = UUID.randomUUID().toString();
      c =
          connection(
              name,
              client,
              o ->
                  o.disconnectedHandler((conn, disconnectedEvent) -> called.set(true))
                      .interruptedHandler(
                          (conn, disconnectionEvent) -> interruptedLatch.countDown())
                      .reconnectedHandler((conn, connectionEvent) -> reconnectedLatch.countDown())
                      .reconnectEnabled(true));

      Cli.closeConnection(name);
      Assertions.assertThat(interruptedLatch).completes();
      Assertions.assertThat(reconnectedLatch).completes();
      assertThat(called).isFalse();

      c.close();
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void dynamicReceiver() throws Exception {
    try (Client client = client()) {
      org.apache.qpid.protonj2.client.Connection c1 = connection(client);
      Session s1 = c1.openSession();
      ReceiverOptions receiverOptions = new ReceiverOptions();
      receiverOptions.sourceOptions().capabilities("temporary-queue");
      Receiver receiver = s1.openDynamicReceiver(receiverOptions);
      receiver.openFuture().get();
      assertThat(receiver.address()).isNotNull();

      org.apache.qpid.protonj2.client.Connection c2 = connection(client);
      Session s2 = c2.openSession();
      Sender sender = s2.openSender(receiver.address());
      String body = UUID.randomUUID().toString();
      sender.send(Message.create(body));

      Delivery delivery = receiver.receive(10, SECONDS);
      assertThat(delivery).isNotNull();
      assertThat(delivery.message().body()).isEqualTo(body);
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_2_0)
  void refuseLinkSenderToMissingExchangeShouldReturnNotFound() throws Exception {
    try (Client client = client()) {
      org.apache.qpid.protonj2.client.Connection c = connection(client);
      Session s = c.openSession();
      assertThatThrownBy(
              () -> ExceptionUtils.wrapGet(s.openSender("/exchanges/missing").openFuture()))
          .isInstanceOf(ClientLinkRemotelyClosedException.class)
          .asInstanceOf(throwable(ClientLinkRemotelyClosedException.class))
          .matches(e -> "amqp:not-found".equals(e.getErrorCondition().condition()));
      checkSession(s);
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_2_0)
  void refuseLinkSenderToInvalidAddressShouldReturnInvalidField() throws Exception {
    try (Client client = client()) {
      org.apache.qpid.protonj2.client.Connection c = connection(client);
      Session s = c.openSession();
      assertThatThrownBy(() -> ExceptionUtils.wrapGet(s.openSender("/fruit/orange").openFuture()))
          .isInstanceOf(ClientLinkRemotelyClosedException.class)
          .asInstanceOf(throwable(ClientLinkRemotelyClosedException.class))
          .matches(e -> "amqp:invalid-field".equals(e.getErrorCondition().condition()));
      checkSession(s);
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_2_0)
  void refuseLinkReceiverToMissingQueueShouldReturnNotFound() throws Exception {
    try (Client client = client()) {
      org.apache.qpid.protonj2.client.Connection c = connection(client);
      Session s = c.openSession().openFuture().get(10, SECONDS);
      assertThatThrownBy(
              () ->
                  ExceptionUtils.wrapGet(
                      s.openReceiver("/queues/missing", new ReceiverOptions().creditWindow(0))
                          .openFuture()))
          .isInstanceOf(ClientLinkRemotelyClosedException.class)
          .asInstanceOf(throwable(ClientLinkRemotelyClosedException.class))
          .matches(e -> "amqp:not-found".equals(e.getErrorCondition().condition()));
      checkSession(s);
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_2_0)
  void refuseLinkReceiverToInvalidAddressShouldReturnInvalidField() throws Exception {
    try (Client client = client()) {
      org.apache.qpid.protonj2.client.Connection c = connection(client);
      Session s = c.openSession().openFuture().get(10, SECONDS);
      assertThatThrownBy(
              () ->
                  ExceptionUtils.wrapGet(
                      s.openReceiver("/fruit/orange", new ReceiverOptions().creditWindow(0))
                          .openFuture()))
          .isInstanceOf(ClientLinkRemotelyClosedException.class)
          .asInstanceOf(throwable(ClientLinkRemotelyClosedException.class))
          .matches(e -> "amqp:invalid-field".equals(e.getErrorCondition().condition()));
      checkSession(s);
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_2_0)
  void requestReplyVolatileQueue() throws Exception {
    try (Client client = client()) {
      org.apache.qpid.protonj2.client.Connection serverC = connection(client);
      Session serverS = serverC.openSession();
      Sender serverSnd = serverS.openAnonymousSender(null);
      Receiver serverR =
          serverS.openReceiver(
              "/queues/" + q,
              new ReceiverOptions()
                  .deliveryMode(AT_LEAST_ONCE)
                  .autoSettle(false)
                  .autoAccept(false));

      org.apache.qpid.protonj2.client.Connection clientC = connection(client);
      Session clientS = clientC.openSession();
      ReceiverOptions receiverOptions = new ReceiverOptions().deliveryMode(AT_MOST_ONCE);
      receiverOptions
          .sourceOptions()
          .capabilities("rabbitmq:volatile-queue")
          .expiryPolicy(ExpiryPolicy.LINK_CLOSE)
          .durabilityMode(DurabilityMode.NONE);
      Receiver clientR = clientS.openDynamicReceiver(receiverOptions);
      clientR.openFuture().get();
      assertThat(clientR.address()).isNotNull();

      Sender clientSnd = clientC.openSender("/queues/" + q);
      String body = UUID.randomUUID().toString();
      String corrId = UUID.randomUUID().toString();
      Message<String> request = Message.create(body).replyTo(clientR.address()).messageId(corrId);
      clientSnd.send(request);

      Delivery delivery = serverR.receive(10, SECONDS);
      assertThat(delivery).isNotNull();
      request = delivery.message();
      Message<String> response =
          Message.create("*** " + request.body() + " ***")
              .to(request.replyTo())
              .correlationId(request.messageId());
      serverSnd.send(response);
      delivery.disposition(DeliveryState.accepted(), true);

      delivery = clientR.receive(10, SECONDS);
      assertThat(delivery).isNotNull();
      response = delivery.message();
      assertThat(response.correlationId()).isEqualTo(corrId);
      assertThat(response.body()).isEqualTo("*** " + body + " ***");
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_0)
  void asynchronousGet() throws Exception {
    try (Client client = client()) {
      org.apache.qpid.protonj2.client.Connection c1 = connection(client, o -> o.traceFrames(true));
      Session s1 = c1.openSession();
      ReceiverOptions receiverOptions =
          new ReceiverOptions().deliveryMode(AT_LEAST_ONCE).autoAccept(false).creditWindow(0);
      receiverOptions.sourceOptions().capabilities("temporary-queue");
      ClientReceiver receiver = (ClientReceiver) s1.openDynamicReceiver(receiverOptions);
      receiver.openFuture().get();
      assertThat(receiver.address()).isNotNull();

      org.apache.qpid.protonj2.client.Connection c2 = connection(client);
      Session s2 = c2.openSession();
      Sender sender = s2.openSender(receiver.address());
      String body = UUID.randomUUID().toString();
      sender.send(Message.create(body));

      IntConsumer send =
          count -> {
            for (int i = 0; i < count; i++) {
              try {
                sender.send(Message.create("test"));
              } catch (ClientException e) {
                throw new RuntimeException(e);
              }
            }
          };

      Consumer<Delivery> accept =
          delivery -> {
            try {
              delivery.disposition(DeliveryState.accepted(), true);
            } catch (ClientException e) {
              throw new RuntimeException(e);
            }
          };

      Duration timeout = Duration.ofMillis(100L);
      Function<Integer, List<Delivery>> receive =
          messageCount -> {
            try {
              List<Delivery> messages = new ArrayList<>(messageCount);
              receiver.addCredit(messageCount);
              Delivery delivery = null;
              while ((delivery = receiver.receive(timeout.toMillis(), MILLISECONDS)) != null) {
                messages.add(delivery);
              }
              receiver.drain().get(timeout.toMillis(), MILLISECONDS);
              delivery = receiver.tryReceive();
              if (delivery != null) {
                messages.add(delivery);
              }
              return messages;
            } catch (ClientException
                | InterruptedException
                | ExecutionException
                | TimeoutException e) {
              throw new RuntimeException(e);
            }
          };

      List<Delivery> deliveries = receive.apply(1);
      assertThat(deliveries).hasSize(1);
      assertThat(deliveries.get(0).message().body()).isEqualTo(body);
      accept.accept(deliveries.get(0));

      deliveries = receive.apply(1);
      assertThat(deliveries).isEmpty();

      int messageCount = 10;
      send.accept(messageCount);
      deliveries = receive.apply(messageCount);
      assertThat(deliveries).hasSize(messageCount);
      deliveries.forEach(accept);

      send.accept(messageCount);
      deliveries = receive.apply(messageCount + 2);
      assertThat(deliveries).hasSize(messageCount);
      deliveries.forEach(accept);

      send.accept(messageCount * 2);
      deliveries = receive.apply(messageCount);
      assertThat(deliveries).hasSize(messageCount);
      deliveries.forEach(accept);

      deliveries = receive.apply(messageCount);
      assertThat(deliveries).hasSize(messageCount);
      deliveries.forEach(accept);

      deliveries = receive.apply(messageCount);
      assertThat(deliveries).isEmpty();
    }
  }

  private static void checkSession(Session s) throws Exception {
    ReceiverOptions receiverOptions = new ReceiverOptions();
    receiverOptions.sourceOptions().capabilities("temporary-queue");
    Receiver receiver = s.openDynamicReceiver(receiverOptions);
    receiver.openFuture().get(10, SECONDS);
  }
}
