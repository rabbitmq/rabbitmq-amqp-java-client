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
package com.rabbitmq.model;

import static com.rabbitmq.model.TestUtils.*;
import static java.nio.charset.StandardCharsets.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.qpid.protonj2.buffer.impl.ProtonByteArrayBuffer;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.codec.decoders.ProtonDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncoder;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.junit.jupiter.api.*;

public class ClientTest {

  static Environment environment;
  static Management management;
  String q;

  @BeforeAll
  static void initAll() {
    environment = environmentBuilder().build();
    management = environment.management();
  }

  @BeforeEach
  void init(TestInfo info) {
    q = TestUtils.name(info);
    management.queue().name(q).declare();
  }

  @AfterEach
  void tearDown() {
    management.queueDeletion().delete(q);
  }

  @AfterAll
  static void tearDownAll() {
    environment.close();
  }

  @Test
  void deliveryCount() throws Exception {
    try (Client client = client();
        Publisher publisher = environment.publisherBuilder().address(q).build()) {
      int messageCount = 10;
      CountDownLatch publishLatch = new CountDownLatch(5);
      IntStream.range(0, messageCount)
          .forEach(
              ignored ->
                  publisher.publish(
                      publisher.message().addData("".getBytes(UTF_8)),
                      context -> publishLatch.countDown()));

      Connection connection = connection(client);
      Receiver receiver = connection.openReceiver(q, new ReceiverOptions());
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
      Connection connection =
          connection(client, o -> o.traceFrames(false).maxFrameSize(maxFrameSize));

      Sender sender =
          connection.openSender(q, new SenderOptions().deliveryMode(DeliveryMode.AT_LEAST_ONCE));
      byte[] body = new byte[maxFrameSize * 4];
      Arrays.fill(body, (byte) 'A');
      Tracker tracker = sender.send(Message.create(body));
      tracker.awaitSettlement();

      Receiver receiver =
          connection.openReceiver(
              q,
              new ReceiverOptions()
                  .deliveryMode(DeliveryMode.AT_LEAST_ONCE)
                  .autoSettle(false)
                  .autoAccept(false));
      Delivery delivery = receiver.receive(100, TimeUnit.SECONDS);
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
      Connection connection =
          connection(client, o -> o.traceFrames(false).maxFrameSize(maxFrameSize));

      StreamSender sender =
          connection.openStreamSender(
              q, new StreamSenderOptions().deliveryMode(DeliveryMode.AT_LEAST_ONCE));
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
              q,
              new StreamReceiverOptions()
                  .deliveryMode(DeliveryMode.AT_LEAST_ONCE)
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
      Connection connection = connection(client, o -> o.traceFrames(false));

      String linkPairName = "my-link-pair";
      String managementNodeAddress = "$management";
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

      sender.openFuture().get(1, TimeUnit.SECONDS);
      receiver.openFuture().get(1, TimeUnit.SECONDS);

      Map<String, Object> body = new HashMap<>();
      body.put("name", q);
      body.put("durable", true);
      body.put("exclusive", false);
      body.put("auto_delete", false);
      body.put("type", "queue");
      body.put("arguments", Collections.emptyMap());
      ProtonEncoder encoder = ProtonEncoderFactory.create();
      ProtonByteArrayBuffer buffer = new ProtonByteArrayBuffer();
      encoder.writeMap(buffer, encoder.newEncoderState(), body);
      byte[] requestBody = Arrays.copyOf(buffer.getReadableArray(), buffer.getReadableBytes());
      UnsignedLong requestId = ulong(requestIdSequence.incrementAndGet());
      Message<byte[]> request =
          Message.create(requestBody)
              .messageId(requestId)
              .to("/$management/entities")
              .subject("POST")
              .replyTo(replyTo)
              .contentType("application/amqp-management+amqp;type=entity");

      sender.send(request);

      Delivery delivery = receiver.receive(1, TimeUnit.SECONDS);
      assertThat(delivery).isNotNull();
      Message<byte[]> response = delivery.message();
      assertThat(response.correlationId()).isEqualTo(requestId);
      assertThat(response.subject()).isEqualTo("201");
      assertThat(response.contentType())
          .isEqualTo("application/amqp-management+amqp;type=entity-collection");
      assertThat(response.property("http:response")).isEqualTo("1.1");
      assertThat(response.property("location")).isNotNull().isInstanceOf(String.class);

      byte[] responseBodyBin = response.body();
      ProtonDecoder decoder = ProtonDecoderFactory.create();
      buffer = new ProtonByteArrayBuffer(responseBodyBin.length);
      buffer.writeBytes(responseBodyBin);
      Map<String, Object> responseBody = decoder.readMap(buffer, decoder.newDecoderState());
      String location = response.property("location").toString();
      assertThat(responseBody)
          .containsEntry("type", "queue")
          .containsEntry("id", q)
          .containsEntry("self", location)
          .containsEntry("management", managementNodeAddress)
          .containsEntry("target", "/queue/" + q);

      requestId = ulong(requestIdSequence.incrementAndGet());
      request =
          Message.create(new byte[0])
              .messageId(requestId)
              .to(location)
              .subject("DELETE")
              .replyTo(replyTo);

      sender.send(request);

      delivery = receiver.receive(1, TimeUnit.SECONDS);
      assertThat(delivery).isNotNull();
      response = delivery.message();
      assertThat(response.correlationId()).isEqualTo(requestId);
      assertThat(response.subject()).isEqualTo("200");
      assertThat(response.contentType()).isEqualTo("application/amqp-management+amqp");
      assertThat(response.property("http:response")).isEqualTo("1.1");

      responseBodyBin = response.body();
      decoder = ProtonDecoderFactory.create();
      buffer = new ProtonByteArrayBuffer(responseBodyBin.length);
      buffer.writeBytes(responseBodyBin);
      responseBody = decoder.readMap(buffer, decoder.newDecoderState());
      assertThat(responseBody).containsEntry("message_count", ulong(0));
    }
  }
}