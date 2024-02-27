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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.rabbitmq.client.Channel;
import com.rabbitmq.model.Management;
import com.rabbitmq.model.ModelException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.qpid.protonj2.buffer.impl.ProtonByteArrayBuffer;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.codec.decoders.ProtonDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncoder;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncoderFactory;

class AmqpManagement implements Management {

  private static final String MANAGEMENT_NODE_ADDRESS = "$management";
  private static final String REPLY_TO = "$me";

  private final Channel channel;
  private final Session session;
  private final Sender sender;
  private final Receiver receiver;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ProtonEncoder encoder = ProtonEncoderFactory.create();
  private final ProtonDecoder decoder = ProtonDecoderFactory.create();
  private final Duration rpcTimeout = Duration.ofSeconds(10);

  AmqpManagement(AmqpEnvironment environment) {
    try {
      this.channel = environment.amqplConnection().createChannel();

      this.session = environment.connection().openSession();

      String linkPairName = "management-link-pair";
      Map<String, Object> properties = Collections.singletonMap("paired", Boolean.TRUE);
      this.sender =
          session.openSender(
              MANAGEMENT_NODE_ADDRESS,
              new SenderOptions()
                  .deliveryMode(DeliveryMode.AT_MOST_ONCE)
                  .linkName(linkPairName)
                  .properties(properties));

      this.receiver =
          session.openReceiver(
              MANAGEMENT_NODE_ADDRESS,
              new ReceiverOptions()
                  .deliveryMode(DeliveryMode.AT_MOST_ONCE)
                  .linkName(linkPairName)
                  .properties(properties));

      this.sender.openFuture().get(this.rpcTimeout.toMillis(), MILLISECONDS);
      this.receiver.openFuture().get(this.rpcTimeout.toMillis(), MILLISECONDS);
    } catch (Exception e) {
      throw new ModelException(e);
    }
  }

  @Override
  public QueueSpecification queue() {
    return new AmqpQueueSpecification(this);
  }

  @Override
  public QueueDeletion queueDeletion() {
    return name -> {
      Map<String, Object> responseBody = delete(queueLocation(name), "queue", CODE_200);
      if (!responseBody.containsKey("message_count")) {
        throw new ModelException("Response body should contain message_count");
      }
    };
  }

  @Override
  public ExchangeSpecification exchange() {
    return new AmqpExchangeSpecification(this);
  }

  @Override
  public ExchangeDeletion exchangeDeletion() {
    return name -> {
      delete(exchangeLocation(name), "exchange", CODE_204);
    };
  }

  @Override
  public BindingSpecification binding() {
    return new AmqpBindingManagement.AmqpBindingSpecification(this);
  }

  @Override
  public UnbindSpecification unbind() {
    return new AmqpBindingManagement.AmqpUnbindSpecification(this);
  }

  Channel channel() {
    return this.channel;
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      try {
        this.channel.close();
        this.receiver.close();
        this.sender.close();
        this.session.close();
      } catch (IOException | TimeoutException e) {
        throw new ModelException(e);
      }
    }
  }

  boolean isOpen() {
    return !this.closed.get();
  }

  void declareQueue(Map<String, Object> body) {
    declare(body, "/$management/entities", "queue");
  }

  void declareExchange(Map<String, Object> body) {
    declare(body, "/$management/entities", "exchange");
  }

  private Map<String, Object> declare(Map<String, Object> body, String target, String type) {
    UUID requestId = messageId();
    try {
      Message<byte[]> request =
          Message.create(encode(body))
              .messageId(requestId)
              .to(target)
              .subject(POST)
              .replyTo(REPLY_TO)
              .contentType(MEDIA_TYPE_ENTITY);

      // TODO synchronize to avoid concurrent calls
      this.sender.send(request);

      Delivery delivery = receiver.receive(this.rpcTimeout.toMillis(), MILLISECONDS);
      checkResponse(delivery, this.rpcTimeout, requestId, CODE_201);
      Message<byte[]> response = delivery.message();
      Map<String, Object> responseBody = decode(response.body());
      if (!type.equals(responseBody.get("type"))) {
        throw new ModelException("Unexpected type: %s instead of %s", body.get("type"), type);
      }
      return responseBody;
    } catch (ClientException e) {
      throw new ModelException("Error on POST operation: " + type, e);
    }
  }

  private Map<String, Object> delete(String target, String type, String expectedResponseCode) {
    UUID requestId = messageId();
    try {
      Message<byte[]> request =
          Message.create(new byte[0])
              .messageId(requestId)
              .to(target)
              .subject(DELETE)
              .replyTo(REPLY_TO);

      // TODO synchronize to avoid concurrent calls
      sender.send(request);
      Delivery delivery = receiver.receive(this.rpcTimeout.toMillis(), MILLISECONDS);
      checkResponse(delivery, this.rpcTimeout, requestId, expectedResponseCode);
      Message<byte[]> response = delivery.message();
      return decode(response.body());
    } catch (ClientException e) {
      throw new ModelException("Error on DELETE operation: " + type, e);
    }
  }

  private byte[] encode(Map<String, Object> map) {
    try (ProtonByteArrayBuffer buffer = new ProtonByteArrayBuffer()) {
      encoder.writeMap(buffer, encoder.newEncoderState(), map);
      return Arrays.copyOf(buffer.getReadableArray(), buffer.getReadableBytes());
    }
  }

  private Map<String, Object> decode(byte[] array) {
    try (ProtonByteArrayBuffer buffer = new ProtonByteArrayBuffer(array.length)) {
      buffer.writeBytes(array);
      return decoder.readMap(buffer, decoder.newDecoderState());
    }
  }

  private static UUID messageId() {
    return UUID.randomUUID();
  }

  private static String queueLocation(String q) {
    return "/" + MANAGEMENT_NODE_ADDRESS + "/queues/" + q;
  }

  private static String exchangeLocation(String e) {
    return "/" + MANAGEMENT_NODE_ADDRESS + "/exchanges/" + e;
  }

  private static void checkResponse(
      Delivery delivery, Duration rpcTimeout, UUID requestId, String expectedResponseCode)
      throws ClientException {
    if (delivery == null) {
      throw new ModelException("No reply received in %d ms", rpcTimeout.toMillis());
    }
    Message<byte[]> response = delivery.message();
    if (!requestId.equals(response.correlationId())) {
      throw new ModelException("Unexpected correlation ID");
    }
    if (!expectedResponseCode.equals(response.subject())) {
      throw new ModelException(
          "Unexpected response code: %s instead of %s", response.subject(), expectedResponseCode);
    }
  }

  private static final String POST = "POST";
  private static final String DELETE = "DELETE";
  private static final String MEDIA_TYPE_ENTITY = "application/amqp-management+amqp;type=entity";
  private static final String CODE_200 = "200";
  private static final String CODE_201 = "201";
  private static final String CODE_204 = "204";

  void bindQueue(String queue, Map<String, Object> body) {
    declare(body, "/$management/queues/" + queue + "/$management/entities", "binding");
  }
}
