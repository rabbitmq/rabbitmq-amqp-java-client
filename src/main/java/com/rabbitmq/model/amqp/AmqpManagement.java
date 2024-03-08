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

import com.rabbitmq.model.Management;
import com.rabbitmq.model.ModelException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.qpid.protonj2.buffer.impl.ProtonByteArrayBuffer;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.codec.decoders.ProtonDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncoder;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncoderFactory;

class AmqpManagement implements Management {

  private static final String MANAGEMENT_NODE_ADDRESS = "/management/v2";
  private static final String REPLY_TO = "$me";

  private static final String GET = "GET";
  private static final String POST = "POST";
  private static final String PUT = "PUT";
  private static final String DELETE = "DELETE";
  private static final String CODE_200 = "200";
  private static final String CODE_201 = "201";
  private static final String CODE_204 = "204";

  private final Session session;
  private final Lock linkPairLock = new ReentrantLock();
  private final Sender sender; // @GuardedBy("linkPairLock")
  private final Receiver receiver; // @GuardedBy("linkPairLock")
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ProtonEncoder encoder = ProtonEncoderFactory.create();
  private final ProtonDecoder decoder = ProtonDecoderFactory.create();
  private final Duration rpcTimeout = Duration.ofSeconds(10);

  AmqpManagement(AmqpConnection connection) {
    try {
      this.session = connection.nativeConnection().openSession();
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
                  .properties(properties)
                  .creditWindow(100));

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
      Map<String, Object> responseBody = delete(queueLocation(name), CODE_200);
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
      delete(exchangeLocation(name), CODE_204);
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

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.receiver.close();
      this.sender.close();
      this.session.close();
    }
  }

  boolean isOpen() {
    return !this.closed.get();
  }

  void declareQueue(String name, Map<String, Object> body) {
    // TODO HTTP encode queue name
    declare(body, "/queues/" + name);
  }

  void declareExchange(String name, Map<String, Object> body) {
    // TODO HTTP encode exchange name
    declare(body, "/exchanges/" + name);
  }

  private Map<String, Object> declare(Map<String, Object> body, String target) {
    return this.declare(body, target, PUT);
  }

  private Map<String, Object> declare(Map<String, Object> body, String target, String operation) {
    return this.callOnLinkPair(
        () -> {
          UUID requestId = messageId();
          try {
            Message<byte[]> request =
                Message.create(encode(body))
                    .messageId(requestId)
                    .to(target)
                    .subject(operation)
                    .replyTo(REPLY_TO);

            this.sender.send(request);

            Delivery delivery = receiver.receive(this.rpcTimeout.toMillis(), MILLISECONDS);
            checkResponse(delivery, this.rpcTimeout, requestId, CODE_201);
            Message<byte[]> response = delivery.message();
            return decodeMap(response.body());
          } catch (ClientException e) {
            throw new ModelException("Error on PUT operation: " + target, e);
          }
        });
  }

  private Map<String, Object> delete(String target, String expectedResponseCode) {
    return this.callOnLinkPair(
        () -> {
          UUID requestId = messageId();
          try {
            Message<byte[]> request =
                Message.create(new byte[0])
                    .messageId(requestId)
                    .to(target)
                    .subject(DELETE)
                    .replyTo(REPLY_TO);

            this.sender.send(request);
            Delivery delivery = receiver.receive(this.rpcTimeout.toMillis(), MILLISECONDS);
            checkResponse(delivery, this.rpcTimeout, requestId, expectedResponseCode);
            Message<byte[]> response = delivery.message();
            return decodeMap(response.body());
          } catch (ClientException e) {
            throw new ModelException("Error on DELETE operation: " + target, e);
          }
        });
  }

  private byte[] encode(Map<String, Object> map) {
    try (ProtonByteArrayBuffer buffer = new ProtonByteArrayBuffer()) {
      encoder.writeMap(buffer, encoder.newEncoderState(), map);
      return Arrays.copyOf(buffer.getReadableArray(), buffer.getReadableBytes());
    }
  }

  @SuppressWarnings("unchecked")
  private <K, V> Map<K, V> decodeMap(byte[] array) {
    return (Map<K, V>) decode(array);
  }

  @SuppressWarnings("unchecked")
  private <T> List<T> decodeList(byte[] array) {
    return (List<T>) decode(array);
  }

  private Object decode(byte[] array) {
    try (ProtonByteArrayBuffer buffer = new ProtonByteArrayBuffer(array.length)) {
      buffer.writeBytes(array);
      return decoder.readObject(buffer, decoder.newDecoderState());
    }
  }

  private static UUID messageId() {
    return UUID.randomUUID();
  }

  private static String queueLocation(String q) {
    // TODO HTTP encode queue name
    return "/queues/" + q;
  }

  private static String exchangeLocation(String e) {
    // TODO HTTP encode exchange name
    return "/exchanges/" + e;
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

  void bind(Map<String, Object> body) {
    declare(body, "/bindings", POST);
  }

  void unbind(
      String destinationField,
      String source,
      String destination,
      String key,
      Map<String, Object> arguments) {
    if (arguments == null || arguments.isEmpty()) {
      String target =
          "/bindings/src="
              + source
              + ";"
              + destinationField
              + "="
              + destination
              + ";key="
              + key
              + ";args=";
      delete(target, CODE_204);
    } else {
      this.callOnLinkPair(
              () -> {
                List<Map<String, Object>> bindings =
                    get(
                        bindingsTarget(destinationField, source, destination, key),
                        this::decodeList);
                return matchBinding(bindings, key, arguments);
              })
          .ifPresent(location -> delete(location, CODE_204));
    }
  }

  private static Optional<String> matchBinding(
      List<Map<String, Object>> bindings, String key, Map<String, Object> arguments) {
    Optional<String> uri;
    if (!bindings.isEmpty()) {
      uri =
          bindings.stream()
              .filter(
                  binding -> {
                    String bindingKey = (String) binding.get("binding_key");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> bindingArguments =
                        (Map<String, Object>) binding.get("arguments");
                    if (key == null && bindingKey == null
                        || key != null && key.equals(bindingKey)) {
                      return arguments == null && bindingArguments == null
                          || arguments != null && arguments.equals(bindingArguments);
                    }
                    return false;
                  })
              .map(b -> b.get("location").toString())
              .findFirst();
    } else {
      uri = Optional.empty();
    }
    return uri;
  }

  private <T> T get(String target, Function<byte[], T> bodyTransformer) throws ClientException {
    UUID requestId = messageId();
    Message<byte[]> request =
        Message.create(new byte[0]).messageId(requestId).to(target).subject(GET).replyTo(REPLY_TO);

    this.sender.send(request);

    Delivery delivery = receiver.receive(this.rpcTimeout.toMillis(), MILLISECONDS);
    checkResponse(delivery, this.rpcTimeout, requestId, CODE_200);
    Message<byte[]> response = delivery.message();
    return bodyTransformer.apply(response.body());
  }

  private String bindingsTarget(
      String destinationField, String source, String destination, String key) {
    // TODO encode query parameters
    return "/bindings?src=" + source + "&" + destinationField + "=" + destination + "&key=" + key;
  }

  private <T> T callOnLinkPair(Callable<T> operation) {
    try {
      if (this.linkPairLock.tryLock(this.rpcTimeout.toMillis(), MILLISECONDS)) {
        try {
          return operation.call();
        } catch (Exception e) {
          throw new ModelException("Error during management operation", e);
        } finally {
          this.linkPairLock.unlock();
        }
      } else {
        throw new ModelException(
            "Could not acquire link pair lock in %d ms", this.rpcTimeout.toMillis());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ModelException("Thread interrupted while waited on link pair lock", e);
    }
  }
}
