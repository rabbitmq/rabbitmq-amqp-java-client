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

import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static com.rabbitmq.client.amqp.impl.Utils.maybeClose;

import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.ObservationCollector;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AmqpPublisher extends ResourceBase implements Publisher {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpPublisher.class);

  private final Long id;
  private volatile Sender sender;
  private final ExecutorService executorService;
  private final String address;
  private final AmqpConnection connection;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final MetricsCollector metricsCollector;
  private final ObservationCollector observationCollector;
  private final Function<Message, Tracker> publishCall;
  private final DefaultAddressBuilder.DestinationSpec destinationSpec;
  private final Duration publishTimeout;
  private final SessionHandler sessionHandler;
  private volatile ObservationCollector.ConnectionInfo connectionInfo;
  private final ExecutorService dispatchingExecutorService;
  private final java.util.function.Consumer<ClientException> nativeCloseHandler;

  AmqpPublisher(AmqpPublisherBuilder builder) {
    super(builder.listeners());
    this.id = ID_SEQUENCE.getAndIncrement();
    this.executorService = builder.connection().environment().publisherExecutorService();
    this.address = builder.address();
    this.destinationSpec = builder.destination();
    this.connection = builder.connection();
    this.publishTimeout = builder.publishTimeout();
    this.sessionHandler = this.connection.createSessionHandler();
    this.dispatchingExecutorService = connection.dispatchingExecutorService();
    this.nativeCloseHandler =
        e ->
            this.dispatchingExecutorService.submit(
                () -> {
                  // get result to make spotbugs happy
                  boolean ignored = maybeCloseConsumerOnException(this, e);
                });
    this.sender =
        this.createSender(
            sessionHandler.session(), this.address, this.publishTimeout, this.nativeCloseHandler);
    this.metricsCollector = this.connection.metricsCollector();
    this.observationCollector = this.connection.observationCollector();
    this.state(OPEN);
    this.metricsCollector.openPublisher();
    this.publishCall =
        msg -> {
          try {
            org.apache.qpid.protonj2.client.Message<?> nativeMessage =
                ((AmqpMessage) msg).nativeMessage();
            return this.sender.send(nativeMessage.durable(true));
          } catch (ClientIllegalStateException e) {
            // the link is closed
            LOGGER.debug("Error while publishing: '{}'. Closing publisher.", e.getMessage());
            this.close(ExceptionUtils.convert(e));
            throw ExceptionUtils.convert(e);
          } catch (ClientException e) {
            LOGGER.debug("Error while publishing: '{}'.", e.getMessage());
            throw ExceptionUtils.convert(e);
          }
        };
    this.connectionInfo = new Utils.ObservationConnectionInfo(this.connection.connectionAddress());
  }

  @Override
  public Message message() {
    return new AmqpMessage();
  }

  @Override
  public Message message(byte[] body) {
    return new AmqpMessage(body);
  }

  @Override
  public void publish(Message message, Callback callback) {
    checkOpen();
    Tracker tracker =
        this.observationCollector.publish(
            destinationSpec.exchange(),
            destinationSpec.routingKey(),
            message,
            this.connectionInfo,
            publishCall);
    tracker
        .settlementFuture()
        .handleAsync(
            (t, ex) -> {
              Status status;
              if (ex == null) {
                status = mapDeliveryState(t.remoteState());
              } else {
                status = Status.REJECTED;
              }
              DefaultContext defaultContext = new DefaultContext(message, status);
              this.metricsCollector.publishDisposition(mapToPublishDisposition(status));
              callback.handle(defaultContext);
              return null;
            },
            this.executorService);
    this.metricsCollector.publish();
  }

  private Status mapDeliveryState(DeliveryState in) {
    if (in.isAccepted()) {
      return Status.ACCEPTED;
    } else if (in.getType() == DeliveryState.Type.REJECTED) {
      return Status.REJECTED;
    } else if (in.getType() == DeliveryState.Type.RELEASED) {
      return Status.RELEASED;
    } else {
      LOGGER.warn("Delivery state not supported: " + in.getType());
      throw new IllegalStateException("This delivery state is not supported: " + in.getType());
    }
  }

  private static MetricsCollector.PublishDisposition mapToPublishDisposition(Status status) {
    if (status == Status.ACCEPTED) {
      return MetricsCollector.PublishDisposition.ACCEPTED;
    } else if (status == Status.REJECTED) {
      return MetricsCollector.PublishDisposition.REJECTED;
    } else if (status == Status.RELEASED) {
      return MetricsCollector.PublishDisposition.RELEASED;
    } else {
      return null;
    }
  }

  void recoverAfterConnectionFailure() {
    this.connectionInfo = new Utils.ObservationConnectionInfo(this.connection.connectionAddress());
    this.sender =
        this.createSender(
            this.sessionHandler.sessionNoCheck(),
            this.address,
            this.publishTimeout,
            this.nativeCloseHandler);
  }

  @Override
  public void close() {
    this.close(null);
  }

  // internal API

  private Sender createSender(
      Session session,
      String address,
      Duration publishTimeout,
      Consumer<ClientException> nativeCloseHandler) {
    SenderOptions senderOptions =
        new SenderOptions()
            .deliveryMode(DeliveryMode.AT_LEAST_ONCE)
            .sendTimeout(
                publishTimeout.isNegative()
                    ? ConnectionOptions.INFINITE
                    : publishTimeout.toMillis())
            .closeHandler(nativeCloseHandler);
    try {
      Sender s =
          address == null
              ? session.openAnonymousSender()
              : session.openSender(address, senderOptions);
      return ExceptionUtils.wrapGet(s.openFuture());
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e, "Error while creating publisher to %s", address);
    }
  }

  void close(Throwable cause) {
    if (this.closed.compareAndSet(false, true)) {
      this.state(State.CLOSING, cause);
      this.connection.removePublisher(this);
      maybeClose(this.sender, e -> LOGGER.info("Error while closing sender", e));
      maybeClose(
          this.sessionHandler,
          e -> LOGGER.info("Error while closing publisher session handler", e));
      this.state(State.CLOSED, cause);
      this.metricsCollector.closePublisher();
    }
  }

  private static boolean maybeCloseConsumerOnException(AmqpPublisher publisher, Exception ex) {
    return ExceptionUtils.maybeCloseOnException(publisher::close, ex);
  }

  private static class DefaultContext implements Publisher.Context {

    private final Message message;
    private final Status status;

    private DefaultContext(Message message, Status status) {
      this.message = message;
      this.status = status;
    }

    @Override
    public Message message() {
      return this.message;
    }

    @Override
    public Status status() {
      return this.status;
    }
  }

  Long id() {
    return this.id;
  }

  String address() {
    return this.address;
  }

  @Override
  public String toString() {
    return "AmqpPublisher{" + "id=" + id + ", address='" + address + '\'' + '}';
  }
}
