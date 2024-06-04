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

import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.ObservationCollector;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
  private volatile ObservationCollector.ConnectionInfo connectionInfo;

  AmqpPublisher(AmqpPublisherBuilder builder) {
    super(builder.listeners());
    this.id = ID_SEQUENCE.getAndIncrement();
    this.executorService = builder.connection().executorService();
    this.address = builder.address();
    this.destinationSpec = builder.destination();
    this.connection = builder.connection();
    this.publishTimeout = builder.publishTimeout();
    this.sender =
        this.createSender(builder.connection().nativeSession(), this.address, this.publishTimeout);
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
            this.close(ExceptionUtils.convert(e));
            throw ExceptionUtils.convert(e);
          } catch (ClientException e) {
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
    this.executorService.submit(
        () -> {
          Status status;
          try {
            tracker.settlementFuture().get();
            status =
                tracker.remoteState() == DeliveryState.accepted() ? Status.ACCEPTED : Status.FAILED;
          } catch (InterruptedException | ExecutionException e) {
            status = Status.FAILED;
          }
          DefaultContext defaultContext = new DefaultContext(message, status);
          this.metricsCollector.publishDisposition(
              status == Status.ACCEPTED
                  ? MetricsCollector.PublishDisposition.ACCEPTED
                  : MetricsCollector.PublishDisposition.FAILED);
          callback.handle(defaultContext);
        });
    this.metricsCollector.publish();
  }

  void recoverAfterConnectionFailure() {
    this.connectionInfo = new Utils.ObservationConnectionInfo(this.connection.connectionAddress());
    this.sender =
        this.createSender(this.connection.nativeSession(false), this.address, this.publishTimeout);
  }

  @Override
  public void close() {
    this.close(null);
  }

  // internal API

  private Sender createSender(Session session, String address, Duration publishTimeout) {
    SenderOptions senderOptions =
        new SenderOptions()
            .deliveryMode(DeliveryMode.AT_LEAST_ONCE)
            .sendTimeout(
                publishTimeout.isNegative()
                    ? ConnectionOptions.INFINITE
                    : publishTimeout.toMillis());
    try {
      if (address == null) {
        return session.openAnonymousSender(senderOptions);
      } else {
        return session.openSender(address, senderOptions);
      }
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e, "Error while creating publisher to {}", address);
    }
  }

  private void close(Throwable cause) {
    if (this.closed.compareAndSet(false, true)) {
      this.state(State.CLOSING, cause);
      this.connection.removePublisher(this);
      try {
        this.sender.close();
      } catch (Exception e) {
        LOGGER.warn("Error while closing sender", e);
      }
      this.state(State.CLOSED, cause);
      this.metricsCollector.closePublisher();
    }
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
}
