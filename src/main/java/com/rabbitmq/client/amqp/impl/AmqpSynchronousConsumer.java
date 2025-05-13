// Copyright (c) 2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.client.amqp.Resource.State.CLOSED;
import static com.rabbitmq.client.amqp.Resource.State.CLOSING;
import static com.rabbitmq.client.amqp.Resource.State.OPEN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.SynchronousConsumer;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryMode;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientReceiver;
import org.apache.qpid.protonj2.engine.Scheduler;
import org.apache.qpid.protonj2.engine.impl.ProtonReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AmqpSynchronousConsumer extends ResourceBase
    implements SynchronousConsumer, ConsumerUtils.CloseableConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSynchronousConsumer.class);

  private final String queue;
  private final String address;
  private final AmqpConnection connection;
  private final SessionHandler sessionHandler;
  private final ClientReceiver nativeReceiver;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  // native receiver internal state, accessed only in the native executor/scheduler
  private ProtonReceiver protonReceiver;
  private volatile Scheduler protonExecutor;

  AmqpSynchronousConsumer(String queue, AmqpConnection connection, List<StateListener> listeners) {
    super(listeners);

    DefaultAddressBuilder<?> addressBuilder = Utils.addressBuilder();
    addressBuilder.queue(queue);
    this.address = addressBuilder.address();
    this.queue = queue;
    this.connection = connection;
    this.sessionHandler = connection.createSessionHandler();
    this.nativeReceiver = this.createNativeReceiver(this.sessionHandler.session(), this.address);
    this.initStateFromNativeReceiver(this.nativeReceiver);
    this.state(OPEN);
  }

  @Override
  public Response get(Duration timeout) {
    List<Response> responses = this.get(1, timeout);
    return responses.isEmpty() ? null : responses.get(0);
  }

  @Override
  public List<Response> get(int messageCount, Duration timeout) {
    checkOpen();
    try {
      List<Response> messages = new ArrayList<>(messageCount);
      nativeReceiver.addCredit(messageCount);
      Delivery delivery = null;
      while ((delivery = nativeReceiver.receive(timeout.toMillis(), MILLISECONDS)) != null) {
        this.includeMessage(messages, delivery);
      }
      nativeReceiver.drain().get(timeout.toMillis(), MILLISECONDS);
      while ((delivery = nativeReceiver.tryReceive()) != null) {
        this.includeMessage(messages, delivery);
      }
      return messages;
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e);
    } catch (ExecutionException e) {
      throw ExceptionUtils.convert(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AmqpException(e);
    } catch (TimeoutException e) {
      throw new AmqpException(e);
    }
  }

  @Override
  public void close() {
    this.close(null);
  }

  // internal API

  private ClientReceiver createNativeReceiver(Session nativeSession, String address) {
    ReceiverOptions receiverOptions =
        new ReceiverOptions()
            .deliveryMode(DeliveryMode.AT_LEAST_ONCE)
            .autoAccept(false)
            .autoSettle(false)
            .creditWindow(0);

    try {
      ClientReceiver receiver =
          (ClientReceiver)
              ExceptionUtils.wrapGet(
                  nativeSession.openReceiver(address, receiverOptions).openFuture());
      return receiver;
    } catch (ClientException e) {
      throw ExceptionUtils.convert(e, "Error while creating receiver from '%s'", address);
    }
  }

  private void initStateFromNativeReceiver(ClientReceiver receiver) {
    try {
      Scheduler protonExecutor = receiver.executor();
      CountDownLatch fieldsSetLatch = new CountDownLatch(1);
      protonExecutor.execute(
          () -> {
            this.protonReceiver = (ProtonReceiver) receiver.protonReceiver();
            this.protonExecutor = protonExecutor;
            fieldsSetLatch.countDown();
          });
      if (!fieldsSetLatch.await(10, TimeUnit.SECONDS)) {
        throw new AmqpException("Could not initialize consumer internal state");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AmqpException(e);
    }
  }

  private void includeMessage(List<Response> responses, Delivery delivery) {
    AmqpMessage message = amqpMessage(delivery);
    if (message != null) {
      DeliveryContext deliveryContext =
          new DeliveryContext(delivery, this.protonExecutor, this.protonReceiver, this);
      responses.add(new DefaultResponse(message, deliveryContext));
    }
  }

  public void close(Throwable cause) {
    if (this.closed.compareAndSet(false, true)) {
      this.state(CLOSING, cause);
      try {
        if (this.nativeReceiver != null) {
          this.nativeReceiver.close();
        }
        this.sessionHandler.close();
      } catch (Exception e) {
        LOGGER.warn("Error while closing receiver", e);
      }
      this.state(CLOSED, cause);
    }
  }

  private static AmqpMessage amqpMessage(Delivery delivery) {
    try {
      AmqpMessage message = new AmqpMessage(delivery.message());
      return message;
    } catch (ClientException e) {
      LOGGER.warn("Error while decoding message: {}", e.getMessage());
      try {
        delivery.disposition(DeliveryState.rejected("", ""), true);
      } catch (ClientException ex) {
        LOGGER.warn("Error while rejecting non-decoded message: {}", ex.getMessage());
      }
      return null;
    }
  }

  private static class DeliveryContext extends ConsumerUtils.DeliveryContextBase {

    private final Scheduler protonExecutor;
    private final ProtonReceiver protonReceiver;

    private DeliveryContext(
        Delivery delivery,
        Scheduler protonExecutor,
        ProtonReceiver protonReceiver,
        AmqpSynchronousConsumer consumer) {
      super(delivery, consumer);
      this.protonExecutor = protonExecutor;
      this.protonReceiver = protonReceiver;
    }

    @Override
    public Consumer.BatchContext batch(int batchSizeHint) {
      return new BatchContext(
          batchSizeHint, this.protonExecutor, this.protonReceiver, this.consumer);
    }

    @Override
    protected void doSettle(DeliveryState state, MetricsCollector.ConsumeDisposition disposition)
        throws Exception {
      delivery.disposition(state, true);
    }
  }

  private static class BatchContext extends ConsumerUtils.BatchContextBase {

    private final Scheduler protonExecutor;
    private final ProtonReceiver protonReceiver;

    private BatchContext(
        int batchSizeHint,
        Scheduler protonExecutor,
        ProtonReceiver protonReceiver,
        ConsumerUtils.CloseableConsumer consumer) {
      super(batchSizeHint, consumer);
      this.protonExecutor = protonExecutor;
      this.protonReceiver = protonReceiver;
    }

    @Override
    protected void doSettle(
        org.apache.qpid.protonj2.types.transport.DeliveryState state,
        MetricsCollector.ConsumeDisposition disposition) {
      long[][] ranges =
          SerialNumberUtils.ranges(this.contexts(), ctx -> ctx.delivery.getDeliveryId());
      this.protonExecutor.execute(
          () -> {
            for (long[] range : ranges) {
              this.protonReceiver.disposition(state, range);
            }
          });
    }
  }

  private static class DefaultResponse implements Response {

    private final AmqpMessage message;
    private final Consumer.Context context;

    private DefaultResponse(AmqpMessage message, Consumer.Context context) {
      this.message = message;
      this.context = context;
    }

    @Override
    public AmqpMessage message() {
      return message;
    }

    @Override
    public Consumer.Context context() {
      return context;
    }
  }
}
