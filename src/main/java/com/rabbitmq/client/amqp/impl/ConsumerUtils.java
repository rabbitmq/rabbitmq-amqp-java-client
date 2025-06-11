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

import static com.rabbitmq.client.amqp.metrics.MetricsCollector.ConsumeDisposition.ACCEPTED;
import static com.rabbitmq.client.amqp.metrics.MetricsCollector.ConsumeDisposition.DISCARDED;
import static com.rabbitmq.client.amqp.metrics.MetricsCollector.ConsumeDisposition.REQUEUED;

import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.impl.ClientConversionSupport;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConsumerUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerUtils.class);

  private ConsumerUtils() {}

  static void handleContextException(CloseableConsumer consumer, Exception ex, String operation) {
    if (maybeCloseConsumerOnException(consumer, ex)) {
      return;
    }
    if (ex instanceof ClientIllegalStateException
        || ex instanceof RejectedExecutionException
        || ex instanceof ClientIOException) {
      LOGGER.debug("message {} failed: {}", operation, ex.getMessage());
    } else if (ex instanceof ClientException) {
      throw ExceptionUtils.convert((ClientException) ex);
    }
  }

  static boolean maybeCloseConsumerOnException(CloseableConsumer consumer, Exception ex) {
    return ExceptionUtils.maybeCloseOnException(consumer::close, ex);
  }

  abstract static class ContextBase implements Consumer.Context {

    static final DeliveryState REJECTED = DeliveryState.rejected(null, null);
    private final AtomicBoolean settled = new AtomicBoolean(false);
    protected final CloseableConsumer consumer;

    protected ContextBase(CloseableConsumer consumer) {
      this.consumer = consumer;
    }

    protected boolean settleState() {
      return this.settled.compareAndSet(false, true);
    }

    protected boolean isSettled() {
      return this.settled.get();
    }
  }

  abstract static class DeliveryContextBase extends ContextBase {

    protected final Delivery delivery;

    protected DeliveryContextBase(Delivery delivery, CloseableConsumer consumer) {
      super(consumer);
      this.delivery = delivery;
    }

    @Override
    public void accept() {
      this.settle(DeliveryState.accepted(), ACCEPTED, "accept");
    }

    @Override
    public void discard() {
      this.settle(REJECTED, DISCARDED, "discard");
    }

    @Override
    public void discard(Map<String, Object> annotations) {
      annotations = annotations == null ? Collections.emptyMap() : annotations;
      Utils.checkMessageAnnotations(annotations);
      this.settle(DeliveryState.modified(true, true, annotations), DISCARDED, "discard (modified)");
    }

    @Override
    public void requeue() {
      this.settle(DeliveryState.released(), REQUEUED, "requeue");
    }

    @Override
    public void requeue(Map<String, Object> annotations) {
      annotations = annotations == null ? Collections.emptyMap() : annotations;
      Utils.checkMessageAnnotations(annotations);
      this.settle(
          DeliveryState.modified(false, false, annotations), REQUEUED, "requeue (modified)");
    }

    protected abstract void doSettle(
        DeliveryState state, MetricsCollector.ConsumeDisposition disposition) throws Exception;

    private void settle(
        DeliveryState state, MetricsCollector.ConsumeDisposition disposition, String label) {
      if (settleState()) {
        try {
          doSettle(state, disposition);
        } catch (Exception e) {
          ConsumerUtils.handleContextException(this.consumer, e, label);
        }
      }
    }
  }

  abstract static class BatchContextBase extends ContextBase implements Consumer.BatchContext {

    private static final org.apache.qpid.protonj2.types.transport.DeliveryState REJECTED =
        new Rejected();
    private final List<DeliveryContextBase> contexts;

    protected BatchContextBase(int batchSizeHint, ConsumerUtils.CloseableConsumer consumer) {
      super(consumer);
      this.contexts = new ArrayList<>(batchSizeHint);
    }

    @Override
    public void add(Consumer.Context context) {
      if (isSettled()) {
        throw new IllegalStateException("Batch is closed");
      } else {
        if (context instanceof DeliveryContextBase) {
          DeliveryContextBase dctx = (DeliveryContextBase) context;
          // marking the context as settled avoids operation on it and deduplicates as well
          if (dctx.settleState()) {
            this.contexts.add(dctx);
          } else {
            throw new IllegalStateException("Message already settled");
          }
        } else {
          throw new IllegalArgumentException("Context type not supported: " + context);
        }
      }
    }

    @Override
    public int size() {
      return this.contexts.size();
    }

    @Override
    public Consumer.BatchContext batch(int batchSizeHint) {
      return this;
    }

    @Override
    public void accept() {
      this.settle(Accepted.getInstance(), ACCEPTED, "accept");
    }

    @Override
    public void discard() {
      this.settle(REJECTED, DISCARDED, "discard");
    }

    @Override
    public void discard(Map<String, Object> annotations) {
      annotations = annotations == null ? Collections.emptyMap() : annotations;
      Utils.checkMessageAnnotations(annotations);
      Modified state =
          new Modified(false, true, ClientConversionSupport.toSymbolKeyedMap(annotations));
      this.settle(state, DISCARDED, "discard (modified)");
    }

    @Override
    public void requeue() {
      this.settle(Released.getInstance(), REQUEUED, "requeue");
    }

    @Override
    public void requeue(Map<String, Object> annotations) {
      annotations = annotations == null ? Collections.emptyMap() : annotations;
      Utils.checkMessageAnnotations(annotations);
      Modified state =
          new Modified(false, false, ClientConversionSupport.toSymbolKeyedMap(annotations));
      this.settle(state, REQUEUED, "requeue (modified)");
    }

    protected List<DeliveryContextBase> contexts() {
      return this.contexts;
    }

    protected abstract void doSettle(
        org.apache.qpid.protonj2.types.transport.DeliveryState state,
        MetricsCollector.ConsumeDisposition disposition)
        throws Exception;

    private void settle(
        org.apache.qpid.protonj2.types.transport.DeliveryState state,
        MetricsCollector.ConsumeDisposition disposition,
        String label) {
      if (settleState()) {
        try {
          doSettle(state, disposition);
        } catch (Exception e) {
          ConsumerUtils.handleContextException(this.consumer, e, label);
        }
      }
    }
  }

  interface CloseableConsumer {

    void close(Throwable e);
  }
}
