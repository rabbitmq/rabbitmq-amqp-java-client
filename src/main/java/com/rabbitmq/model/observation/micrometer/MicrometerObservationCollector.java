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
package com.rabbitmq.model.observation.micrometer;

import com.rabbitmq.model.Consumer;
import com.rabbitmq.model.Message;
import com.rabbitmq.model.ObservationCollector;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.util.function.Function;

class MicrometerObservationCollector implements ObservationCollector {

  private final ObservationRegistry registry;
  private final PublishObservationConvention customPublishConvention, defaultPublishConvention;
  private final DeliverObservationConvention customProcessConvention, defaultProcessConvention;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  MicrometerObservationCollector(
      ObservationRegistry registry,
      PublishObservationConvention customPublishConvention,
      PublishObservationConvention defaultPublishConvention,
      DeliverObservationConvention customProcessConvention,
      DeliverObservationConvention defaultProcessConvention) {
    this.registry = registry;
    this.customPublishConvention = customPublishConvention;
    this.defaultPublishConvention = defaultPublishConvention;
    this.customProcessConvention = customProcessConvention;
    this.defaultProcessConvention = defaultProcessConvention;
  }

  @Override
  public <T> T publish(
      String exchange,
      String routingKey,
      Message message,
      ConnectionInfo connectionInfo,
      Function<Message, T> publishCall) {
    PublishContext context = new PublishContext(exchange, routingKey, message, connectionInfo);
    Observation observation =
        AmqpObservationDocumentation.PUBLISH_OBSERVATION.observation(
            this.customPublishConvention,
            this.defaultPublishConvention,
            () -> context,
            this.registry);
    observation.start();
    try {
      return publishCall.apply(message);
    } catch (RuntimeException e) {
      observation.error(e);
      throw e;
    } finally {
      observation.stop();
    }
  }

  @Override
  public Consumer.MessageHandler subscribe(String queue, Consumer.MessageHandler handler) {
    return new ObservationMessageHandler(
        queue, handler, this.registry, this.customProcessConvention, this.defaultProcessConvention);
  }

  private static final class ObservationMessageHandler implements Consumer.MessageHandler {

    private final String queue;
    private final Consumer.MessageHandler delegate;
    private final ObservationRegistry registry;
    private final DeliverObservationConvention customProcessConvention, defaultProcessConvention;

    private ObservationMessageHandler(
        String queue,
        Consumer.MessageHandler delegate,
        ObservationRegistry registry,
        DeliverObservationConvention customProcessConvention,
        DeliverObservationConvention defaultProcessConvention) {
      this.queue = queue;
      this.delegate = delegate;
      this.registry = registry;
      this.customProcessConvention = customProcessConvention;
      this.defaultProcessConvention = defaultProcessConvention;
    }

    @Override
    public void handle(Consumer.Context context, Message message) {
      DeliverContext deliverContext =
          new DeliverContext(extractExchange(message), extractRoutingKey(message), queue, message);
      Observation observation =
          AmqpObservationDocumentation.PROCESS_OBSERVATION.observation(
              this.customProcessConvention,
              this.defaultProcessConvention,
              () -> deliverContext,
              this.registry);
      observation.observeChecked(() -> delegate.handle(context, message));
    }
  }

  private static String extractExchange(Message message) {
    Object exchange = message.annotation("x-exchange");
    return exchange == null ? null : exchange.toString();
  }

  private static String extractRoutingKey(Message message) {
    Object routingKey = message.annotation("x-routing-key");
    return routingKey == null ? null : routingKey.toString();
  }
}
