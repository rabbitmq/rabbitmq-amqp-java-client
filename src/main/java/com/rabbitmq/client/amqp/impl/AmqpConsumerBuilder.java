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

import static com.rabbitmq.client.amqp.impl.Assert.notNull;

import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.Resource;
import java.time.Instant;
import java.util.*;

class AmqpConsumerBuilder implements ConsumerBuilder {

  private final AmqpConnection connection;
  private String queue;
  private Consumer.MessageHandler messageHandler;
  private int initialCredits = 100;
  private final List<Resource.StateListener> listeners = new ArrayList<>();
  private final Map<String, Object> filters = new LinkedHashMap<>();
  private final Map<String, Object> properties = new LinkedHashMap<>();
  private final StreamOptions streamOptions = new DefaultStreamOptions(this, this.filters);

  AmqpConsumerBuilder(AmqpConnection connection) {
    this.connection = connection;
  }

  @Override
  public ConsumerBuilder queue(String queue) {
    this.queue = queue;
    return this;
  }

  @Override
  public ConsumerBuilder messageHandler(Consumer.MessageHandler handler) {
    this.messageHandler = handler;
    return this;
  }

  @Override
  public ConsumerBuilder initialCredits(int initialCredits) {
    this.initialCredits = initialCredits;
    return this;
  }

  @Override
  public ConsumerBuilder priority(int priority) {
    this.properties.put("rabbitmq:priority", priority);
    return this;
  }

  @Override
  public ConsumerBuilder listeners(Resource.StateListener... listeners) {
    if (listeners == null || listeners.length == 0) {
      this.listeners.clear();
    } else {
      this.listeners.addAll(List.of(listeners));
    }
    return this;
  }

  @Override
  public StreamOptions stream() {
    return this.streamOptions;
  }

  AmqpConnection connection() {
    return connection;
  }

  String queue() {
    return queue;
  }

  Consumer.MessageHandler messageHandler() {
    return messageHandler;
  }

  int initialCredits() {
    return this.initialCredits;
  }

  Map<String, Object> properties() {
    return this.properties;
  }

  List<Resource.StateListener> listeners() {
    return listeners;
  }

  Map<String, Object> filters() {
    return this.filters;
  }

  @Override
  public Consumer build() {
    if (this.queue == null || this.queue.isBlank()) {
      throw new IllegalArgumentException("A queue must be specified");
    }
    if (this.messageHandler == null) {
      throw new IllegalArgumentException("Message handler cannot be null");
    }

    // TODO validate stream (filtering) configuration

    return this.connection.createConsumer(this);
  }

  private static class DefaultStreamOptions implements StreamOptions {

    private final Map<String, Object> filters;
    private final ConsumerBuilder builder;

    private DefaultStreamOptions(ConsumerBuilder builder, Map<String, Object> filters) {
      this.builder = builder;
      this.filters = filters;
    }

    @Override
    public StreamOptions offset(long offset) {
      this.filters.put("rabbitmq:stream-offset-spec", offset);
      return this;
    }

    @Override
    public StreamOptions offset(Instant timestamp) {
      notNull(timestamp, "Timestamp offset cannot be null");
      this.offsetSpecification(Date.from(timestamp));
      return this;
    }

    @Override
    public StreamOptions offset(StreamOffsetSpecification specification) {
      notNull(specification, "Offset specification cannot be null");
      this.offsetSpecification(specification.name().toLowerCase(Locale.ENGLISH));
      return this;
    }

    @Override
    public StreamOptions offset(String interval) {
      notNull(interval, "Interval offset cannot be null");
      if (!Utils.validateMaxAge(interval)) {
        throw new IllegalArgumentException(
            "Invalid value for interval: "
                + interval
                + ". "
                + "Valid examples are: 1Y, 7D, 10m. See https://www.rabbitmq.com/docs/streams#retention.");
      }
      this.offsetSpecification(interval);
      return this;
    }

    @Override
    public StreamOptions filterValues(String... values) {
      this.filters.put("rabbitmq:stream-filter", Arrays.asList(values));
      return this;
    }

    @Override
    public StreamOptions filterMatchUnfiltered(boolean matchUnfiltered) {
      this.filters.put("rabbitmq:stream-match-unfiltered", matchUnfiltered);
      return this;
    }

    @Override
    public ConsumerBuilder builder() {
      return this.builder;
    }

    private void offsetSpecification(Object value) {
      this.filters.put("rabbitmq:stream-offset-spec", value);
    }
  }
}
