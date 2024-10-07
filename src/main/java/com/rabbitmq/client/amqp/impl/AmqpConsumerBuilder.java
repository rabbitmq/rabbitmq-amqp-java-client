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
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import org.apache.qpid.protonj2.types.*;

class AmqpConsumerBuilder implements ConsumerBuilder {

  static SubscriptionListener NO_OP_SUBSCRIPTION_LISTENER = ctx -> {};

  private final AmqpConnection connection;
  private String queue;
  private Consumer.MessageHandler messageHandler;
  private int initialCredits = 100;
  private final List<Resource.StateListener> listeners = new ArrayList<>();
  private final Map<String, DescribedType> filters = new LinkedHashMap<>();
  private final Map<String, Object> properties = new LinkedHashMap<>();
  private final StreamOptions streamOptions = new DefaultStreamOptions(this, this.filters);
  private SubscriptionListener subscriptionListener = NO_OP_SUBSCRIPTION_LISTENER;

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

  @Override
  public ConsumerBuilder subscriptionListener(SubscriptionListener subscriptionListener) {
    this.subscriptionListener = subscriptionListener;
    return this;
  }

  SubscriptionListener subscriptionListener() {
    return this.subscriptionListener;
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

  Map<String, DescribedType> filters() {
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
    return this.connection.createConsumer(this);
  }

  private static class DefaultStreamOptions implements StreamOptions {

    private final Map<String, DescribedType> filters;
    private final ConsumerBuilder builder;
    private final StreamFilterOptions filterOptions;

    private DefaultStreamOptions(ConsumerBuilder builder, Map<String, DescribedType> filters) {
      this.builder = builder;
      this.filters = filters;
      this.filterOptions = new DefaultStreamFilterOptions(this, filters);
    }

    @Override
    public StreamOptions offset(long offset) {
      return this.filter("rabbitmq:stream-offset-spec", offset);
    }

    @Override
    public StreamOptions offset(Instant timestamp) {
      notNull(timestamp, "Timestamp offset cannot be null");
      return this.offsetSpecification(Date.from(timestamp));
    }

    @Override
    public StreamOptions offset(StreamOffsetSpecification specification) {
      notNull(specification, "Offset specification cannot be null");
      return this.offsetSpecification(specification.name().toLowerCase(Locale.ENGLISH));
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
      return this.offsetSpecification(interval);
    }

    @Override
    public StreamOptions filterValues(String... values) {
      if (values == null || values.length == 0) {
        throw new IllegalArgumentException("At least one stream filter value must specified");
      }
      return this.filter("rabbitmq:stream-filter", Arrays.asList(values));
    }

    @Override
    public StreamOptions filterMatchUnfiltered(boolean matchUnfiltered) {
      return this.filter("rabbitmq:stream-match-unfiltered", matchUnfiltered);
    }

    @Override
    public StreamFilterOptions filter() {
      return this.filterOptions;
    }

    @Override
    public ConsumerBuilder builder() {
      return this.builder;
    }

    private StreamOptions offsetSpecification(Object value) {
      return filter("rabbitmq:stream-offset-spec", value);
    }

    private StreamOptions filter(String key, Object value) {
      AmqpConsumerBuilder.filter(this.filters, key, value);
      return this;
    }
  }

  private static void filter(Map<String, DescribedType> filters, String filterName, Object value) {
    filters.put(filterName, filterValue(filterName, value));
  }

  private static DescribedType filterValue(String filterName, Object value) {
    return new UnknownDescribedType(Symbol.getSymbol(filterName), value);
  }

  static StreamOptions streamOptions(Map<String, DescribedType> filters) {
    return new DefaultStreamOptions(null, filters);
  }

  private static class DefaultStreamFilterOptions implements StreamFilterOptions {

    private final StreamOptions streamOptions;
    private final Map<String, DescribedType> filters;

    private DefaultStreamFilterOptions(
        StreamOptions streamOptions, Map<String, DescribedType> filters) {
      this.streamOptions = streamOptions;
      this.filters = filters;
    }

    @Override
    public StreamFilterOptions messageId(Object id) {
      return propertyFilter("message-id", id);
    }

    @Override
    public StreamFilterOptions messageId(String id) {
      return propertyFilter("message-id", id);
    }

    @Override
    public StreamFilterOptions messageId(long id) {
      return propertyFilter("message-id", new UnsignedLong(id));
    }

    @Override
    public StreamFilterOptions messageId(byte[] id) {
      return propertyFilter("message-id", new Binary(id));
    }

    @Override
    public StreamFilterOptions messageId(UUID id) {
      return propertyFilter("message-id", id);
    }

    @Override
    public StreamFilterOptions correlationId(Object correlationId) {
      return propertyFilter("correlation-id", correlationId);
    }

    @Override
    public StreamFilterOptions correlationId(String correlationId) {
      return propertyFilter("correlation-id", correlationId);
    }

    @Override
    public StreamFilterOptions correlationId(long correlationId) {
      return propertyFilter("correlation-id", new UnsignedLong(correlationId));
    }

    @Override
    public StreamFilterOptions correlationId(byte[] correlationId) {
      return propertyFilter("correlation-id", new Binary(correlationId));
    }

    @Override
    public StreamFilterOptions correlationId(UUID correlationId) {
      return propertyFilter("correlation-id", correlationId);
    }

    @Override
    public StreamFilterOptions userId(byte[] userId) {
      return propertyFilter("user-id", new Binary(userId));
    }

    @Override
    public StreamFilterOptions to(String to) {
      return propertyFilter("to", to);
    }

    @Override
    public StreamFilterOptions subject(String subject) {
      return propertyFilter("subject", subject);
    }

    @Override
    public StreamFilterOptions replyTo(String replyTo) {
      return propertyFilter("reply-to", replyTo);
    }

    @Override
    public StreamFilterOptions contentType(String contentType) {
      return propertyFilter("content-type", Symbol.valueOf(contentType));
    }

    @Override
    public StreamFilterOptions contentEncoding(String contentEncoding) {
      return propertyFilter("content-encoding", Symbol.valueOf(contentEncoding));
    }

    @Override
    public StreamFilterOptions absoluteExpiryTime(long absoluteExpiryTime) {
      return propertyFilter("absolute-expiry-time", new Date(absoluteExpiryTime));
    }

    @Override
    public StreamFilterOptions creationTime(long creationTime) {
      return propertyFilter("creation-time", new Date(creationTime));
    }

    @Override
    public StreamFilterOptions groupId(String groupId) {
      return propertyFilter("group-id", groupId);
    }

    @Override
    public StreamFilterOptions groupSequence(int groupSequence) {
      return propertyFilter("group-sequence", UnsignedInteger.valueOf(groupSequence));
    }

    @Override
    public StreamFilterOptions replyToGroupId(String groupId) {
      return propertyFilter("reply-to-group-id", groupId);
    }

    @Override
    public StreamFilterOptions property(String key, boolean value) {
      return this.applicationPropertyFilter(key, value);
    }

    @Override
    public StreamFilterOptions property(String key, byte value) {
      return this.applicationPropertyFilter(key, value);
    }

    @Override
    public StreamFilterOptions property(String key, short value) {
      return this.applicationPropertyFilter(key, value);
    }

    @Override
    public StreamFilterOptions property(String key, int value) {
      return this.applicationPropertyFilter(key, value);
    }

    @Override
    public StreamFilterOptions property(String key, long value) {
      return this.applicationPropertyFilter(key, value);
    }

    @Override
    public StreamFilterOptions propertyUnsigned(String key, byte value) {
      return this.applicationPropertyFilter(key, UnsignedByte.valueOf(value));
    }

    @Override
    public StreamFilterOptions propertyUnsigned(String key, short value) {
      return this.applicationPropertyFilter(key, UnsignedShort.valueOf(value));
    }

    @Override
    public StreamFilterOptions propertyUnsigned(String key, int value) {
      return this.applicationPropertyFilter(key, UnsignedInteger.valueOf(value));
    }

    @Override
    public StreamFilterOptions propertyUnsigned(String key, long value) {
      return this.applicationPropertyFilter(key, UnsignedLong.valueOf(value));
    }

    @Override
    public StreamFilterOptions property(String key, float value) {
      return this.applicationPropertyFilter(key, value);
    }

    @Override
    public StreamFilterOptions property(String key, double value) {
      return this.applicationPropertyFilter(key, value);
    }

    @Override
    public StreamFilterOptions propertyDecimal32(String key, BigDecimal value) {
      return this.applicationPropertyFilter(key, new Decimal32(value));
    }

    @Override
    public StreamFilterOptions propertyDecimal64(String key, BigDecimal value) {
      return this.applicationPropertyFilter(key, new Decimal64(value));
    }

    @Override
    public StreamFilterOptions propertyDecimal128(String key, BigDecimal value) {
      return this.applicationPropertyFilter(key, new Decimal128(value));
    }

    @Override
    public StreamFilterOptions property(String key, char value) {
      return this.applicationPropertyFilter(key, value);
    }

    @Override
    public StreamFilterOptions propertyTimestamp(String key, long value) {
      return this.applicationPropertyFilter(key, new Date(value));
    }

    @Override
    public StreamFilterOptions property(String key, UUID value) {
      return this.applicationPropertyFilter(key, value);
    }

    @Override
    public StreamFilterOptions property(String key, byte[] value) {
      return this.applicationPropertyFilter(key, new Binary(value));
    }

    @Override
    public StreamFilterOptions property(String key, String value) {
      return this.applicationPropertyFilter(key, value);
    }

    @Override
    public StreamFilterOptions propertySymbol(String key, String value) {
      return this.applicationPropertyFilter(key, Symbol.valueOf(value));
    }

    @Override
    public StreamOptions stream() {
      return this.streamOptions;
    }

    private StreamFilterOptions propertyFilter(String propertyKey, Object propertyValue) {
      Map<Symbol, Object> filter = filter("amqp:properties-filter");
      filter.put(Symbol.valueOf(propertyKey), propertyValue);
      return this;
    }

    private StreamFilterOptions applicationPropertyFilter(
        String propertyKey, Object propertyValue) {
      Map<String, Object> filter = filter("amqp:application-properties-filter");
      filter.put(propertyKey, propertyValue);
      return this;
    }

    @SuppressWarnings("unchecked")
    private <K> Map<K, Object> filter(String filterName) {
      DescribedType type =
          this.filters.computeIfAbsent(
              filterName, fName -> filterValue(fName, new LinkedHashMap<>()));
      return (Map<K, Object>) type.getDescribed();
    }
  }
}
