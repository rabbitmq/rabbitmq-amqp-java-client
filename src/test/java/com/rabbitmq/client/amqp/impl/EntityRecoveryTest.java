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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Management;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityRecoveryTest {

  private static final String EXCLUSIVE_ACCESS_MSG_FORMAT =
      "Unexpected response code: 400 instead of 200, 201 (message: 'cannot obtain exclusive access to locked queue '%s' in vhost '/'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.')";
  private static final String NO_EXCHANGE_MSG_FORMAT =
      "Unexpected response code: 400 instead of 204 (message: 'no exchange '%s' in vhost '/'')";
  private static final String NO_QUEUE_MSG_FORMAT =
      "Unexpected response code: 400 instead of 204 (message: 'no queue '%s' in vhost '/'')";

  @Mock AmqpConnection connection;
  @Mock RecordingTopologyListener topologyListener;
  @Mock Management management;
  @Mock Management.QueueSpecification queueSpec;
  @Mock Management.QueueInfo queueInfo;
  @Mock Management.ExchangeSpecification exchangeSpec;
  @Mock Management.BindingSpecification bindingSpec;
  @Mock RecordingTopologyListener.QueueSpec mockQueueSpec;
  @Mock RecordingTopologyListener.ExchangeSpec mockExchangeSpec;
  @Mock RecordingTopologyListener.ExchangeSpec mockSourceExchangeSpec;
  @Mock RecordingTopologyListener.BindingSpec mockBindingSpec;

  EntityRecovery entityRecovery;
  BackOffDelayPolicy backOffDelayPolicy;

  @BeforeEach
  void setUp() {
    backOffDelayPolicy = BackOffDelayPolicy.fixed(Duration.ofMillis(10));
    lenient().when(connection.managementNoCheck()).thenReturn(management);
    lenient().when(connection.recoveryBackOffDelayPolicy()).thenReturn(backOffDelayPolicy);
    lenient().when(management.queue()).thenReturn(queueSpec);
    lenient().when(queueSpec.name(anyString())).thenReturn(queueSpec);
    lenient().when(queueSpec.exclusive(anyBoolean())).thenReturn(queueSpec);
    lenient().when(queueSpec.autoDelete(anyBoolean())).thenReturn(queueSpec);
    lenient().when(queueSpec.argument(anyString(), any())).thenReturn(queueSpec);
    lenient().when(queueSpec.declare()).thenReturn(queueInfo);

    lenient().when(management.exchange()).thenReturn(exchangeSpec);
    lenient().when(exchangeSpec.name(anyString())).thenReturn(exchangeSpec);
    lenient().when(exchangeSpec.autoDelete(anyBoolean())).thenReturn(exchangeSpec);
    lenient().when(exchangeSpec.type(anyString())).thenReturn(exchangeSpec);
    lenient().when(exchangeSpec.argument(anyString(), any())).thenReturn(exchangeSpec);

    lenient().when(management.binding()).thenReturn(bindingSpec);
    lenient().when(bindingSpec.sourceExchange(anyString())).thenReturn(bindingSpec);
    lenient().when(bindingSpec.destinationQueue(anyString())).thenReturn(bindingSpec);
    lenient().when(bindingSpec.destinationExchange(anyString())).thenReturn(bindingSpec);
    lenient().when(bindingSpec.key(anyString())).thenReturn(bindingSpec);
    lenient().when(bindingSpec.argument(anyString(), any())).thenReturn(bindingSpec);

    entityRecovery = new EntityRecovery(connection, topologyListener);
  }

  @Test
  void recoverQueueShouldSucceedOnFirstAttemptForExclusiveQueue() {
    when(mockQueueSpec.name()).thenReturn("test-queue");
    when(mockQueueSpec.exclusive()).thenReturn(true);
    when(mockQueueSpec.autoDelete()).thenReturn(false);
    when(mockQueueSpec.arguments()).thenReturn(Collections.emptyMap());

    entityRecovery.recoverQueue(mockQueueSpec);

    verify(connection).managementNoCheck();
    verify(management).queue();
    verify(queueSpec).name("test-queue");
    verify(queueSpec).exclusive(true);
    verify(queueSpec).autoDelete(false);
    verify(queueSpec).declare();
  }

  @Test
  void recoverQueueShouldSucceedOnFirstAttemptForAutoDeleteQueue() {
    when(mockQueueSpec.name()).thenReturn("test-queue");
    when(mockQueueSpec.exclusive()).thenReturn(false);
    when(mockQueueSpec.autoDelete()).thenReturn(true);
    when(mockQueueSpec.arguments()).thenReturn(Collections.emptyMap());

    entityRecovery.recoverQueue(mockQueueSpec);

    verify(connection).managementNoCheck();
    verify(management).queue();
    verify(queueSpec).name("test-queue");
    verify(queueSpec).exclusive(false);
    verify(queueSpec).autoDelete(true);
    verify(queueSpec).declare();
  }

  @Test
  void recoverQueueShouldRetryOnExclusiveAccessException() {
    String q = "test-queue";
    AmqpException exclusiveAccessException =
        new AmqpException(String.format(EXCLUSIVE_ACCESS_MSG_FORMAT, q));

    when(mockQueueSpec.name()).thenReturn(q);
    when(mockQueueSpec.exclusive()).thenReturn(true);
    when(mockQueueSpec.autoDelete()).thenReturn(false);
    when(mockQueueSpec.arguments()).thenReturn(Collections.emptyMap());

    when(queueSpec.declare()).thenThrow(exclusiveAccessException).thenReturn(queueInfo);

    entityRecovery.recoverQueue(mockQueueSpec);

    verify(queueSpec, times(2)).declare();
  }

  @Test
  void recoverQueueShouldNotRetryOnExclusiveAccessExceptionForNonExclusiveQueue() {
    String q = "test-queue";
    AmqpException exclusiveAccessException =
        new AmqpException(String.format(EXCLUSIVE_ACCESS_MSG_FORMAT, q));

    when(mockQueueSpec.name()).thenReturn("test-queue");
    when(mockQueueSpec.exclusive()).thenReturn(false);
    when(mockQueueSpec.autoDelete()).thenReturn(true);
    when(mockQueueSpec.arguments()).thenReturn(Collections.emptyMap());

    when(queueSpec.declare()).thenThrow(exclusiveAccessException);

    entityRecovery.recoverQueue(mockQueueSpec);

    verify(queueSpec, times(1)).declare();
  }

  @Test
  void recoverQueueShouldNotRetryOnOtherExceptions() {
    AmqpException otherException = new AmqpException("Some other error");

    when(mockQueueSpec.name()).thenReturn("test-queue");
    when(mockQueueSpec.exclusive()).thenReturn(true);
    when(mockQueueSpec.autoDelete()).thenReturn(false);
    when(mockQueueSpec.arguments()).thenReturn(Collections.emptyMap());

    when(queueSpec.declare()).thenThrow(otherException);

    entityRecovery.recoverQueue(mockQueueSpec);

    verify(queueSpec, times(1)).declare();
  }

  @Test
  void recoverQueueShouldSkipNonExclusiveNonAutoDeleteQueues() {
    when(mockQueueSpec.exclusive()).thenReturn(false);
    when(mockQueueSpec.autoDelete()).thenReturn(false);

    entityRecovery.recoverQueue(mockQueueSpec);

    verify(connection, never()).managementNoCheck();
    verify(management, never()).queue();
  }

  @Test
  void recoverQueueShouldHandleArguments() {
    HashMap<String, Object> arguments = new HashMap<>();
    arguments.put("x-max-length", 1000);
    arguments.put("x-message-ttl", 60000);

    when(mockQueueSpec.name()).thenReturn("test-queue");
    when(mockQueueSpec.exclusive()).thenReturn(true);
    when(mockQueueSpec.autoDelete()).thenReturn(false);
    when(mockQueueSpec.arguments()).thenReturn(arguments);

    entityRecovery.recoverQueue(mockQueueSpec);

    verify(queueSpec).argument("x-max-length", 1000);
    verify(queueSpec).argument("x-message-ttl", 60000);
    verify(queueSpec).declare();
  }

  @Test
  void recoverBindingShouldSucceedOnFirstAttempt() {
    when(mockBindingSpec.source()).thenReturn("ex");
    when(mockBindingSpec.destination()).thenReturn("q");
    when(mockBindingSpec.key()).thenReturn("foo");
    when(mockBindingSpec.toQueue()).thenReturn(true);
    when(mockBindingSpec.arguments()).thenReturn(Collections.emptyMap());

    entityRecovery.recoverBinding(mockBindingSpec, List.of(), List.of());

    verify(bindingSpec).bind();
    verify(management, never()).exchange();
    verify(management, never()).queue();
  }

  @Test
  void recoverBindingShouldRecreateExchangeAndQueueOnNotFoundExceptionForQueueBinding() {
    String exchangeName = "ex";
    String queueName = "q";
    when(mockBindingSpec.source()).thenReturn(exchangeName);
    when(mockBindingSpec.destination()).thenReturn(queueName);
    when(mockBindingSpec.key()).thenReturn("foo");
    when(mockBindingSpec.toQueue()).thenReturn(true);
    when(mockBindingSpec.arguments()).thenReturn(Collections.emptyMap());

    AmqpException notFoundException =
        new AmqpException(String.format(NO_EXCHANGE_MSG_FORMAT, exchangeName));
    doThrow(notFoundException).doNothing().when(bindingSpec).bind();

    when(mockExchangeSpec.name()).thenReturn(exchangeName);
    when(mockExchangeSpec.autoDelete()).thenReturn(true);
    when(mockExchangeSpec.type()).thenReturn("direct");
    when(mockExchangeSpec.arguments()).thenReturn(Collections.emptyMap());

    when(mockQueueSpec.name()).thenReturn(queueName);
    when(mockQueueSpec.exclusive()).thenReturn(true);
    when(mockQueueSpec.autoDelete()).thenReturn(false);
    when(mockQueueSpec.arguments()).thenReturn(Collections.emptyMap());

    entityRecovery.recoverBinding(
        mockBindingSpec, List.of(mockExchangeSpec), List.of(mockQueueSpec));

    verify(bindingSpec, times(2)).bind();
    verify(exchangeSpec).declare();
    verify(queueSpec).declare();
  }

  @Test
  void recoverBindingShouldRecreateExchangesOnNotFoundExceptionForExchangeToExchangeBinding() {
    String sourceExchange = "src-ex";
    String destinationExchange = "dest-ex";
    when(mockBindingSpec.source()).thenReturn(sourceExchange);
    when(mockBindingSpec.destination()).thenReturn(destinationExchange);
    when(mockBindingSpec.key()).thenReturn("foo");
    when(mockBindingSpec.toQueue()).thenReturn(false);
    when(mockBindingSpec.arguments()).thenReturn(Collections.emptyMap());

    AmqpException notFoundException =
        new AmqpException(String.format(NO_EXCHANGE_MSG_FORMAT, destinationExchange));
    doThrow(notFoundException).doNothing().when(bindingSpec).bind();

    when(mockSourceExchangeSpec.name()).thenReturn(sourceExchange);
    when(mockSourceExchangeSpec.autoDelete()).thenReturn(false);
    when(mockSourceExchangeSpec.type()).thenReturn("direct");
    when(mockSourceExchangeSpec.arguments()).thenReturn(Collections.emptyMap());

    when(mockExchangeSpec.name()).thenReturn(destinationExchange);
    when(mockExchangeSpec.autoDelete()).thenReturn(true);
    when(mockExchangeSpec.type()).thenReturn("direct");
    when(mockExchangeSpec.arguments()).thenReturn(Collections.emptyMap());

    entityRecovery.recoverBinding(
        mockBindingSpec, List.of(mockSourceExchangeSpec, mockExchangeSpec), List.of());

    verify(bindingSpec, times(2)).bind();
    verify(exchangeSpec, times(2)).declare();
    verify(management, never()).queue();
  }

  @Test
  void recoverBindingShouldNotRetryOnOtherExceptions() {
    when(mockBindingSpec.source()).thenReturn("ex");
    when(mockBindingSpec.destination()).thenReturn("q");
    when(mockBindingSpec.key()).thenReturn("foo");
    when(mockBindingSpec.toQueue()).thenReturn(true);
    when(mockBindingSpec.arguments()).thenReturn(Collections.emptyMap());

    doThrow(new AmqpException("Some other error")).when(bindingSpec).bind();

    entityRecovery.recoverBinding(
        mockBindingSpec, List.of(mockExchangeSpec), List.of(mockQueueSpec));

    verify(bindingSpec, times(1)).bind();
    verify(management, never()).exchange();
    verify(management, never()).queue();
  }

  @Test
  void recoverBindingShouldGiveUpAfterOneRetry() {
    String exchangeName = "ex";
    String queueName = "q";
    when(mockBindingSpec.source()).thenReturn(exchangeName);
    when(mockBindingSpec.destination()).thenReturn(queueName);
    when(mockBindingSpec.key()).thenReturn("foo");
    when(mockBindingSpec.toQueue()).thenReturn(true);
    when(mockBindingSpec.arguments()).thenReturn(Collections.emptyMap());

    AmqpException notFoundException =
        new AmqpException(String.format(NO_QUEUE_MSG_FORMAT, queueName));
    doThrow(notFoundException).when(bindingSpec).bind();

    when(mockExchangeSpec.name()).thenReturn(exchangeName);
    when(mockExchangeSpec.autoDelete()).thenReturn(true);
    when(mockExchangeSpec.type()).thenReturn("direct");
    when(mockExchangeSpec.arguments()).thenReturn(Collections.emptyMap());

    when(mockQueueSpec.name()).thenReturn(queueName);
    when(mockQueueSpec.exclusive()).thenReturn(true);
    when(mockQueueSpec.autoDelete()).thenReturn(false);
    when(mockQueueSpec.arguments()).thenReturn(Collections.emptyMap());

    entityRecovery.recoverBinding(
        mockBindingSpec, List.of(mockExchangeSpec), List.of(mockQueueSpec));

    verify(bindingSpec, times(2)).bind();
  }
}
