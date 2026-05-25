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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityRecoveryTest {

  private static final String EXCLUSIVE_ACCESS_MSG_FORMAT =
      "Unexpected response code: 400 instead of 200, 201 (message: 'cannot obtain exclusive access to locked queue '%s' in vhost '/'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.')";

  @Mock AmqpConnection connection;
  @Mock RecordingTopologyListener topologyListener;
  @Mock Management management;
  @Mock Management.QueueSpecification queueSpec;
  @Mock Management.QueueInfo queueInfo;
  @Mock RecordingTopologyListener.QueueSpec mockQueueSpec;

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
}
