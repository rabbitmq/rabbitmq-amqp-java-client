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

import static com.rabbitmq.client.amqp.impl.ExceptionUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.amqp.AmqpException;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientSessionRemotelyClosedException;
import org.junit.jupiter.api.Test;

public class ExceptionUtilsTest {

  @Test
  void convertTest() {
    assertThat(
            convert(
                new ClientSessionRemotelyClosedException(
                    "", errorCondition(ERROR_UNAUTHORIZED_ACCESS))))
        .isInstanceOf(AmqpException.AmqpSecurityException.class);
    assertThat(
            convert(new ClientSessionRemotelyClosedException("", errorCondition(ERROR_NOT_FOUND))))
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class);
    assertThat(convert(new ClientSessionRemotelyClosedException("")))
        .isInstanceOf(AmqpException.AmqpResourceClosedException.class);
    assertThat(convert(new ClientLinkRemotelyClosedException("", errorCondition(ERROR_NOT_FOUND))))
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class);
    assertThat(
            convert(
                new ClientLinkRemotelyClosedException("", errorCondition(ERROR_RESOURCE_DELETED))))
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class);
    assertThat(convert(new ClientLinkRemotelyClosedException("")))
        .isInstanceOf(AmqpException.AmqpResourceClosedException.class);
  }

  ErrorCondition errorCondition(String condition) {
    return ErrorCondition.create(condition, null);
  }
}
