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
import com.rabbitmq.client.amqp.AmqpException.AmqpConnectionException;
import com.rabbitmq.client.amqp.AmqpException.AmqpResourceClosedException;
import javax.net.ssl.SSLException;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.exceptions.*;
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
        .isInstanceOf(AmqpResourceClosedException.class);
    assertThat(convert(new ClientLinkRemotelyClosedException("", errorCondition(ERROR_NOT_FOUND))))
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class);
    assertThat(
            convert(
                new ClientLinkRemotelyClosedException("", errorCondition(ERROR_RESOURCE_DELETED))))
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class);
    assertThat(convert(new ClientLinkRemotelyClosedException("")))
        .isInstanceOf(AmqpResourceClosedException.class);
    assertThat(convert(new ClientConnectionRemotelyClosedException("connection reset")))
        .isInstanceOf(AmqpConnectionException.class);
    assertThat(convert(new ClientConnectionRemotelyClosedException("connection refused")))
        .isInstanceOf(AmqpConnectionException.class);
    assertThat(convert(new ClientConnectionRemotelyClosedException("connection forced")))
        .isInstanceOf(AmqpConnectionException.class);
    assertThat(convert(new ClientConnectionRemotelyClosedException("", new RuntimeException())))
        .isInstanceOf(AmqpConnectionException.class)
        .hasCauseInstanceOf(ClientConnectionRemotelyClosedException.class);
    assertThat(convert(new ClientConnectionRemotelyClosedException("", new SSLException(""))))
        .isInstanceOf(AmqpException.AmqpSecurityException.class)
        .hasCauseInstanceOf(SSLException.class);
    assertThat(convert(new ClientException("")))
        .isInstanceOf(AmqpException.class)
        .hasCauseInstanceOf(ClientException.class);

    assertThat(
            convert(
                new ClientIllegalStateException(
                    "The Sender was explicitly closed",
                    new ClientLinkRemotelyClosedException(
                        "", errorCondition(ERROR_RESOURCE_DELETED)))))
        .isInstanceOf(AmqpException.AmqpEntityDoesNotExistException.class)
        .hasCauseInstanceOf(ClientLinkRemotelyClosedException.class);
  }

  @Test
  void testNoRunningStreamMemberOnNode() {
    assertThat(
            noRunningStreamMemberOnNode(
                new AmqpResourceClosedException(
                    "stream queue 'stream-RecoveryClusterTest_clusterRestart-a69d-db752afee52a' in vhost '/' does not have a running replica on the local node [condition = amqp:internal-error]")))
        .isTrue();
    assertThat(noRunningStreamMemberOnNode(new AmqpResourceClosedException("noproc"))).isTrue();
    assertThat(noRunningStreamMemberOnNode(new AmqpResourceClosedException("foo"))).isFalse();
    assertThat(noRunningStreamMemberOnNode(new AmqpConnectionException("foo", null))).isFalse();
  }

  ErrorCondition errorCondition(String condition) {
    return ErrorCondition.create(condition, null);
  }
}
