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

import com.rabbitmq.client.amqp.AmqpException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.net.ssl.SSLException;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.exceptions.*;

abstract class ExceptionUtils {

  static final String ERROR_UNAUTHORIZED_ACCESS = "amqp:unauthorized-access";

  private ExceptionUtils() {}

  static <T> T wrapGet(Future<T> future)
      throws ExecutionException, InterruptedException, ClientException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ClientException) {
        throw (ClientException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  static AmqpException convert(ClientException e) {
    // TODO convert Proton exception into exception of lib hierarchy
    if (e.getCause() instanceof SSLException) {
      return new AmqpException.AmqpSecurityException(e.getCause());
    } else if (e instanceof ClientConnectionSecurityException) {
      throw new AmqpException.AmqpSecurityException(e);
    } else if (e instanceof ClientConnectionRemotelyClosedException) {
      return new AmqpException(e);
    } else if (e instanceof ClientSessionRemotelyClosedException) {
      if (isUnauthorizedAccess(((ClientSessionRemotelyClosedException) e).getErrorCondition())) {
        return new AmqpException.AmqpSecurityException(e.getMessage(), e);
      } else {
        return new AmqpException(e);
      }
    } else {
      return new AmqpException(e);
    }
  }

  static AmqpException convertOnConnection(ClientException e) {
    if (e instanceof ClientConnectionRemotelyClosedException) {
      // the user does not have access to the virtual host or TLS error
      return new AmqpException.AmqpSecurityException(
          e.getCause() instanceof SSLException ? e.getCause() : e);
    } else {
      return convert(e);
    }
  }

  static AmqpException convert(ExecutionException e) {
    if (e.getCause() instanceof ClientException) {
      return convert((ClientException) e.getCause());
    } else {
      return new AmqpException(e.getCause() == null ? e : e.getCause());
    }
  }

  static AmqpException convert(ClientException e, String format, Object... args) {
    // TODO convert Proton exception into exception of lib hierarchy
    return new AmqpException(String.format(format, args), e);
  }

  static boolean resourceDeleted(ClientResourceRemotelyClosedException e) {
    return e.getErrorCondition() != null
        && "amqp:resource-deleted".equals(e.getErrorCondition().condition());
  }

  static boolean notFound(ClientResourceRemotelyClosedException e) {
    return e.getErrorCondition() != null
        && "amqp:not-found".equals(e.getErrorCondition().condition());
  }

  private static boolean isUnauthorizedAccess(ErrorCondition errorCondition) {
    return errorConditionEquals(errorCondition, ERROR_UNAUTHORIZED_ACCESS);
  }

  private static boolean errorConditionEquals(ErrorCondition errorCondition, String expected) {
    return errorCondition != null && expected.equals(errorCondition.condition());
  }
}
