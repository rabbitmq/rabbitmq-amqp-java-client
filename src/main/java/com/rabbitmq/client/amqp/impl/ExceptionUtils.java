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
  static final String ERROR_NOT_FOUND = "amqp:not-found";
  static final String ERROR_RESOURCE_DELETED = "amqp:resource-deleted";

  private ExceptionUtils() {}

  static <T> T wrapGet(Future<T> future) throws ClientException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ClientException) {
        throw (ClientException) e.getCause();
      } else {
        throw convert(e);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AmqpException(e);
    }
  }

  static AmqpException convert(ClientException e) {
    return convert(e, null);
  }

  static AmqpException convert(ExecutionException e) {
    if (e.getCause() instanceof ClientException) {
      return convert((ClientException) e.getCause());
    } else {
      return new AmqpException(e.getCause() == null ? e : e.getCause());
    }
  }

  static AmqpException convert(ClientException e, String format, Object... args) {
    String message = format != null ? String.format(format, args) : null;
    if (e.getCause() instanceof SSLException) {
      return new AmqpException.AmqpSecurityException(message, e.getCause());
    } else if (e instanceof ClientConnectionSecurityException) {
      return new AmqpException.AmqpSecurityException(message, e);
    } else if (isNetworkError(e)) {
      return new AmqpException.AmqpConnectionException(e.getMessage(), e);
    } else if (e instanceof ClientSessionRemotelyClosedException) {
      ErrorCondition errorCondition =
          ((ClientSessionRemotelyClosedException) e).getErrorCondition();
      if (isUnauthorizedAccess(errorCondition)) {
        return new AmqpException.AmqpSecurityException(e.getMessage(), e);
      } else if (isNotFound(errorCondition)) {
        return new AmqpException.AmqpEntityDoesNotExistException(e.getMessage(), e);
      } else {
        return new AmqpException.AmqpResourceClosedException(e.getMessage(), e);
      }
    } else if (e instanceof ClientLinkRemotelyClosedException) {
      ErrorCondition errorCondition = ((ClientLinkRemotelyClosedException) e).getErrorCondition();
      if (isNotFound(errorCondition)) {
        return new AmqpException.AmqpEntityDoesNotExistException(e.getMessage(), e);
      } else if (isResourceDeleted(errorCondition)) {
        return new AmqpException.AmqpEntityDoesNotExistException(e.getMessage(), e);
      } else {
        return new AmqpException.AmqpResourceClosedException(e.getMessage(), e);
      }
    } else if (e instanceof ClientConnectionRemotelyClosedException) {
      ErrorCondition errorCondition =
          ((ClientConnectionRemotelyClosedException) e).getErrorCondition();
      if (isNetworkError(e) || !isUnauthorizedAccess(errorCondition)) {
        return new AmqpException.AmqpConnectionException(e.getMessage(), e);
      } else {
        return new AmqpException(e.getMessage(), e);
      }
    } else {
      return new AmqpException(message, e);
    }
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

  private static boolean isNotFound(ErrorCondition errorCondition) {
    return errorConditionEquals(errorCondition, ERROR_NOT_FOUND);
  }

  private static boolean isResourceDeleted(ErrorCondition errorCondition) {
    return errorConditionEquals(errorCondition, ERROR_RESOURCE_DELETED);
  }

  private static boolean errorConditionEquals(ErrorCondition errorCondition, String expected) {
    return errorCondition != null && expected.equals(errorCondition.condition());
  }

  private static boolean isNetworkError(ClientException e) {
    if (e instanceof ClientConnectionRemotelyClosedException) {
      String message = e.getMessage();
      if (message != null) {
        message = message.toLowerCase();
        return message.contains("connection reset") || message.contains("connection refused");
      }
    }
    return false;
  }
}
