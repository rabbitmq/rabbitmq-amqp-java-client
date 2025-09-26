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
import java.util.function.Consumer;
import javax.net.ssl.SSLException;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionSecurityException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientSessionRemotelyClosedException;

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

  static AmqpException convert(Exception e) {
    if (e instanceof AmqpException) {
      return (AmqpException) e;
    } else if (e instanceof ClientException) {
      return convert((ClientException) e);
    } else {
      return new AmqpException(e);
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
    return convert(e, true, format, args);
  }

  private static AmqpException convert(
      ClientException e, boolean checkCause, String format, Object... args) {
    String message = format != null ? String.format(format, args) : null;
    AmqpException result;
    if (e.getCause() instanceof SSLException) {
      result = new AmqpException.AmqpSecurityException(message, e.getCause());
    } else if (e instanceof ClientConnectionSecurityException) {
      result = new AmqpException.AmqpSecurityException(message, e);
    } else if (isNetworkError(e)) {
      result = new AmqpException.AmqpConnectionException(e.getMessage(), e);
    } else if (e instanceof ClientSessionRemotelyClosedException
        || e instanceof ClientLinkRemotelyClosedException) {
      ErrorCondition errorCondition =
          ((ClientResourceRemotelyClosedException) e).getErrorCondition();
      if (isUnauthorizedAccess(errorCondition)) {
        result = new AmqpException.AmqpSecurityException(e.getMessage(), e);
      } else if (isNotFound(errorCondition)) {
        result = new AmqpException.AmqpEntityDoesNotExistException(e.getMessage(), e);
      } else if (isResourceDeleted(errorCondition)) {
        result = new AmqpException.AmqpEntityDoesNotExistException(e.getMessage(), e);
      } else {
        result = new AmqpException.AmqpResourceClosedException(e.getMessage(), e);
      }
    } else if (e instanceof ClientConnectionRemotelyClosedException) {
      ErrorCondition errorCondition =
          ((ClientConnectionRemotelyClosedException) e).getErrorCondition();
      if (isNetworkError(e) || !isUnauthorizedAccess(errorCondition)) {
        result = new AmqpException.AmqpConnectionException(e.getMessage(), e);
      } else {
        result = new AmqpException(e.getMessage(), e);
      }
    } else {
      result = new AmqpException(message, e);
    }
    if (checkCause
        && AmqpException.class.getName().equals(result.getClass().getName())
        && e.getCause() instanceof ClientException) {
      // we end up with a generic exception, we try to narrow down with the cause
      result = convert((ClientException) e.getCause(), false, format, args);
    }
    return result;
  }

  static boolean resourceDeleted(ClientResourceRemotelyClosedException e) {
    return e.getErrorCondition() != null
        && "amqp:resource-deleted".equals(e.getErrorCondition().condition());
  }

  static boolean notFound(ClientResourceRemotelyClosedException e) {
    return e.getErrorCondition() != null
        && "amqp:not-found".equals(e.getErrorCondition().condition());
  }

  static boolean unauthorizedAccess(ClientResourceRemotelyClosedException e) {
    return isUnauthorizedAccess(e.getErrorCondition());
  }

  static boolean noRunningStreamMemberOnNode(Exception e) {
    if (e instanceof AmqpException.AmqpResourceClosedException) {
      String message = e.getMessage();
      if (message == null) {
        return false;
      } else {
        return (message.contains("stream queue")
                && message.contains("does not have a running replica on the local node"))
            || message.contains("noproc");
      }
    } else {
      return false;
    }
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

  static boolean maybeCloseOnException(Consumer<Throwable> closing, Exception ex) {
    if (ex instanceof ClientLinkRemotelyClosedException
        || ex instanceof ClientSessionRemotelyClosedException) {
      ClientResourceRemotelyClosedException e = (ClientResourceRemotelyClosedException) ex;
      if (notFound(e) || resourceDeleted(e) || unauthorizedAccess(e)) {
        closing.accept(ExceptionUtils.convert(e));
        return true;
      }
    }
    return false;
  }
}
