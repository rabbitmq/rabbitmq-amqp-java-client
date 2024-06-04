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
package com.rabbitmq.amqp.client.impl;

import com.rabbitmq.amqp.client.ModelException;
import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;

abstract class ExceptionUtils {

  private ExceptionUtils() {}

  static ModelException convert(ClientException e) {
    // TODO convert Proton exception into exception of lib hierarchy
    if (e.getCause() != null && e.getCause() instanceof SSLException) {
      return new ModelException(e.getCause());
    } else {
      return new ModelException(e);
    }
  }

  static ModelException convert(ExecutionException e) {
    // TODO convert Proton exception into exception of lib hierarchy
    if (e.getCause() != null && e.getCause() instanceof ClientException) {
      return convert((ClientException) e.getCause());
    } else {
      return new ModelException(e);
    }
  }

  static ModelException convert(ClientException e, String format, Object... args) {
    // TODO convert Proton exception into exception of lib hierarchy
    return new ModelException(String.format(format, args), e);
  }

  static boolean resourceDeleted(ClientResourceRemotelyClosedException e) {
    return e.getErrorCondition() != null
        && "amqp:resource-deleted".equals(e.getErrorCondition().condition());
  }

  static boolean notFound(ClientResourceRemotelyClosedException e) {
    return e.getErrorCondition() != null
        && "amqp:not-found".equals(e.getErrorCondition().condition());
  }
}
