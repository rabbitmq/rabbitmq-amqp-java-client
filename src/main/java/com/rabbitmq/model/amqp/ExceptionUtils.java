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
package com.rabbitmq.model.amqp;

import com.rabbitmq.model.ModelException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;

abstract class ExceptionUtils {

  private ExceptionUtils() {}

  static ModelException convert(ClientException e) {
    // TODO convert Proton exception into exception of lib hierarchy
    return new ModelException(e);
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
    System.out.println(e.getMessage());
    return e.getErrorCondition() != null
        && "amqp:not-found".equals(e.getErrorCondition().condition());
  }
}
