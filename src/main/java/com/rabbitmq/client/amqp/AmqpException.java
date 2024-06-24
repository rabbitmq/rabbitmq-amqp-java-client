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
package com.rabbitmq.client.amqp;

public class AmqpException extends RuntimeException {

  public AmqpException(Throwable cause) {
    super(cause);
  }

  public AmqpException(String format, Object... args) {
    super(String.format(format, args));
  }

  public AmqpException(String message, Throwable cause) {
    super(message, cause);
  }

  public static class AmqpSecurityException extends AmqpException {

    public AmqpSecurityException(String message, Throwable cause) {
      super(message, cause);
    }

    public AmqpSecurityException(Throwable cause) {
      super(cause);
    }
  }

  public static class AmqpEntityNotFoundException extends AmqpException {

    public AmqpEntityNotFoundException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static class AmqpResourceInvalidStateException extends AmqpException {

    public AmqpResourceInvalidStateException(String format, Object... args) {
      super(format, args);
    }

    public AmqpResourceInvalidStateException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static class AmqpResourceClosedException extends AmqpResourceInvalidStateException {

    public AmqpResourceClosedException(String message) {
      super(message);
    }

    public AmqpResourceClosedException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
