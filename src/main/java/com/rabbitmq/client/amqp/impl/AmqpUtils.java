// Copyright (c) 2026 Broadcom. All Rights Reserved.
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
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.metrics.MetricsCollector;
import java.util.function.Predicate;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Rejected;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AmqpUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpUtils.class);

  private AmqpUtils() {}

  static final Predicate<Exception> EXCLUSIVE_ACCESS_EXCEPTION_PREDICATE =
      e ->
          e instanceof AmqpException
              && e.getMessage() != null
              && e.getMessage().contains("cannot obtain exclusive access to locked queue")
              && e.getMessage()
                  .contains(
                      "the exclusive property value does not match that of the original declaration");

  static Publisher.Status mapDeliveryState(DeliveryState in) {
    if (in.isAccepted()) {
      return Publisher.Status.ACCEPTED;
    } else if (in.getType() == DeliveryState.Type.REJECTED) {
      return Publisher.Status.REJECTED;
    } else if (in.getType() == DeliveryState.Type.RELEASED) {
      return Publisher.Status.RELEASED;
    } else {
      LOGGER.warn("Delivery state not supported: " + in.getType());
      throw new IllegalStateException("This delivery state is not supported: " + in.getType());
    }
  }

  static Throwable maybeMapToRejectedException(DeliveryState deliveryState) {
    Throwable result = null;
    if (deliveryState instanceof Rejected) {
      Rejected rejected = (Rejected) deliveryState;
      result =
          new AmqpException.AmqpMessageRejectedException(
              rejected.getDescription() == null
                  ? "Message has been rejected"
                  : rejected.getDescription());
    }
    return result;
  }

  static MetricsCollector.PublishDisposition mapToPublishDisposition(Publisher.Status status) {
    if (status == Publisher.Status.ACCEPTED) {
      return MetricsCollector.PublishDisposition.ACCEPTED;
    } else if (status == Publisher.Status.REJECTED) {
      return MetricsCollector.PublishDisposition.REJECTED;
    } else if (status == Publisher.Status.RELEASED) {
      return MetricsCollector.PublishDisposition.RELEASED;
    } else {
      return null;
    }
  }
}
