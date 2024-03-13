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

import com.rabbitmq.model.Message;
import com.rabbitmq.model.ModelException;
import com.rabbitmq.model.Publisher;
import java.util.concurrent.*;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

class AmqpPublisher implements Publisher {

  private final Sender sender;
  private final ExecutorService executorService;

  AmqpPublisher(AmqpConnection connection, String address) {
    this.executorService = connection.executorService();
    try {
      this.sender =
          connection
              .nativeConnection()
              .openSender(address, new SenderOptions().deliveryMode(DeliveryMode.AT_LEAST_ONCE));
    } catch (ClientException e) {
      throw new ModelException(e);
    }
  }

  @Override
  public Message message() {
    return new AmqpMessage();
  }

  @Override
  public void publish(Message message, Callback callback) {
    try {
      // TODO catch ClientSendTimedOutException
      org.apache.qpid.protonj2.client.Message<?> nativeMessage =
          ((AmqpMessage) message).nativeMessage();
      Tracker tracker = this.sender.send(nativeMessage.durable(true));
      this.executorService.submit(
          () -> {
            Status status;
            try {
              tracker.settlementFuture().get();
              status =
                  tracker.remoteState() == DeliveryState.accepted()
                      ? Status.ACCEPTED
                      : Status.FAILED;
            } catch (InterruptedException | ExecutionException e) {
              status = Status.FAILED;
            }
            callback.handle(new DefaultContext(message, status));
          });
    } catch (ClientException e) {
      throw new ModelException(e);
    }
  }

  @Override
  public void close() {
    this.sender.close();
  }

  private static class DefaultContext implements Context {

    private final Message message;
    private final Status status;

    private DefaultContext(Message message, Status status) {
      this.message = message;
      this.status = status;
    }

    @Override
    public Message message() {
      return this.message;
    }

    @Override
    public Status status() {
      return this.status;
    }
  }
}