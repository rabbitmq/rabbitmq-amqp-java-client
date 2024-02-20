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

import com.rabbitmq.client.Channel;
import com.rabbitmq.model.Management;
import com.rabbitmq.model.ModelException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

class AmqpManagement implements Management {

  private final Channel channel;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  AmqpManagement(AmqpEnvironment environment) {
    try {
      this.channel = environment.amqplConnection().createChannel();
    } catch (IOException e) {
      throw new ModelException(e);
    }
  }

  @Override
  public QueueSpecification queue() {
    return new AmqpQueueSpecification(this);
  }

  @Override
  public QueueDeletion queueDeletion() {
    return name -> {
      try {
        channel().queueDelete(name);
      } catch (IOException e) {
        throw new ModelException(e);
      }
    };
  }

  @Override
  public ExchangeSpecification exchange() {
    return new AmqpExchangeSpecification(this);
  }

  @Override
  public ExchangeDeletion exchangeDeletion() {
    return name -> {
      try {
        channel().exchangeDelete(name);
      } catch (IOException e) {
        throw new ModelException(e);
      }
    };
  }

  @Override
  public BindingSpecification binding() {
    return new AmqpBindingManagement.AmqpBindingSpecification(this);
  }

  @Override
  public UnbindSpecification unbind() {
    return new AmqpBindingManagement.AmqpUnbindSpecification(this);
  }

  Channel channel() {
    return this.channel;
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      try {
        this.channel.close();
      } catch (IOException | TimeoutException e) {
        throw new ModelException(e);
      }
    }
  }

  boolean isOpen() {
    return !this.closed.get();
  }
}
