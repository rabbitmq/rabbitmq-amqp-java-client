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
package com.rabbitmq.model.docs;

import com.rabbitmq.model.*;
import com.rabbitmq.model.amqp.AmqpEnvironmentBuilder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static com.rabbitmq.model.Management.ExchangeType.FANOUT;
import static com.rabbitmq.model.Publisher.Status.ACCEPTED;

class Api {

  void environment() {
    // tag::environment-creation[]
    Environment environment = new AmqpEnvironmentBuilder()
        .build();
    // end::environment-creation[]
  }

  void connection() {
    Environment environment = null;
    // tag::connection-creation[]
    Connection connection = environment.connectionBuilder()
        .build();
    // end::connection-creation[]
  }

  void connectionSettings() {
    // tag::connection-settings[]
    Environment environment = new AmqpEnvironmentBuilder()
        .connectionSettings()
        .uri("amqp://guest:guest@localhost:5672/%2f") // <1>
        .environmentBuilder().build();

    Connection connection = environment.connectionBuilder()
        .uri("amqp://admin:admin@localhost:5672/%2f") // <2>
        .build();
    // end::connection-settings[]
  }


  void publishing() {
    Connection connection = null;
    // tag::publisher-creation[]
    Publisher publisher = connection.publisherBuilder()
        .exchange("foo").key("bar")
        .build();
    // end::publisher-creation[]


    // tag::message-creation[]
    Message message = publisher
        .message("hello".getBytes(StandardCharsets.UTF_8))
        .messageId(1L);
    // end::message-creation[]

    // tag::message-publishing[]
    publisher.publish(message, context -> {
      if (context.status() == ACCEPTED) {
        // the broker accepted (confirmed) the message
      } else {
        // deal with possible failure
      }
    });
    // end::message-publishing[]
  }

  void targetAddressFormatExchangeKey() {
    Connection connection = null;
    // tag::target-address-exchange-key[]
    Publisher publisher = connection.publisherBuilder()
        .exchange("foo").key("bar") // <1>
        .build();
    // end::target-address-exchange-key[]
  }

  void targetAddressFormatExchange() {
    Connection connection = null;
    // tag::target-address-exchange[]
    Publisher publisher = connection.publisherBuilder()
        .exchange("foo") // <1>
        .build();
    // end::target-address-exchange[]
  }

  void targetAddressFormatQueue() {
    Connection connection = null;
    // tag::target-address-queue[]
    Publisher publisher = connection.publisherBuilder()
        .queue("some-queue") // <1>
        .build();
    // end::target-address-queue[]
  }

  void targetAddressNull() {
    Connection connection = null;
    // tag::target-address-null[]
    Publisher publisher = connection.publisherBuilder()
        .build(); // <1>

    Message message1 = publisher.message()
        .address().exchange("foo").key("bar") // <2>
        .message();

    Message message2 = publisher.message()
        .address().exchange("foo") // <3>
        .message();

    Message message3 = publisher.message()
        .address().queue("my-queue") // <4>
        .message();
    // end::target-address-null[]
  }

  void consuming() {
    Connection connection = null;
    // tag::consumer-consume[]
    connection.consumerBuilder()
        .queue("some-queue")
        .messageHandler((context, message) -> {
          byte[] body = message.body(); // <1>
          // ... <2>
          context.accept(); // <3>
        })
        .build();
    // end::consumer-consume[]
  }

  void management() {
    Connection connection = null;
    // tag::management[]
    Management management = connection.management();
    // end::management[]
  }

  void exchanges() {
    Management management = null;
    // tag::fanout-exchange[]
    management.exchange()
        .name("my-exchange")
        .type(FANOUT)
        .declare();
    // end::fanout-exchange[]

    // tag::delayed-message-exchange[]
    management.exchange()
        .name("my-exchange")
        .type("x-delayed-message")
        .autoDelete(false)
        .argument("x-delayed-type", "direct")
        .declare();
    // end::delayed-message-exchange[]

    // tag::delete-exchange[]
    management.exchangeDeletion().delete("my-exchange");
    // end::delete-exchange[]
  }

  void queues() {
    Management management = null;
    // tag::queue-creation[]
    management.queue()
        .name("my-queue")
        .exclusive(true)
        .autoDelete(false)
        .declare();
    // end::queue-creation[]

    // tag::queue-creation-with-arguments[]
    management
        .queue()
        .name("my-queue")
        .messageTtl(Duration.ofMinutes(10)) // <1>
        .maxLengthBytes(ByteCapacity.MB(100)) // <1>
        .declare();
    // end::queue-creation-with-arguments[]

    // tag::quorum-queue-creation[]
    management
        .queue()
        .name("my-quorum-queue")
        .quorum() // <1>
          .quorumInitialGroupSize(3)
          .deliveryLimit(3)
        .queue()
        .declare();
    // end::quorum-queue-creation[]

    // tag::queue-deletion[]
    management.queueDeletion().delete("my-queue");
    // end::queue-deletion[]
  }

  void bindings() {
    Management management = null;
    // tag::binding[]
    management.binding()
        .sourceExchange("my-exchange")
        .destinationQueue("my-queue")
        .key("foo")
        .bind();
    // end::binding[]

    // tag::exchange-binding[]
    management.binding()
        .sourceExchange("my-exchange")
        .destinationExchange("my-other-exchange")
        .key("foo")
        .bind();
    // end::exchange-binding[]

    // tag::unbinding[]
    management.unbind()
        .sourceExchange("my-exchange")
        .destinationQueue("my-queue")
        .key("foo")
        .unbind();
    // end::unbinding[]
  }

  void listeners() {
    Environment environment = null;
    // tag::listener-connection[]
    Connection connection =  environment.connectionBuilder()
        .listeners(context -> { // <1>
      context.previousState(); // <2>
      context.currentState(); // <3>
      context.failureCause(); // <4>
      context.resource(); // <5>
    }).build();
    // end::listener-connection[]

    // tag::listener-publisher[]
    Publisher publisher = connection.publisherBuilder()
        .listeners(context -> { // <1>
          // ...
        })
        .exchange("foo").key("bar")
        .build();
    // end::listener-publisher[]

    // tag::listener-consumer[]
    Consumer consumer = connection.consumerBuilder()
        .listeners(context -> { // <1>
          // ...
        })
        .queue("my-queue")
        .build();
    // end::listener-consumer[]
  }

  void connectionRecoveryBackOff() {
    Environment environment = null;
    // tag::connection-recovery-back-off[]
    Connection connection = environment.connectionBuilder()
        .recovery() // <1>
        .backOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(2))) // <2>
        .connectionBuilder().build();
    // end::connection-recovery-back-off[]
  }

  void connectionRecoveryNoTopologyRecovery() {
    Environment environment = null;
    // tag::connection-recovery-no-topology-recovery[]
    Connection connection = environment.connectionBuilder()
        .recovery()
        .topology(false) // <1>
        .connectionBuilder()
        .listeners(context -> {
          // <2>
        })
        .build();
    // end::connection-recovery-no-topology-recovery[]
  }

  void connectionRecoveryDeactivate() {
    Environment environment = null;
    // tag::connection-recovery-deactivate[]
    Connection connection = environment.connectionBuilder()
        .recovery()
        .activated(false) // <1>
        .connectionBuilder().build();
    // end::connection-recovery-deactivate[]
  }

}
