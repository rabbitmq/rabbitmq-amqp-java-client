package com.rabbitmq.client.amqp.docs;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Duration;

import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.ByteCapacity;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;

public class WebsiteDocumentation {

  void environment() {
    Environment environment = new AmqpEnvironmentBuilder()
        .build();
    // ...
    // close the environment when the application stops
    environment.close();
  }

  void connection() {
    Environment environment = null;

    // open a connection from the environment
    Connection connection = environment.connectionBuilder()
        .uri("amqp://admin:admin@localhost:5672/%2f")
        .build();
    // ...
    // close the connection when it is no longer necessary
    connection.close();
  }

  void publishing() {
    Connection connection = null;

    Publisher publisher = connection.publisherBuilder()
        .exchange("foo").key("bar")
        .build();
    // ...
    // close the publisher when it is no longer necessary
    publisher.close();

    // create the message
    Message message = publisher
        .message("hello".getBytes(UTF_8))
        .messageId(1L);

    // publish the message and deal with broker feedback
    publisher.publish(message, context -> {
      // the broker confirmation
      if (context.status() == Publisher.Status.ACCEPTED) {
        // the broker accepted (confirmed) the message
      } else {
        // deal with possible failure
      }
    });
  }

  void publisherAddressFormat() {
    Connection connection = null;

    // publish to an exchange with a routing key
    Publisher publisher1 = connection.publisherBuilder()
        .exchange("foo").key("bar") // /exchanges/foo/bar
        .build();

    // publish to an exchange without a routing key
    Publisher publisher2 = connection.publisherBuilder()
        .exchange("foo") // /exchanges/foo
        .build();

    // publish to a queue
    Publisher publisher3 = connection.publisherBuilder()
        .queue("some-queue") // /queues/some-queue
        .build();
  }

  void publishAddressFormatInMessages() {
    Connection connection = null;

    // no target defined on publisher creation
    Publisher publisher = connection.publisherBuilder()
        .build();

    // publish to an exchange with a routing key
    Message message1 = publisher.message()
        .toAddress().exchange("foo").key("bar")
        .message();

    // publish to an exchange without a routing key
    Message message2 = publisher.message()
        .toAddress().exchange("foo")
        .message();

    // publish to a queue
    Message message3 = publisher.message()
        .toAddress().queue("my-queue")
        .message();
  }

  void publishStreamFiltering() {
    Publisher publisher = null;
    byte[] body = null;
    Message message = publisher.message(body)
        .annotation("x-stream-filter-value", "invoices");
    publisher.publish(message, context -> {
      // confirm callback
    });
  }

  void consuming() {
    Connection connection = null;
    Consumer consumer = connection.consumerBuilder()
        .queue("some-queue")
        .messageHandler((context, message) -> {
          byte[] body = message.body();
          // ...
          context.accept(); // settle the message
        })
        .build(); // do not forget to build the instance!

    // pause the delivery of messages
    consumer.pause();
    // ensure the number of unsettled messages reaches 0
    long unsettledMessageCount = consumer.unsettledMessageCount();
    // close the consumer
    consumer.close();

  }

  void consumingSupportForStreams() {
    Connection connection = null;

    Consumer consumer = connection.consumerBuilder()
        .queue("some-stream")
        .stream()
            .offset(ConsumerBuilder.StreamOffsetSpecification.FIRST)
        .builder()
        .messageHandler((context, message) -> {
          // message processing
        })
        .build();
  }

  void consumingStreamFiltering() {
    Connection connection = null;

    Consumer consumer = connection.consumerBuilder()
        .queue("some-stream")
        .stream()
        .filterValues("invoices", "orders")
        .filterMatchUnfiltered(true)
        .builder()
        .messageHandler((ctx, msg) -> {
          String filterValue = (String) msg.annotation("x-stream-filter-value");
          // there must be some client-side filter logic
          if ("invoices".equals(filterValue) || "orders".equals(filterValue)) {
            // message processing
          }
          ctx.accept();
        })
        .build();

  }

  void consumingAmqpFilterExpressions() {
    Connection connection = null;

    Consumer consumer = connection.consumerBuilder()
        .queue("some-stream")
        .stream()
        .filter()
          .subject("$p:foo") // subject starts with 'foo'
          .property("k1", "v1") // 'k1' application property equals to 'v1'
        .stream()
        .builder()
        .messageHandler((context, message) -> {
          // message processing
        })
        .build();

  }

  void management() {
    Connection connection = null;

    Management management = connection.management();
    // ...
    // close the management instance when it is no longer needed
    management.close();
  }

  void managementExchange() {
    Management management = null;

    management.exchange()
        .name("my-exchange")
        .type(Management.ExchangeType.FANOUT) // enum for built-in type
        .declare();

    management.exchange()
        .name("my-exchange")
        .type("x-delayed-message") // non-built-in type
        .autoDelete(false)
        .argument("x-delayed-type", "direct")
        .declare();

    management.exchangeDelete("my-exchange");
  }

  void managementQueues() {
    Management management = null;

    management.queue()
        .name("my-queue")
        .exclusive(true)
        .autoDelete(false)
        .declare();

    management
        .queue()
        .name("my-queue")
        .type(Management.QueueType.CLASSIC)
        .messageTtl(Duration.ofMinutes(10))
        .maxLengthBytes(ByteCapacity.MB(100))
        .declare();

    management
        .queue()
        .name("my-quorum-queue")
        .quorum() // set queue type to 'quorum'
          .initialMemberCount(3) // specific to quorum queues
          .deliveryLimit(3) // specific to quorum queues
        .queue()
        .declare();

    Management.QueueInfo info = management.queueInfo("my-queue");
    long messageCount = info.messageCount();
    int consumerCount = info.consumerCount();
    String leaderNode = info.leader();

    management.queueDelete("my-queue");
  }

  void binding() {
    Management management = null;

    management.binding()
        .sourceExchange("my-exchange")
        .destinationQueue("my-queue")
        .key("foo")
        .bind();

    management.binding()
        .sourceExchange("my-exchange")
        .destinationExchange("my-other-exchange")
        .key("foo")
        .bind();

    management.unbind()
        .sourceExchange("my-exchange")
        .destinationQueue("my-queue")
        .key("foo")
        .unbind();
  }

  void resourceListeners() {
    Environment environment = null;

    Connection connection = environment.connectionBuilder()
        .listeners(context -> { // set the listener
          context.previousState(); // the previous state
          context.currentState(); // the current (new) state
          context.failureCause(); // the cause of the failure (in case of failure)
          context.resource(); // the connection
        }).build();

    Publisher publisher = connection.publisherBuilder()
        .listeners(context -> {
          // ...
        })
        .exchange("foo").key("bar")
        .build();

    Consumer consumer = connection.consumerBuilder()
        .listeners(context -> {
          // ...
        })
        .queue("my-queue")
        .build();

  }

  void recovery() {
    Environment environment = null;
    Connection connection = environment.connectionBuilder()
        .recovery()
        .backOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(2)))
        .connectionBuilder().build();


  }

  void deactivateTopologyRecovery() {
    Environment environment = null;
    Connection connection = environment.connectionBuilder()
        .recovery()
        .topology(false)
        .connectionBuilder()
        .listeners(context -> {

        })
        .build();
  }

  void deactivateRecovery() {
    Environment environment = null;
    Connection connection = environment.connectionBuilder()
        .recovery()
        .activated(false)
        .connectionBuilder().build();
  }

  void propertyFilterExpressions() {
    Connection connection = null;
    Consumer consumer = connection.consumerBuilder()
        .stream().filter()
            .userId("John".getBytes(UTF_8))
            .subject("&p:Order")
            .property("region", "emea")
        .stream().builder()
        .queue("my-queue")
        .messageHandler((ctx, msg ) -> {
          // message processing
        })
        .build();
  }

  void sqlFilterExpressions() {
    Connection connection = null;
    Consumer consumer = connection.consumerBuilder()
        .stream().filter()
            .sql("properties.user_id = 'John' AND " +
                 "properties.subject LIKE 'Order%' AND " +
                 "region = 'emea'")
        .stream().builder()
        .queue("my-queue")
        .messageHandler((ctx, msg ) -> {
          // message processing
        })
        .build();
  }

  void combinedFilterExpressions() {
    Connection connection = null;
    Consumer consumer = connection.consumerBuilder()
        .stream()
            .filterValues("order.created")
            .filter()
                .sql("p.subject = 'order.created' AND " +
                     "p.creation_time > UTC() - 3600000 AND " +
                     "region IN ('AMER', 'EMEA', 'APJ') AND " +
                     "(h.priority > 4 OR price >= 99.99 OR premium_customer = TRUE)")
        .stream().builder()
        .queue("my-queue")
        .messageHandler((ctx, msg ) -> {
            // message processing
        })
        .build();
  }
}
