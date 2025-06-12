///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS com.rabbitmq.client:amqp-client:${env.RABBITMQ_LIBRARY_VERSION}
//DEPS org.slf4j:slf4j-simple:1.7.36

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SanityCheck {

  private static final Logger LOGGER = LoggerFactory.getLogger("rabbitmq");

  public static void main(String[] args) {

    try (Environment env = new AmqpEnvironmentBuilder().build()){
      LOGGER.info("connecting");
      Connection connection = env.connectionBuilder().build();
      LOGGER.info("connected");
      String q = connection.management().queue().exclusive(true).declare().name();
      LOGGER.info("test queue created");
      CountDownLatch publishLatch = new CountDownLatch(1);
      Publisher publisher = connection.publisherBuilder().queue(q).build();
      LOGGER.info("publisher created");
      publisher.publish(
          publisher.message("".getBytes(StandardCharsets.UTF_8)),
          ctx -> publishLatch.countDown());

      LOGGER.info("waiting for message disposition");

      boolean done = publishLatch.await(5, TimeUnit.SECONDS);
      if (!done) {
        throw new IllegalStateException("Did not receive message disposition");
      }

      LOGGER.info("got message disposition");

      CountDownLatch consumeLatch = new CountDownLatch(1);
      connection.consumerBuilder().queue(q)
          .messageHandler((context, message) -> {
            context.accept();
            consumeLatch.countDown();
          })
          .build();

      LOGGER.info("created consumer, waiting for message");

      done = consumeLatch.await(5, TimeUnit.SECONDS);
      if (!done) {
        throw new IllegalStateException("Did not receive message");
      }

      LOGGER.info("got message");

      LOGGER.info(
          "Test succeeded with AMQP Client {}",
          Environment.class.getPackage().getImplementationVersion());
      System.exit(0);
    } catch (Exception e) {
      LOGGER.info(
          "Test failed with AMQP Client {}",
          Environment.class.getPackage().getImplementationVersion(),
          e);
      System.exit(1);
    }
  }
}
