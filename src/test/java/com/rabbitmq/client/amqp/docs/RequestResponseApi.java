package com.rabbitmq.client.amqp.docs;

import com.rabbitmq.client.amqp.Requester;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.Responder;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RequestResponseApi {

  void withDefaults() throws Exception {
    Connection connection = null;
    // tag::responder-creation[]
    Responder responder = connection.responderBuilder() // <1>
        .requestQueue("request-queue") // <2>
        .handler((ctx, req) -> { // <3>
          String in = new String(req.body(), UTF_8);
          String out = "*** " + in + " ***";
          return ctx.message(out.getBytes(UTF_8)); // <4>
        }).build();
    // end::responder-creation[]

    // tag::requester-creation[]
    Requester requester = connection.requesterBuilder() // <1>
        .requestAddress().queue("request-queue") // <2>
        .requester()
        .build();
    // end::requester-creation[]

    // tag::requester-request[]
    Message request = requester.message("hello".getBytes(UTF_8)); // <1>
    CompletableFuture<Message> replyFuture = requester.publish(request); // <2>
    Message reply = replyFuture.get(10, TimeUnit.SECONDS); // <3>
    // end::requester-request[]
  }

  void withCustomSettings() {
    Connection connection = null;
    // tag::custom-requester-creation[]
    String replyToQueue = connection.management().queue()
        .autoDelete(true).exclusive(true)
        .declare().name(); // <1>
    Requester requester = connection.requesterBuilder()
        .correlationIdSupplier(UUID::randomUUID) // <2>
        .requestPostProcessor((msg, corrId) ->
            msg.correlationId(corrId) // <3>
               .replyToAddress().queue(replyToQueue).message()) // <4>
        .replyToQueue(replyToQueue)
        .requestAddress().queue("request-queue") // <5>
        .requester()
        .build();
    // end::custom-requester-creation[]

    // tag::custom-responder-creation[]
    Responder responder = connection.responderBuilder()
        .correlationIdExtractor(Message::correlationId) // <1>
        .requestQueue("request-queue")
        .handler((ctx, req) -> {
          String in = new String(req.body(), UTF_8);
          String out = "*** " + in + " ***";
          return ctx.message(out.getBytes(UTF_8));
        }).build();
    // end::custom-responder-creation[]
  }
}
