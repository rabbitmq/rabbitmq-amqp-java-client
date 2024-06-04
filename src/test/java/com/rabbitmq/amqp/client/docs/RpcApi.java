package com.rabbitmq.amqp.client.docs;

import com.rabbitmq.amqp.client.RpcClient;
import com.rabbitmq.amqp.client.Connection;
import com.rabbitmq.amqp.client.Message;
import com.rabbitmq.amqp.client.RpcServer;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RpcApi {

  void rpcWithDefaults() throws Exception {
    Connection connection = null;
    // tag::rpc-server-creation[]
    RpcServer rpcServer = connection.rpcServerBuilder() // <1>
        .requestQueue("rpc-server") // <2>
        .handler((ctx, req) -> { // <3>
          String in = new String(req.body(), UTF_8);
          String out = "*** " + in + " ***";
          return ctx.message(out.getBytes(UTF_8)); // <4>
        }).build();
    // end::rpc-server-creation[]

    // tag::rpc-client-creation[]
    RpcClient rpcClient = connection.rpcClientBuilder() // <1>
        .requestAddress().queue("rpc-server") // <2>
        .rpcClient()
        .build();
    // end::rpc-client-creation[]

    // tag::rpc-client-request[]
    Message request = rpcClient.message("hello".getBytes(UTF_8)); // <1>
    CompletableFuture<Message> replyFuture = rpcClient.publish(request); // <2>
    Message reply = replyFuture.get(10, TimeUnit.SECONDS); // <3>
    // end::rpc-client-request[]
  }

  void rpcWithCustomSettings() throws Exception {
    Connection connection = null;
    // tag::rpc-custom-client-creation[]
    String replyToQueue = connection.management().queue()
        .autoDelete(true).exclusive(true)
        .declare().name(); // <1>
    RpcClient rpcClient = connection.rpcClientBuilder()
        .correlationIdSupplier(UUID::randomUUID) // <2>
        .requestPostProcessor((msg, corrId) ->
            msg.correlationId(corrId) // <3>
               .replyToAddress().queue(replyToQueue).message()) // <4>
        .replyToQueue(replyToQueue)
        .requestAddress().queue("rpc-server") // <5>
        .rpcClient()
        .build();
    // end::rpc-custom-client-creation[]

    // tag::rpc-custom-server-creation[]
    RpcServer rpcServer = connection.rpcServerBuilder()
        .correlationIdExtractor(Message::correlationId) // <1>
        .requestQueue("rpc-server")
        .handler((ctx, req) -> {
          String in = new String(req.body(), UTF_8);
          String out = "*** " + in + " ***";
          return ctx.message(out.getBytes(UTF_8));
        }).build();
    // end::rpc-custom-server-creation[]
  }
}
