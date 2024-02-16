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
package com.rabbitmq.model;

import java.util.concurrent.ExecutionException;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.futures.AsyncResult;
import org.junit.jupiter.api.Test;

public class ClientTest {

  @Test
  void test() throws ClientException, InterruptedException, ExecutionException {
    ClientOptions clientOptions = new ClientOptions();
    Client client = Client.create(clientOptions);

    ConnectionOptions connectionOptions = new ConnectionOptions();
    connectionOptions.user("guest");
    connectionOptions.password("guest");

    Connection connection = client.connect("localhost", 5672, connectionOptions);
    connection.openFuture().get();

    Sender sender =
        connection.openSender(
            "/amq/queue/test", new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE));

    Tracker tracker = sender.send(org.apache.qpid.protonj2.client.Message.create("Hello World"));
    //    ((AsyncResult) tracker.settlementFuture()).complete();
    System.out.println("class " + (tracker.settlementFuture() instanceof AsyncResult));
    System.out.println(tracker.settled());

    Thread.sleep(100);

    System.out.println(tracker.settled());

    connection.close();
  }
}
