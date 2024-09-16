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

import java.util.function.BiFunction;
import java.util.function.Function;

/** API to configure and create a {@link RpcServer}. */
public interface RpcServerBuilder {

  /**
   * The queue to wait for requests on.
   *
   * @param requestQueue request queue
   * @return this builder instance
   */
  RpcServerBuilder requestQueue(String requestQueue);

  /**
   * The logic to process requests and issue replies.
   *
   * @param handler handler
   * @return this builder instance
   */
  RpcServerBuilder handler(RpcServer.Handler handler);

  /**
   * Logic to extract the correlation ID from a request message.
   *
   * <p>The default implementation uses the message ID.
   *
   * @param correlationIdExtractor logic to extract the correlation ID
   * @return this builder instance
   */
  RpcServerBuilder correlationIdExtractor(Function<Message, Object> correlationIdExtractor);

  /**
   * A callback called after request processing but before sending the reply message.
   *
   * <p>The callback accepts the request message and the correlation ID as parameters. It must
   * return the message that will be sent as the reply, after having potentially modified it.
   *
   * <p>The default implementation set the correlation ID field and returns the updated message.
   *
   * @param replyPostProcessor logic to post-process reply message
   * @return this builder instance
   */
  RpcServerBuilder replyPostProcessor(BiFunction<Message, Object, Message> replyPostProcessor);

  /**
   * Create the configured instance.
   *
   * @return the configured instance
   */
  RpcServer build();
}
