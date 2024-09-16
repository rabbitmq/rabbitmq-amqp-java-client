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

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/** API to configure and create a {@link RpcClient}. */
public interface RpcClientBuilder {

  /**
   * Builder for the request address.
   *
   * @return the request address builder
   */
  RpcClientAddressBuilder requestAddress();

  /**
   * The queue the client expects responses on.
   *
   * <p>The queue <b>must</b> exist if it is set.
   *
   * <p>The RPC client will create an exclusive, auto-delete queue if it is not set.
   *
   * @param replyToQueue reply queue
   * @return this builder instance
   */
  RpcClientBuilder replyToQueue(String replyToQueue);

  /**
   * The generator for correlation ID.
   *
   * <p>The default generator uses a fixed random UUID prefix and a strictly monotonic increasing
   * sequence suffix.
   *
   * @param correlationIdSupplier correlation ID generator
   * @return the this builder instance
   */
  RpcClientBuilder correlationIdSupplier(Supplier<Object> correlationIdSupplier);

  /**
   * A callback before sending a request message.
   *
   * <p>The callback accepts the request message and the correlation ID as parameters. It must
   * return the message that will be sent as request, after having potentially modified it.
   *
   * <p>The default post-processor sets the reply-to field and assigns the correlation ID to the
   * message ID field.
   *
   * @param requestPostProcessor logic to post-process request message
   * @return this builder instance
   */
  RpcClientBuilder requestPostProcessor(BiFunction<Message, Object, Message> requestPostProcessor);

  /**
   * Callback to extract the correlation ID from a reply message.
   *
   * <p>The correlation ID is then used to correlate the reply message to an outstanding request
   * message.
   *
   * <p>The default implementation uses the correlation ID field.
   *
   * @param correlationIdExtractor correlation ID extractor
   * @return this builder instance
   */
  RpcClientBuilder correlationIdExtractor(Function<Message, Object> correlationIdExtractor);

  /**
   * Timeout before failing outstanding requests.
   *
   * @param timeout timeout
   * @return the builder instance
   */
  RpcClientBuilder requestTimeout(Duration timeout);

  /**
   * Build the configured instance.
   *
   * @return the configured instance
   */
  RpcClient build();

  /** Builder for the request address. */
  interface RpcClientAddressBuilder extends AddressBuilder<RpcClientAddressBuilder> {

    /**
     * Go back to the RPC client builder.
     *
     * @return the RPC client builder
     */
    RpcClientBuilder rpcClient();
  }
}
