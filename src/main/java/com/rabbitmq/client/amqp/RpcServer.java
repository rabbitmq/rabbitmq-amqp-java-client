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

/**
 * Client server class for RPC.
 *
 * @see RpcServerBuilder
 */
public interface RpcServer extends AutoCloseable {

  /** Contract to process a request message and return a reply message. */
  @FunctionalInterface
  interface Handler {

    /**
     * Process request message.
     *
     * @param ctx context
     * @param request request message
     * @return the reply message
     */
    Message handle(Context ctx, Message request);
  }

  /** Request processing context. */
  interface Context {

    /**
     * Tell whether the requester is still able to receive the response.
     *
     * <p>The call assumes a reply-to queue address has been set on the request message and checks
     * whether this queue still exists or not.
     *
     * <p>A time-consuming request handler can use this call from time to time to make sure it still
     * worth keeping processing the request.
     *
     * @param request the incoming request
     * @return true if the requester is still considered alive, false otherwise
     */
    boolean isRequesterAlive(Message request);

    /**
     * Create a message meant to be published by the underlying publisher instance.
     *
     * <p>Once returned in the {@link Handler#handle(Context, Message)} the message instance should
     * be not be modified or even reused.
     *
     * @return a message
     */
    Message message();

    /**
     * Create a message meant to be published by the underlying publisher instance.
     *
     * <p>Once returned in the {@link Handler#handle(Context, Message)} the message instance should
     * be not be modified or even reused.
     *
     * @param body message body
     * @return a message with the provided body
     */
    Message message(byte[] body);
  }

  /**
   * Pause the server to stop receiving messages.
   *
   * <p>Outstanding requests will still be processed.
   */
  void pause();

  /** Request to receive messages again. */
  void unpause();

  /** Close the RPC server and its resources. */
  @Override
  void close();
}
