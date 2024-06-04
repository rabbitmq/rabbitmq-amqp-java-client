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
package com.rabbitmq.amqp1;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public interface RpcClientBuilder {

  RpcClientAddressBuilder requestAddress();

  RpcClientBuilder replyToQueue(String replyToQueue);

  RpcClientBuilder correlationIdSupplier(Supplier<Object> correlationIdSupplier);

  RpcClientBuilder requestPostProcessor(BiFunction<Message, Object, Message> requestPostProcessor);

  RpcClientBuilder correlationIdExtractor(Function<Message, Object> correlationIdExtractor);

  RpcClientBuilder requestTimeout(Duration timeout);

  RpcClient build();

  interface RpcClientAddressBuilder extends AddressBuilder<RpcClientAddressBuilder> {

    RpcClientBuilder rpcClient();
  }
}
