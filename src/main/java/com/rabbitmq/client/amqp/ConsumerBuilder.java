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

import java.time.Instant;

public interface ConsumerBuilder {

  ConsumerBuilder queue(String queue);

  ConsumerBuilder messageHandler(Consumer.MessageHandler handler);

  ConsumerBuilder initialCredits(int initialCredits);

  ConsumerBuilder listeners(Resource.StateListener... listeners);

  StreamOptions stream();

  Consumer build();

  interface StreamOptions {

    StreamOptions offset(long offset);

    StreamOptions offset(Instant timestamp);

    StreamOptions offset(StreamOffsetSpecification specification);

    StreamOptions offset(String interval);

    StreamOptions filterValues(String... values);

    StreamOptions filterMatchUnfiltered(boolean matchUnfiltered);

    ConsumerBuilder builder();
  }

  enum StreamOffsetSpecification {
    FIRST,
    LAST,
    NEXT
  }
}
