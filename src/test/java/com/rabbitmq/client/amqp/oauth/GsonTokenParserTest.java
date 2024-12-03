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
package com.rabbitmq.client.amqp.oauth;

import static com.rabbitmq.client.amqp.oauth.OAuthTestUtils.sampleJsonToken;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

public class GsonTokenParserTest {

  TokenParser parser = new GsonTokenParser();

  @Test
  void parse() {
    String accessToken = UUID.randomUUID().toString();
    Duration expireIn = ofSeconds(60);
    String jsonToken = sampleJsonToken(accessToken, expireIn);
    Token token = parser.parse(jsonToken);
    assertThat(token.value()).isEqualTo(accessToken);
    assertThat(token.expirationTime())
        .isCloseTo(System.currentTimeMillis() + expireIn.toMillis(), Offset.offset(1000L));
  }
}
