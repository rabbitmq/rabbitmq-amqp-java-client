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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Map;

public class GsonTokenParser implements TokenParser {

  private static final Gson GSON = new Gson();
  private static final TypeToken<Map<String, Object>> MAP_TYPE = new TypeToken<>() {};

  @Override
  public Token parse(String tokenAsString) {
    Map<String, Object> tokenAsMap = GSON.fromJson(tokenAsString, MAP_TYPE);
    String accessToken = (String) tokenAsMap.get("access_token");
    // in seconds, see https://www.rfc-editor.org/rfc/rfc6749#section-5.1
    long expiresIn = ((Number) tokenAsMap.get("expires_in")).longValue();
    long expirationTime = System.currentTimeMillis() + expiresIn * 1_000;
    return new DefaultTokenInfo(accessToken, expirationTime);
  }

  private static final class DefaultTokenInfo implements Token {

    private final String value;
    private final long expirationTime;

    private DefaultTokenInfo(String value, long expirationTime) {
      this.value = value;
      this.expirationTime = expirationTime;
    }

    @Override
    public String value() {
      return this.value;
    }

    @Override
    public long expirationTime() {
      return this.expirationTime;
    }
  }
}
