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
package com.rabbitmq.client.amqp.impl;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.amqp.oauth.Token;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.HmacKey;

final class JwtTestUtils {

  private static final String BASE64_KEY = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH";
  private static final HmacKey KEY = new HmacKey(Base64.getDecoder().decode(BASE64_KEY));
  private static final String AUDIENCE = "rabbitmq";
  private static final Gson GSON = new Gson();
  private static final TypeToken<Map<String, Object>> MAP_TYPE = new TypeToken<>() {};

  private JwtTestUtils() {}

  static String token(long expirationTime) {
    try {
      JwtClaims claims = new JwtClaims();
      claims.setIssuer("unit_test");
      claims.setAudience(AUDIENCE);
      claims.setExpirationTime(NumericDate.fromMilliseconds(expirationTime));
      claims.setStringListClaim(
          "scope", List.of("rabbitmq.configure:*/*", "rabbitmq.write:*/*", "rabbitmq.read:*/*"));
      claims.setStringClaim("random", RandomStringUtils.insecure().nextAscii(6));

      JsonWebSignature signature = new JsonWebSignature();

      signature.setKeyIdHeaderValue("token-key");
      signature.setAlgorithmHeaderValue(AlgorithmIdentifiers.HMAC_SHA256);
      signature.setKey(KEY);
      signature.setPayload(claims.toJson());
      return signature.getCompactSerialization();
    } catch (Exception e) {
      System.out.println("ERROR " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  static Token parseToken(String tokenAsString) {
    long expirationTime;
    try {
      JwtConsumer consumer =
          new JwtConsumerBuilder()
              .setExpectedAudience(AUDIENCE)
              // we do not validate the expiration time
              .setEvaluationTime(NumericDate.fromMilliseconds(0))
              .setVerificationKey(KEY)
              .build();
      JwtClaims claims = consumer.processToClaims(tokenAsString);
      expirationTime = claims.getExpirationTime().getValueInMillis();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new Token() {
      @Override
      public String value() {
        return tokenAsString;
      }

      @Override
      public long expirationTime() {
        return expirationTime;
      }
    };
  }

  static Map<String, Object> parse(String json) {
    return GSON.fromJson(json, MAP_TYPE);
  }
}
