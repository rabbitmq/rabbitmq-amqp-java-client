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

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Publisher;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class OAuth2RealTest {

  @Test
  void test() throws Exception {
    String token =
        "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJHbmwyWmxiUmgzckFyNld5bWM5ODhfNWNZN1Q1R3VlUGQ1ZHBKbFhESlVrIn0.eyJleHAiOjE3MzQ2ODI2MjksImlhdCI6MTczNDY4MjMyOSwianRpIjoiMzg0ZTY2ZTMtOGE4NC00ZmI1LWIyZjAtZDg5NDI4YzcxMjg2IiwiaXNzIjoiaHR0cHM6Ly9sb2NhbGhvc3Q6ODQ0My9yZWFsbXMvdGVzdCIsImF1ZCI6InJhYmJpdG1xIiwic3ViIjoiNTBiMzg0NjQtNDQwYy00OGQyLTg4MGQtMDUwYTdkMjhmN2IwIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoicHJvZHVjZXIiLCJzZXNzaW9uX3N0YXRlIjoiOWNjNGNiZGYtYjY4ZS00NTM3LWJjNmMtMmRiOGI3NGY3MGY2IiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImRlZmF1bHQtcm9sZXMtdGVzdCIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6InJhYmJpdG1xLnJlYWQ6Ki8qIHJhYmJpdG1xLndyaXRlOiovKiByYWJiaXRtcS5jb25maWd1cmU6Ki8qIiwic2lkIjoiOWNjNGNiZGYtYjY4ZS00NTM3LWJjNmMtMmRiOGI3NGY3MGY2IiwiY2xpZW50SWQiOiJwcm9kdWNlciIsImNsaWVudEhvc3QiOiIxNzIuMjIuMC4xIiwiY2xpZW50QWRkcmVzcyI6IjE3Mi4yMi4wLjEifQ.flIe8YhawDB4Xpp1mxbsAZtwXcaX1OSztZX7QUYmQhbDCtABH7ywAZsgnoIs6zYR1alrxvtCFF9FlHeO1hzK4KW07bba5FX2ttFnd-6Z_9uQQM9YhNH80uRfakQDr6goUSaV2vTY0DqFTMtgQIl7Bj4DFwoGzsfGAeidXpK8uFrZMjc3SONk1LdXA005jXmzjPjcdvfMGHM4RG0Rx9zvonou_SsOaEmbg026jdOlVmVsLljxkkZF5VMTQTfbtEFicPUnmAN4GdCee0gAoDu7LsgFZUWn-t6QDgLmdGtMZPo3zQcaWsKXPtuea7_FcPIO9l25zN6jeE72UwBT3_Io4g";
    Environment environment =
        new AmqpEnvironmentBuilder()
            .connectionSettings()
            .oauth2()
            .tokenEndpointUri("https://localhost:8443/realms/test/protocol/openid-connect/token")
            .clientId("producer")
            .clientSecret("kbOFBXI9tANgKUq8vXHLhT6YhbivgXxn")
            .tls()
            .sslContext(TlsTestUtils.alwaysTrustSslContext())
            .oauth2()
            .connection()
            .environmentBuilder()
            .build();

    Connection c = environment.connectionBuilder().build();

    String q = UUID.randomUUID().toString();
    c.management().queue(q).exclusive(true).declare();

    c.consumerBuilder().queue(q).messageHandler((ctx, msg) -> ctx.accept()).build();

    Publisher p = c.publisherBuilder().queue(q).build();
    while (true) {
      Thread.sleep(100);
      p.publish(p.message(), ctx -> {});
    }
  }
}
