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
package com.rabbitmq.client.amqp.oauth2;

import java.time.Duration;

public final class OAuth2TestUtils {

  private OAuth2TestUtils() {}

  public static String sampleJsonToken(String accessToken, Duration expiresIn) {
    String json =
        "{\n"
            + "  \"access_token\" : \"{accessToken}\",\n"
            + "  \"token_type\" : \"bearer\",\n"
            + "  \"expires_in\" : {expiresIn},\n"
            + "  \"scope\" : \"clients.read emails.write scim.userids password.write idps.write notifications.write oauth.login scim.write critical_notifications.write\",\n"
            + "  \"jti\" : \"18c1b1dfdda04382a8bcc14d077b71dd\"\n"
            + "}";
    return json.replace("{accessToken}", accessToken)
        .replace("{expiresIn}", expiresIn.toSeconds() + "");
  }
}
