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
package com.rabbitmq.amqp1.observation.micrometer;

abstract class Utils {

  private Utils() {}

  private static final String EXCHANGE_DELIMITER = "/exchange/";
  private static final String KEY_DELIMITER = "/key/";
  private static final String QUEUE_DELIMITER = "/queue/";
  private static final int EXCHANGE_DELIMITER_LENGTH = EXCHANGE_DELIMITER.length();
  private static final int KEY_DELIMITER_LENGTH = KEY_DELIMITER.length();
  private static final int QUEUE_DELIMITER_LENGTH = QUEUE_DELIMITER.length();
  private static final String EMPTY_STRING = "";

  static String[] exchangeRoutingKeyFromTo(String to) {
    String[] exRk = new String[] {null, null};
    if (to != null) {
      if (to.startsWith(QUEUE_DELIMITER)) {
        exRk[0] = EMPTY_STRING;
        exRk[1] = to.substring(QUEUE_DELIMITER_LENGTH);
      } else if (to.startsWith(EXCHANGE_DELIMITER)) {
        int keyDelimiterIndex = to.indexOf(KEY_DELIMITER);
        exRk[0] =
            to.substring(
                EXCHANGE_DELIMITER_LENGTH,
                keyDelimiterIndex == -1 ? to.length() : keyDelimiterIndex);
        exRk[1] =
            keyDelimiterIndex == -1 ? null : to.substring(keyDelimiterIndex + KEY_DELIMITER_LENGTH);
      }
    }
    return exRk;
  }
}
