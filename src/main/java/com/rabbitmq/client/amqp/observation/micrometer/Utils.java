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
package com.rabbitmq.client.amqp.observation.micrometer;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

abstract class Utils {

  private Utils() {}

  private static final String EXCHANGE_DELIMITER = "/exchanges/";
  private static final String KEY_DELIMITER = "/";
  private static final String QUEUE_DELIMITER = "/queues/";
  private static final int EXCHANGE_DELIMITER_LENGTH = EXCHANGE_DELIMITER.length();
  private static final int KEY_DELIMITER_LENGTH = KEY_DELIMITER.length();
  private static final int QUEUE_DELIMITER_LENGTH = QUEUE_DELIMITER.length();
  private static final String EMPTY_STRING = "";

  static String[] exchangeRoutingKeyFromTo(String to) {
    String[] exRk = new String[] {null, null};
    if (to != null) {
      if (to.startsWith(QUEUE_DELIMITER)) {
        exRk[0] = EMPTY_STRING;
        exRk[1] = percentDecode(to.substring(QUEUE_DELIMITER_LENGTH));
      } else if (to.startsWith(EXCHANGE_DELIMITER)) {
        int keyDelimiterIndex = to.indexOf(KEY_DELIMITER, EXCHANGE_DELIMITER_LENGTH);
        exRk[0] =
            percentDecode(
                to.substring(
                    EXCHANGE_DELIMITER_LENGTH,
                    keyDelimiterIndex == -1 ? to.length() : keyDelimiterIndex));
        exRk[1] =
            percentDecode(
                keyDelimiterIndex == -1
                    ? null
                    : to.substring(keyDelimiterIndex + KEY_DELIMITER_LENGTH));
      }
    }
    return exRk;
  }

  // from Apache HttpComponents PercentCodec
  private static final int RADIX = 16;

  static String percentDecode(final CharSequence content) {
    if (content == null) {
      return null;
    }
    final ByteBuffer bb = ByteBuffer.allocate(content.length());
    final CharBuffer cb = CharBuffer.wrap(content);
    while (cb.hasRemaining()) {
      final char c = cb.get();
      if (c == '%' && cb.remaining() >= 2) {
        final char uc = cb.get();
        final char lc = cb.get();
        final int u = Character.digit(uc, RADIX);
        final int l = Character.digit(lc, RADIX);
        if (u != -1 && l != -1) {
          bb.put((byte) ((u << 4) + l));
        } else {
          bb.put((byte) '%');
          bb.put((byte) uc);
          bb.put((byte) lc);
        }
      } else {
        bb.put((byte) c);
      }
    }
    bb.flip();
    return StandardCharsets.UTF_8.decode(bb).toString();
  }
}
