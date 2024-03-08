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
package com.rabbitmq.model.amqp;

import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

abstract class UriUtils {

  private UriUtils() {}

  static final BitSet UNRESERVED = new BitSet(256);
  private static final int RADIX = 16;

  static {
    for (int i = 'a'; i <= 'z'; i++) {
      UNRESERVED.set(i);
    }
    for (int i = 'A'; i <= 'Z'; i++) {
      UNRESERVED.set(i);
    }
    // numeric characters
    for (int i = '0'; i <= '9'; i++) {
      UNRESERVED.set(i);
    }
    UNRESERVED.set('-');
    UNRESERVED.set('.');
    UNRESERVED.set('_');
    UNRESERVED.set('~');
  }

  // from Apache HttpComponents PercentCodec
  static String encodePathSegment(String segment) {
    if (segment == null) {
      return null;
    }
    StringBuilder buf = new StringBuilder();
    final CharBuffer cb = CharBuffer.wrap(segment);
    final ByteBuffer bb = StandardCharsets.UTF_8.encode(cb);
    while (bb.hasRemaining()) {
      final int b = bb.get() & 0xff;
      if (UNRESERVED.get(b)) {
        buf.append((char) b);
      } else {
        buf.append("%");
        final char hex1 = Character.toUpperCase(Character.forDigit((b >> 4) & 0xF, RADIX));
        final char hex2 = Character.toUpperCase(Character.forDigit(b & 0xF, RADIX));
        buf.append(hex1);
        buf.append(hex2);
      }
    }
    return buf.toString();
  }

  static String encodeHttpParameter(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
