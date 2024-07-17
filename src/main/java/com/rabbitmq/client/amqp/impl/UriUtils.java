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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

abstract class UriUtils {

  // based on Apache HttpComponents PercentCodec

  private UriUtils() {}

  static final BitSet SUB_DELIMS = new BitSet(256);
  static final BitSet UNRESERVED = new BitSet(256);
  static final BitSet PCHAR = new BitSet(256);
  static final BitSet QUERY_PARAM = new BitSet(256);
  private static final int RADIX = 16;

  static {
    SUB_DELIMS.set('!');
    SUB_DELIMS.set('$');
    SUB_DELIMS.set('&');
    SUB_DELIMS.set('\'');
    SUB_DELIMS.set('(');
    SUB_DELIMS.set(')');
    SUB_DELIMS.set('*');
    SUB_DELIMS.set('+');
    SUB_DELIMS.set(',');
    SUB_DELIMS.set(';');
    SUB_DELIMS.set('=');

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
    PCHAR.or(UNRESERVED);
    PCHAR.or(SUB_DELIMS);
    PCHAR.set(':');
    PCHAR.set('@');

    QUERY_PARAM.or(PCHAR);
    QUERY_PARAM.set('/');
    QUERY_PARAM.set('?');
    QUERY_PARAM.clear('=');
    QUERY_PARAM.clear('&');
  }

  static String encodeNonUnreserved(String value) {
    return encode(value, UNRESERVED);
  }

  static String encodePathSegment(String segment) {
    return encode(segment, PCHAR);
  }

  static String encodeParameter(String value) {
    return encode(value, QUERY_PARAM);
  }

  private static String encode(String value, BitSet safeCharacters) {
    if (value == null) {
      return null;
    }
    StringBuilder buf = new StringBuilder();
    final CharBuffer cb = CharBuffer.wrap(value);
    final ByteBuffer bb = StandardCharsets.UTF_8.encode(cb);
    while (bb.hasRemaining()) {
      final int b = bb.get() & 0xff;
      if (safeCharacters.get(b)) {
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
}
