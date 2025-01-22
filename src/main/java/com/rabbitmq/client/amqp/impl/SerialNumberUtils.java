// Copyright (c) 2025 Broadcom. All Rights Reserved.
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

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

final class SerialNumberUtils {

  // https://www.ietf.org/rfc/rfc1982.txt
  // SERIAL_BITS = 32
  // 2 ^ SERIAL_BITS
  static final long SERIAL_SPACE = 0x100000000L;
  // 2 ^ (SERIAL_BITS - 1) - 1
  private static final long SERIAL_MAX_ADDEND = 0x7fffffffL;
  // 2 ^ (SERIAL_BITS - 1)
  private static final long COMPARE = 2_147_483_648L;

  private static final Comparator<Long> COMPARATOR = new SerialNumberComparator();

  private SerialNumberUtils() {}

  static long inc(long s) {
    return (s + 1) % SERIAL_SPACE;
  }

  static <T> List<T> sort(List<T> list, Function<T, Long> serialNumberExtractor) {
    list.sort(Comparator.comparing(serialNumberExtractor, COMPARATOR));
    return list;
  }

  /**
   * Compute contiguous ranges of serial numbers.
   *
   * <p>The list is sorted but the method assumes it contains no duplicates.
   *
   * @param list
   * @param serialNumberExtractor
   * @return
   * @param <T>
   */
  static <T> long[][] ranges(List<T> list, Function<T, Long> serialNumberExtractor) {
    if (list.isEmpty()) {
      return new long[0][0];
    }
    sort(list, serialNumberExtractor);
    long s1 = serialNumberExtractor.apply(list.get(0));
    long[] range = new long[] {s1, s1};
    List<long[]> ranges = new ArrayList<>();
    ranges.add(range);
    for (int i = 1; i < list.size(); i++) {
      long v = serialNumberExtractor.apply(list.get(i));
      if (v == inc(range[1])) {
        range[1] = v;
      } else {
        range = new long[] {v, v};
        ranges.add(range);
      }
    }
    return ranges.toArray(new long[][] {});
  }

  static int compare(long s1, long s2) {
    if (s1 == s2) {
      return 0;
    } else if (((s1 < s2) && (s2 - s1) < COMPARE) || ((s1 > s2) && (s1 - s2) > COMPARE)) {
      return -1;
    } else if (((s1 < s2) && (s2 - s1) > COMPARE) || ((s1 > s2) && (s1 - s2) < COMPARE)) {
      return 1;
    }
    throw new IllegalArgumentException("Cannot compare serial numbers " + s1 + " and " + s2);
  }

  private static class SerialNumberComparator implements Comparator<Long>, Serializable {

    private static final long serialVersionUID = -584535356973875111L;

    @Override
    public int compare(Long o1, Long o2) {
      return SerialNumberUtils.compare(o1, o2);
    }
  }
}
