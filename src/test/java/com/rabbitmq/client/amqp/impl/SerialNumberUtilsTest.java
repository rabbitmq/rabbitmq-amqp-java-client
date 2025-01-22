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

import static com.rabbitmq.client.amqp.impl.SerialNumberUtils.*;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SerialNumberUtilsTest {

  @Test
  void testInc() {
    assertThat(inc(SERIAL_SPACE - 1)).isZero();
    assertThat(inc(inc(SERIAL_SPACE - 1))).isEqualTo(1);
  }

  @Test
  void testCompare() {
    assertThat(compare(0, 0)).isZero();
    assertThat(compare(SERIAL_SPACE - 1, SERIAL_SPACE - 1)).isZero();
    assertThat(compare(0, 1)).isNegative();
    assertThat(compare(1, 0)).isPositive();
    assertThat(compare(0, 2)).isNegative();
    long maxAddend = (long) (Math.pow(2, 32 - 1) - 1);
    assertThat(compare(0, maxAddend)).isNegative();
    assertThatThrownBy(() -> compare(0, maxAddend + 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(compare(SERIAL_SPACE - 5, 30_000)).isNegative();
    assertThat(compare(1, 0)).isPositive();
    assertThat(compare(maxAddend, 0)).isPositive();
    assertThatThrownBy(() -> compare(maxAddend + 1, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testSort() {
    assertThat(sort(sns(), identity())).isEmpty();
    assertThat(sort(sns(3L), identity())).containsExactly(3L);
    assertThat(sort(sns(3L), identity())).containsExactly(3L);
    assertThat(sort(sns(4294967000L, 4294967293L, 4294967294L, 4294967295L, 0, 3, 4), identity()))
        .containsExactly(4294967000L, 4294967293L, 4294967294L, 4294967295L, 0L, 3L, 4L);
  }

  @Test
  void testRanges() {
    checkRanges(sns(), rgs());
    checkRanges(sns(0), rgs(0, 0));
    checkRanges(sns(0, 1), rgs(0, 1));
    checkRanges(sns(1, 0), rgs(0, 1));
    checkRanges(sns(0, 2), rgs(0, 0, 2, 2));
    checkRanges(sns(2, 0), rgs(0, 0, 2, 2));

    // SPACE - 1 = (2 ^ 32) - 1 = 4294967295
    checkRanges(
        sns(4294967290L, 4294967295L), rgs(4294967290L, 4294967290L, 4294967295L, 4294967295L));
    checkRanges(
        sns(4294967295L, 4294967290L), rgs(4294967290L, 4294967290L, 4294967295L, 4294967295L));
    checkRanges(sns(0, 1, 3, 4, 5, 10, 18, 19), rgs(0, 1, 3, 5, 10, 10, 18, 19));
    checkRanges(sns(1, 10, 0, 3, 4, 5, 19, 18), rgs(0, 1, 3, 5, 10, 10, 18, 19));

    checkRanges(sns(4294967294L, 0), rgs(4294967294L, 4294967294L, 0, 0));
    checkRanges(sns(0, 4294967294L), rgs(4294967294L, 4294967294L, 0, 0));

    checkRanges(sns(4294967295L, 0), rgs(4294967295L, 0));

    checkRanges(
        sns(4294967294L, 4294967295L, 0, 1, 3, 4, 5, 10, 18, 19),
        rgs(4294967294L, 1, 3, 5, 10, 10, 18, 19));
    checkRanges(
        sns(1, 10, 4294967294L, 0, 3, 4, 5, 19, 18, 4294967295L),
        rgs(4294967294L, 1, 3, 5, 10, 10, 18, 19));
  }

  private static void checkRanges(List<Long> serialNumbers, long[][] ranges) {
    assertThat(ranges(serialNumbers, identity())).isDeepEqualTo(ranges);
  }

  private static List<Long> sns(long... sns) {
    List<Long> l = new ArrayList<>();
    for (long sn : sns) {
      l.add(sn);
    }
    return l;
  }

  long[][] rgs(long... flatRanges) {
    if (flatRanges.length == 0) {
      return new long[][] {};
    }
    if (flatRanges.length % 2 != 0) {
      throw new IllegalArgumentException();
    }
    int rangeCount = flatRanges.length / 2;
    long[][] ranges = new long[rangeCount][];
    for (int i = 0; i < rangeCount; i++) {
      ranges[i] = new long[] {flatRanges[i * 2], flatRanges[i * 2 + 1]};
    }
    return ranges;
  }
}
