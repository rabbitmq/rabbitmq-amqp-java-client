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

public final class Tuples {

  private Tuples() {}

  public static <A, B> Pair<A, B> pair(A v1, B v2) {
    return new Pair<>(v1, v2);
  }

  public static class Pair<A, B> {

    private final A v1;
    private final B v2;

    private Pair(A v1, B v2) {
      this.v1 = v1;
      this.v2 = v2;
    }

    public A v1() {
      return this.v1;
    }

    public B v2() {
      return this.v2;
    }
  }
}
