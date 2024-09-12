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
package com.rabbitmq.client.amqp;

import java.util.Objects;

/** Utility class to represent a hostname and a port. */
public class Address {

  private final String host;

  private final int port;

  public Address(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public String host() {
    return this.host;
  }

  public int port() {
    return this.port;
  }

  @Override
  public String toString() {
    return "Address{" + "host='" + host + '\'' + ", port=" + port + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Address address = (Address) o;
    return port == address.port && host.equals(address.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }
}
