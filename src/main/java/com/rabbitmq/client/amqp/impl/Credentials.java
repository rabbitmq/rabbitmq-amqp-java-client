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

interface Credentials {

  Credentials NO_OP = new NoOpCredentials();

  Registration register(String name, AuthenticationCallback refreshCallback);

  interface Registration {

    void connect(AuthenticationCallback callback);

    void unregister();
  }

  interface AuthenticationCallback {

    void authenticate(String username, String password);
  }

  class NoOpCredentials implements Credentials {

    @Override
    public Registration register(String name, AuthenticationCallback refreshCallback) {
      return new NoOpRegistration();
    }
  }

  class NoOpRegistration implements Registration {

    @Override
    public void connect(AuthenticationCallback callback) {}

    @Override
    public void unregister() {}
  }
}
