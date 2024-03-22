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

import java.util.List;

interface ManagementRecovery {

  ManagementRecovery NO_OP = new NoOpManagementRecovery();

  void exchangeDeclared(AmqpExchangeSpecification specification);

  void exchangeDeleted(String name);

  void queueDeclared(AmqpQueueSpecification specification);

  void queueDeleted(String name);

  void bindingDeclared(AmqpBindingManagement.AmqpBindingSpecification specification);

  void bindingDeleted(AmqpBindingManagement.AmqpUnbindSpecification specification);

  void consumerCreated(long id, String address);

  void consumerDeleted(long id, String address);

  static ManagementRecovery compose(List<ManagementRecovery> managementRecoveries) {
    return new ManagementRecovery() {

      @Override
      public void exchangeDeclared(AmqpExchangeSpecification specification) {
        managementRecoveries.forEach(mr -> mr.exchangeDeclared(specification));
      }

      @Override
      public void exchangeDeleted(String name) {
        managementRecoveries.forEach(mr -> mr.exchangeDeleted(name));
      }

      @Override
      public void queueDeclared(AmqpQueueSpecification specification) {
        managementRecoveries.forEach(mr -> mr.queueDeclared(specification));
      }

      @Override
      public void queueDeleted(String name) {
        managementRecoveries.forEach(mr -> mr.queueDeleted(name));
      }

      @Override
      public void bindingDeclared(AmqpBindingManagement.AmqpBindingSpecification specification) {
        managementRecoveries.forEach(mr -> mr.bindingDeclared(specification));
      }

      @Override
      public void bindingDeleted(AmqpBindingManagement.AmqpUnbindSpecification specification) {
        managementRecoveries.forEach(mr -> mr.bindingDeleted(specification));
      }

      @Override
      public void consumerCreated(long id, String address) {
        managementRecoveries.forEach(mr -> mr.consumerCreated(id, address));
      }

      @Override
      public void consumerDeleted(long id, String address) {
        managementRecoveries.forEach(mr -> mr.consumerDeleted(id, address));
      }
    };
  }

  class NoOpManagementRecovery implements ManagementRecovery {

    @Override
    public void exchangeDeclared(AmqpExchangeSpecification specification) {}

    @Override
    public void exchangeDeleted(String name) {}

    @Override
    public void queueDeclared(AmqpQueueSpecification specification) {}

    @Override
    public void queueDeleted(String name) {}

    @Override
    public void bindingDeclared(AmqpBindingManagement.AmqpBindingSpecification specification) {}

    @Override
    public void bindingDeleted(AmqpBindingManagement.AmqpUnbindSpecification specification) {}

    @Override
    public void consumerCreated(long id, String address) {}

    @Override
    public void consumerDeleted(long id, String address) {}
  }
}
