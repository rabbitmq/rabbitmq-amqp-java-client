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

import com.rabbitmq.model.Management;
import com.rabbitmq.model.ModelException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

abstract class AmqpBindingManagement {

  private AmqpBindingManagement() {}

  static class BindingState {

    private final AmqpManagement managememt;

    private String source, destination, key;
    private final Map<String, Object> arguments = new LinkedHashMap<>();
    private boolean toQueue = true;

    BindingState(AmqpManagement managememt) {
      this.managememt = managememt;
    }
  }

  static class AmqpBindingSpecification implements Management.BindingSpecification {

    private final BindingState state;

    AmqpBindingSpecification(AmqpManagement management) {
      this.state = new BindingState(management);
    }

    @Override
    public Management.BindingSpecification queue() {
      this.state.toQueue = true;
      return this;
    }

    @Override
    public Management.BindingSpecification exchange() {
      this.state.toQueue = false;
      return this;
    }

    @Override
    public Management.BindingSpecification source(String source) {
      this.state.source = source;
      return this;
    }

    @Override
    public Management.BindingSpecification destination(String destination) {
      this.state.destination = destination;
      return this;
    }

    @Override
    public Management.BindingSpecification key(String key) {
      this.state.key = key;
      return this;
    }

    @Override
    public Management.BindingSpecification argument(String key, Object value) {
      this.state.arguments.put(key, value);
      return this;
    }

    @Override
    public void bind() {
      try {
        if (this.state.toQueue) {
          this.state
              .managememt
              .channel()
              .queueBind(
                  this.state.destination, this.state.source,
                  this.state.key, this.state.arguments);

        } else {
          this.state
              .managememt
              .channel()
              .exchangeBind(
                  this.state.destination, this.state.source,
                  this.state.key, this.state.arguments);
        }
      } catch (IOException e) {
        throw new ModelException(e);
      }
    }
  }

  static class AmqpUnbindSpecification implements Management.UnbindSpecification {

    private final BindingState state;

    AmqpUnbindSpecification(AmqpManagement management) {
      this.state = new BindingState(management);
    }

    @Override
    public Management.UnbindSpecification queue() {
      this.state.toQueue = true;
      return this;
    }

    @Override
    public Management.UnbindSpecification exchange() {
      this.state.toQueue = false;
      return this;
    }

    @Override
    public Management.UnbindSpecification source(String source) {
      this.state.source = source;
      return this;
    }

    @Override
    public Management.UnbindSpecification destination(String destination) {
      this.state.destination = destination;
      return this;
    }

    @Override
    public Management.UnbindSpecification key(String key) {
      this.state.key = key;
      return this;
    }

    @Override
    public Management.UnbindSpecification argument(String key, Object value) {
      this.state.arguments.put(key, value);
      return this;
    }

    @Override
    public void unbind() {
      try {
        if (this.state.toQueue) {
          this.state
              .managememt
              .channel()
              .queueUnbind(
                  this.state.destination, this.state.source,
                  this.state.key, this.state.arguments);

        } else {
          this.state
              .managememt
              .channel()
              .exchangeUnbind(
                  this.state.destination, this.state.source,
                  this.state.key, this.state.arguments);
        }
      } catch (IOException e) {
        throw new ModelException(e);
      }
    }
  }
}
