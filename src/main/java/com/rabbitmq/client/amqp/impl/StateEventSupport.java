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

import com.rabbitmq.client.amqp.Resource;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StateEventSupport {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateEventSupport.class);

  private final List<Resource.StateListener> listeners;

  StateEventSupport(List<Resource.StateListener> listeners) {
    this.listeners = new ArrayList<>(listeners);
  }

  void dispatch(
      Resource resource,
      Throwable failureCause,
      Resource.State previousState,
      Resource.State currentState) {
    if (!this.listeners.isEmpty()) {
      Resource.Context context =
          new DefaultContext(resource, failureCause, previousState, currentState);
      this.listeners.forEach(
          l -> {
            try {
              l.handle(context);
            } catch (Exception e) {
              LOGGER.warn("Error in resource listener", e);
            }
          });
    }
  }

  private static class DefaultContext implements Resource.Context {

    private final Resource resource;
    private final Throwable failureCause;
    private final Resource.State previousState;
    private final Resource.State currentState;

    private DefaultContext(
        Resource resource,
        Throwable failureCause,
        Resource.State previousState,
        Resource.State currentState) {
      this.resource = resource;
      this.failureCause = failureCause;
      this.previousState = previousState;
      this.currentState = currentState;
    }

    @Override
    public Resource resource() {
      return this.resource;
    }

    @Override
    public Throwable failureCause() {
      return this.failureCause;
    }

    @Override
    public Resource.State previousState() {
      return this.previousState;
    }

    @Override
    public Resource.State currentState() {
      return this.currentState;
    }
  }
}
