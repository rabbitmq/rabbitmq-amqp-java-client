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
package com.rabbitmq.amqp1.impl;

import static com.rabbitmq.amqp1.Resource.State.OPEN;
import static com.rabbitmq.amqp1.Resource.State.OPENING;

import com.rabbitmq.amqp1.ModelException;
import com.rabbitmq.amqp1.Resource;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

abstract class ResourceBase implements Resource {

  private final AtomicReference<State> state = new AtomicReference<>();
  private final StateEventSupport stateEventSupport;

  ResourceBase(List<StateListener> listeners) {
    this.stateEventSupport = new StateEventSupport(listeners);
    this.state(OPENING);
  }

  protected void checkOpen() {
    if (this.state.get() != OPEN) {
      throw new ModelException(
          "Resource is not open, current state is %s", this.state.get().name());
    }
  }

  protected void state(Resource.State state) {
    this.state(state, null);
  }

  protected void state(Resource.State state, Throwable failureCause) {
    Resource.State previousState = this.state.getAndSet(state);
    if (state != previousState) {
      this.dispatch(previousState, state, failureCause);
    }
  }

  private void dispatch(State previous, State current, Throwable failureCause) {
    this.stateEventSupport.dispatch(this, failureCause, previous, current);
  }
}
