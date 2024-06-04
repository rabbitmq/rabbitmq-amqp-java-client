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
package com.rabbitmq.client.amqp.observation.micrometer;

import com.rabbitmq.client.amqp.ObservationCollector;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.observation.ObservationRegistry;

public class MicrometerObservationCollectorBuilder {

  private ObservationRegistry registry = ObservationRegistry.NOOP;
  private PublishObservationConvention customPublishConvention = null;
  private PublishObservationConvention defaultPublishConvention =
      new DefaultPublishObservationConvention();
  private DeliverObservationConvention customProcessConvention = null;
  private DeliverObservationConvention defaultProcessConvention =
      new DefaultProcessObservationConvention();

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public MicrometerObservationCollectorBuilder registry(ObservationRegistry registry) {
    this.registry = registry;
    return this;
  }

  public MicrometerObservationCollectorBuilder customPublishConvention(
      PublishObservationConvention customPublishConvention) {
    this.customPublishConvention = customPublishConvention;
    return this;
  }

  public MicrometerObservationCollectorBuilder defaultPublishConvention(
      PublishObservationConvention defaultPublishConvention) {
    this.defaultPublishConvention = defaultPublishConvention;
    return this;
  }

  public MicrometerObservationCollectorBuilder customProcessConvention(
      DeliverObservationConvention customProcessConvention) {
    this.customProcessConvention = customProcessConvention;
    return this;
  }

  public MicrometerObservationCollectorBuilder defaultProcessConvention(
      DeliverObservationConvention defaultProcessConvention) {
    this.defaultProcessConvention = defaultProcessConvention;
    return this;
  }

  public ObservationCollector build() {
    return new MicrometerObservationCollector(
        this.registry,
        this.customPublishConvention,
        this.defaultPublishConvention,
        this.customProcessConvention,
        this.defaultProcessConvention);
  }
}
