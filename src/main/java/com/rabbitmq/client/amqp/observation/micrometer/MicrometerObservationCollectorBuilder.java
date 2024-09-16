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

/**
 * Builder to create an instance of {@link ObservationCollector} using <a
 * href="https://micrometer.io/">Micrometer</a> observation.
 */
public class MicrometerObservationCollectorBuilder {

  private ObservationRegistry registry = ObservationRegistry.NOOP;
  private PublishObservationConvention customPublishConvention = null;
  private PublishObservationConvention defaultPublishConvention =
      new DefaultPublishObservationConvention();
  private DeliverObservationConvention customProcessConvention = null;
  private DeliverObservationConvention defaultProcessConvention =
      new DefaultProcessObservationConvention();

  /**
   * Set the Micrometer's {@link ObservationRegistry}.
   *
   * @param registry registry
   * @return this builder instance
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public MicrometerObservationCollectorBuilder registry(ObservationRegistry registry) {
    this.registry = registry;
    return this;
  }

  /**
   * Set the custom {@link io.micrometer.observation.ObservationConvention} for publishing.
   *
   * @param customPublishConvention observation convention
   * @return this builder instance
   */
  public MicrometerObservationCollectorBuilder customPublishConvention(
      PublishObservationConvention customPublishConvention) {
    this.customPublishConvention = customPublishConvention;
    return this;
  }

  /**
   * Set the default {@link io.micrometer.observation.ObservationConvention} for publishing.
   *
   * @param defaultPublishConvention observation convention
   * @return this builder instance
   */
  public MicrometerObservationCollectorBuilder defaultPublishConvention(
      PublishObservationConvention defaultPublishConvention) {
    this.defaultPublishConvention = defaultPublishConvention;
    return this;
  }

  /**
   * Set the custom {@link io.micrometer.observation.ObservationConvention} for processing
   * (consuming).
   *
   * @param customProcessConvention observation convention
   * @return this builder instance
   */
  public MicrometerObservationCollectorBuilder customProcessConvention(
      DeliverObservationConvention customProcessConvention) {
    this.customProcessConvention = customProcessConvention;
    return this;
  }

  /**
   * Set the default {@link io.micrometer.observation.ObservationConvention} for processing
   * (consuming).
   *
   * @param defaultProcessConvention observation convention
   * @return this builder instance
   */
  public MicrometerObservationCollectorBuilder defaultProcessConvention(
      DeliverObservationConvention defaultProcessConvention) {
    this.defaultProcessConvention = defaultProcessConvention;
    return this;
  }

  /**
   * Create the configured instance.
   *
   * @return the created observation collector instance
   */
  public ObservationCollector build() {
    return new MicrometerObservationCollector(
        this.registry,
        this.customPublishConvention,
        this.defaultPublishConvention,
        this.customProcessConvention,
        this.defaultProcessConvention);
  }
}
