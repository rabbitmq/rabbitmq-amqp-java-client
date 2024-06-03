/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.engine.impl;

import org.apache.qpid.protonj2.engine.EngineSaslDriver;
import org.apache.qpid.protonj2.engine.sasl.SaslClientContext;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;
import org.apache.qpid.protonj2.engine.sasl.SaslServerContext;

/**
 * A Default No-Op SASL context that is used to provide the engine with a stub
 * when no SASL is configured for the operating engine.
 */
public final class ProtonEngineNoOpSaslDriver implements EngineSaslDriver {

    /**
     * Default singleton instance of the No-Op SASL engine driver.
     */
    public static final ProtonEngineNoOpSaslDriver INSTANCE = new ProtonEngineNoOpSaslDriver();

    /**
     * AMQP specified default value for maximum SASL frame size.
     */
    public static final int MIN_MAX_SASL_FRAME_SIZE = 512;

    @Override
    public SaslState getSaslState() {
        return SaslState.NONE;
    }

    @Override
    public SaslOutcome getSaslOutcome() {
        return null;
    }

    @Override
    public int getMaxFrameSize() {
        return MIN_MAX_SASL_FRAME_SIZE;
    }

    @Override
    public void setMaxFrameSize(int maxFrameSize) {
    }

    @Override
    public SaslClientContext client() {
        throw new IllegalStateException("Engine not configured with a SASL layer");
    }

    @Override
    public SaslServerContext server() {
        throw new IllegalStateException("Engine not configured with a SASL layer");
    }
}
