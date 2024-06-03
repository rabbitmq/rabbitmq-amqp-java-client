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
package org.apache.qpid.protonj2.engine.impl.sasl;

import org.apache.qpid.protonj2.engine.EngineSaslDriver;
import org.apache.qpid.protonj2.engine.EngineState;
import org.apache.qpid.protonj2.engine.impl.ProtonEngine;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;

/**
 * Proton Engine SASL Context implementation.
 */
final class ProtonEngineSaslDriver implements EngineSaslDriver {

    /**
     * Default max frame size value used by this engine SASL driver if not otherwise configured.
     */
    public final static int DEFAULT_MAX_SASL_FRAME_SIZE = 4096;

    /*
     * The specification define lower bound for SASL frame size.
     */
    private final static int MIN_MAX_SASL_FRAME_SIZE = 512;

    private final ProtonSaslHandler handler;
    private final ProtonEngine engine;

    private int maxFrameSize = DEFAULT_MAX_SASL_FRAME_SIZE;
    private ProtonSaslContext context;

    ProtonEngineSaslDriver(ProtonEngine engine, ProtonSaslHandler handler) {
        this.handler = handler;
        this.engine = engine;
    }

    @Override
    public ProtonSaslClientContext client() {
        if (context != null && context.isServer()) {
            throw new IllegalStateException("Engine SASL Context already operating in server mode");
        }
        if (engine.state().ordinal() > EngineState.STARTED.ordinal()) {
            throw new IllegalStateException("Engine is already shutdown or failed, cannot create client context.");
        }

        if (context == null) {
            context = new ProtonSaslClientContext(handler);
            // If already started we initialize here to ensure that it gets done
            if (engine.state() == EngineState.STARTED) {
                context.handleContextInitialization(engine);
            }
        }

        return (ProtonSaslClientContext) context;
    }

    @Override
    public ProtonSaslServerContext server() {
        if (context != null && context.isClient()) {
            throw new IllegalStateException("Engine SASL Context already operating in client mode");
        }
        if (engine.state().ordinal() > EngineState.STARTED.ordinal()) {
            throw new IllegalStateException("Engine is already shutdown or failed, cannot create server context.");
        }

        if (context == null) {
            context = new ProtonSaslServerContext(handler);
            // If already started we initialize here to ensure that it gets done
            if (engine.state() == EngineState.STARTED) {
                context.handleContextInitialization(engine);
            }
        }

        return (ProtonSaslServerContext) context;
    }

    @Override
    public SaslState getSaslState() {
        return context == null ? SaslState.IDLE : context.getSaslState();
    }

    @Override
    public SaslOutcome getSaslOutcome() {
        return context == null ? null : context.getSaslOutcome();
    }

    @Override
    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    @Override
    public void setMaxFrameSize(int maxFrameSize) {
        if (getSaslState() == SaslState.IDLE) {
            if (maxFrameSize < MIN_MAX_SASL_FRAME_SIZE) {
                throw new IllegalArgumentException("Cannot set a max frame size lower than: " + MIN_MAX_SASL_FRAME_SIZE);
            } else {
                this.maxFrameSize = maxFrameSize;
            }
        } else {
            throw new IllegalStateException("Cannot configure max SASL frame size after SASL negotiations have started");
        }
    }

    //----- Internal Engine SASL Context API

    void handleEngineStarting(ProtonEngine engine) {
        if (context != null) {
            context.handleContextInitialization(engine);
        }
    }

    ProtonSaslContext context() {
        return context;
    }
}
