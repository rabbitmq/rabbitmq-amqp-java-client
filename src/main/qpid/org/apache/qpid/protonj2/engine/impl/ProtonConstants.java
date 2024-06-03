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

/**
 * Constants referenced throughout the proton engine code.
 */
public final class ProtonConstants {

    /**
     * The minimum allowed AMQP maximum frame size defined by the specification.
     */
    public static final int MIN_MAX_AMQP_FRAME_SIZE = 512;

    /**
     * The default AMQP max frame size used by the engine and connection if none is set
     * by the client or remote peer.
     */
    public static final int DEFAULT_MAX_AMQP_FRAME_SIZE = 65535;

    /**
     * The maximum value for AMQP channels as defined by the specification.
     */
    public static final int CHANNEL_MAX = 65535;

    /**
     * The maximum value for AMQP handles as defined by the specification.
     */
    public static final long HANDLE_MAX = 0xFFFFFFFFL;

    //----- Proton engine handler names

    /**
     * Engine handler that acts on AMQP performatives
     */
    public static final String AMQP_PERFORMATIVE_HANDLER = "amqp";

    /**
     * Engine handler that acts on SASL performatives
     */
    public static final String SASL_PERFORMATIVE_HANDLER = "sasl";

    /**
     * Engine handler that encodes performatives and writes the resulting buffer
     */
    public static final String FRAME_ENCODING_HANDLER = "frame-encoder";

    /**
     * Engine handler that decodes performatives and forwards the frames
     */
    public static final String FRAME_DECODING_HANDLER = "frame-decoder";

    /**
     * Engine handler that logs incoming and outgoing performatives and frames
     */
    public static final String FRAME_LOGGING_HANDLER = "frame-logger";

}
