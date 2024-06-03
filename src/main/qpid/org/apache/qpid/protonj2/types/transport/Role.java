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
package org.apache.qpid.protonj2.types.transport;

import org.apache.qpid.protonj2.codec.EncodingCodes;

public enum Role {

    SENDER(false), RECEIVER(true);

    private final boolean receiver;

    private Role(boolean receiver) {
        this.receiver = receiver;
    }

    public boolean getValue() {
        return receiver;
    }

    public byte encodingCode() {
        if (receiver) {
            return EncodingCodes.BOOLEAN_TRUE;
        } else {
            return EncodingCodes.BOOLEAN_FALSE;
        }
    }

    public static Role valueOf(boolean role) {
        if (role) {
            return RECEIVER;
        } else {
            return SENDER;
        }
    }

    public static Role valueOf(Boolean role) {
        if (Boolean.TRUE.equals(role)) {
            return RECEIVER;
        } else {
            return SENDER;
        }
    }
}
