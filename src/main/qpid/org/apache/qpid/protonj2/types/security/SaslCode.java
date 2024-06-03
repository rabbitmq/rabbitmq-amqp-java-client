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
package org.apache.qpid.protonj2.types.security;

import org.apache.qpid.protonj2.types.UnsignedByte;

public enum SaslCode {

    OK, AUTH, SYS, SYS_PERM, SYS_TEMP;

    public UnsignedByte getValue() {
        return UnsignedByte.valueOf((byte) ordinal());
    }

    public byte byteValue() {
        return UnsignedByte.valueOf((byte) ordinal()).byteValue();
    }

    public static SaslCode valueOf(UnsignedByte v) {
        return SaslCode.values()[v.byteValue()];
    }
}
