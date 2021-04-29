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

package org.apache.ignite.network.internal.direct;

import java.nio.ByteBuffer;

/**
 * Direct marshalling utils.
 */
public class DirectUtils {
    /**
     * Reads a direct message type from a byte buffer.
     *
     * @param buffer Byte buffer.
     * @return Direct message type.
     */
    public static short getMessageType(ByteBuffer buffer) {
        byte b0 = buffer.get();
        byte b1 = buffer.get();

        return makeMessageType(b0, b1);
    }

    /**
     * Concatenates the two parameter bytes to form a message type value.
     *
     * @param b0 The first byte.
     * @param b1 The second byte.
     */
    private static short makeMessageType(byte b0, byte b1) {
        return (short)((b1 & 0xFF) << 8 | b0 & 0xFF);
    }

}
