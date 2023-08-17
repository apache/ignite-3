/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.nio.ByteBuffer;

/**
 * Utilities to work with general-purpose varints.
 */
public class VarIntUtils {
    /**
     * Returns number of bytes that are needed to represent the given integer as a varint.
     *
     * @param val Int value.
     * @return Number of bytes that are needed to represent the given integer as a varint.
     */
    public static int varIntLength(int val) {
        val++;

        int len = 0;

        while ((val & 0xFFFF_FF80) != 0) {
            len++;

            val >>>= 7;
        }

        return len + 1;
    }

    /**
     * Writes a primitive {@code int} value as a varint to the provided array.
     *
     * @param val Int value.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int putVarIntToBytes(int val, byte[] bytes, int off) {
        val++;

        int pos = off;

        while ((val & 0xFFFF_FF80) != 0) {
            byte b = (byte) (val | 0x80);

            bytes[pos++] = b;

            val >>>= 7;
        }

        bytes[pos++] = (byte) val;

        return pos - off;
    }

    /**
     * Reads a varint from a buffer.
     *
     * @param buf Buffer from which to read.
     * @return Integer value that was encoded as a varint.
     */
    public static int readVarInt(ByteBuffer buf) {
        int res = 0;

        for (int shift = 0; ; shift += 7) {
            byte b = buf.get();

            res |= (b & 0x7F) << shift;

            if (b >= 0) {
                break;
            }
        }

        return res - 1;
    }
}
