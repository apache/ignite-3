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
 * Utilities to work with general-purpose varints: that is, integers represented with a variable number of bytes.
 *
 * <p>The varint encoding used here is as follows. First, the number is incremented (this is a tweak to allow -1 be
 * represented as 1 byte as this encoding only produces compact representation for positive numbers of small magnitude).
 * Then the binary string representing the number is split into 7-bit chunks,
 * each chunk that is not a 'leading zero' is encoded as 1 byte. The highest bit of the byte is 1 if there are more
 * bytes (encoding more chunks) and 0 if this is the last byte of the varint representation. Other 7 bits of the byte
 * represent the bits of the chunk. The least-significant chunk is encoded first.
 *
 * <p>This is a variation of <a href="https://en.wikipedia.org/wiki/Variable-length_quantity">VLQ</a> ;
 * the difference is that we encode least-significant chunk first (and not last as they do) and that we increment
 * the number before encoding it.
 */
public class VarIntUtils {
    /**
     * Returns number of bytes that are needed to represent the given integer as a varint.
     *
     * @param val Int value.
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
