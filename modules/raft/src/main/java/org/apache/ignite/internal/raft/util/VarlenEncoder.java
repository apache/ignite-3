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

package org.apache.ignite.internal.raft.util;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Class containing methods for encoding/decoding long values using variable length encoding.
 */
// Based on DirectByteBufferStreamImplV1.
public class VarlenEncoder {
    /** Mask for clearing 7 least significant bits. */
    private static final long LEAST_SIGNIFICANT_BITS_MASK = 0xFFFF_FFFF_FFFF_FF80L;

    /**
     * Writes the given value to the given buffer.
     *
     * @return Number of bytes written.
     */
    public static int writeLong(long val, ByteBuffer out) {
        int startPos = out.position();

        while ((val & LEAST_SIGNIFICANT_BITS_MASK) != 0) {
            byte b = (byte) (val | 0x80);

            out.put(b);

            val >>>= 7;
        }

        out.put((byte) val);

        return out.position() - startPos;
    }

    /**
     * Writes the given value to the given buffer address.
     *
     * @return Number of bytes written.
     * @see GridUnsafe#bufferAddress(ByteBuffer)
     */
    public static int writeLong(long val, long addr) {
        int offset = 0;

        while ((val & LEAST_SIGNIFICANT_BITS_MASK) != 0) {
            byte b = (byte) (val | 0x80);

            GridUnsafe.putByte(addr + offset, b);

            val >>>= 7;

            offset++;
        }

        GridUnsafe.putByte(addr + offset, (byte) val);

        offset++;

        return offset;
    }

    /**
     * Returns the number of bytes, required by the {@link #writeLong} to write the value.
     */
    public static int sizeInBytes(long val) {
        if (val >= 0) {
            if (val < (1L << 7)) {
                return 1;
            } else if (val < (1L << 14)) {
                return 2;
            } else if (val < (1L << 21)) {
                return 3;
            } else if (val < (1L << 28)) {
                return 4;
            } else if (val < (1L << 35)) {
                return 5;
            } else if (val < (1L << 42)) {
                return 6;
            } else if (val < (1L << 49)) {
                return 7;
            } else if (val < (1L << 56)) {
                return 8;
            } else {
                return 9;
            }
        } else {
            return 10;
        }
    }

    /**
     * Reads a long value, written by one of the {@link #writeLong} methods, from the given buffer.
     */
    public static long readLong(ByteBuffer buffer) {
        long val = 0;
        int shift = 0;

        while (true) {
            byte b = buffer.get();

            val |= (long) (b & 0x7F) << shift;

            if ((b & 0x80) == 0) {
                return val;
            } else {
                shift += 7;
            }
        }
    }
}
