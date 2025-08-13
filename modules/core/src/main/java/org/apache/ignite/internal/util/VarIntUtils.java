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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
     * Returns number of bytes that are needed to represent the given long as a varint.
     *
     * @param val Int value.
     */
    public static int varIntLength(long val) {
        val++;

        int len = 0;

        while ((val & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
            len++;

            val >>>= 7;
        }

        return len + 1;
    }

    /**
     * Writes a primitive {@code long} value as a varint to the provided output.
     *
     * @param val Value.
     * @return Number of bytes written.
     */
    public static int writeVarInt(long val, DataOutput out) throws IOException {
        val++;

        int written = 0;

        while ((val & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
            byte b = (byte) (val | 0x80);

            out.writeByte(b);

            val >>>= 7;
            written++;
        }

        out.writeByte((byte) val);

        return written + 1;
    }

    /**
     * Writes a primitive {@code int} value as a varint to the provided array.
     *
     * @param val Int value.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int putVarIntToBytes(long val, byte[] bytes, int off) {
        val++;

        int pos = off;

        while ((val & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
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
     * <p>https://issues.apache.org/jira/browse/IGNITE-26177
     *
     * @param buf Buffer from which to read.
     * @return Long value that was encoded as a varint.
     */
    @SuppressWarnings("PMD.UnnecessaryCast")
    public static long readVarInt(ByteBuffer buf) {
        long res = 0;

        for (int shift = 0; ; shift += 7) {
            byte b = buf.get();

            res |= ((long) b & 0x7F) << shift;

            if (b >= 0) {
                break;
            }
        }

        return res - 1;
    }

    /**
     * Reads a varint from a byte buffer.
     *
     * <p>https://issues.apache.org/jira/browse/IGNITE-26177
     *
     * @param bytes Array from which to read.
     * @param off Offset to start reading from.
     * @return Long value that was encoded as a varint.
     */
    @SuppressWarnings("PMD.UnnecessaryCast")
    public static long readVarInt(byte[] bytes, int off) {
        long res = 0;

        int index = off;
        for (int shift = 0; ; shift += 7) {
            byte b = bytes[index++];

            res |= ((long) b & 0x7F) << shift;

            if (b >= 0) {
                break;
            }
        }

        return res - 1;
    }

    /**
     * Reads a varint from an input.
     *
     * <p>https://issues.apache.org/jira/browse/IGNITE-26177
     *
     * @param in Input from which to read.
     * @return Long value that was encoded as a varint.
     */
    @SuppressWarnings("PMD.UnnecessaryCast")
    public static long readVarInt(DataInput in) throws IOException {
        long res = 0;

        for (int shift = 0; ; shift += 7) {
            byte b = in.readByte();

            res |= ((long) b & 0x7F) << shift;

            if (b >= 0) {
                break;
            }
        }

        return res - 1;
    }
}
