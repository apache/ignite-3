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
import org.apache.ignite.internal.lang.IgniteStringBuilder;

/**
 * Utility methods for converting various types to HEX string representation.
 */
public class HexStringUtils {
    /** Byte bit-mask. */
    private static final int MASK = 0xf;

    /**
     * Converts byte array to hex string.
     *
     * @param arr Array of bytes.
     * @return Hex string.
     */
    public static String toHexString(byte[] arr) {
        return toHexString(arr, Integer.MAX_VALUE);
    }

    /**
     * Converts byte array to hex string.
     *
     * @param arr Array of bytes.
     * @param maxLen Maximum length of result string. Rounds down to a power of two.
     * @return Hex string.
     */
    public static String toHexString(byte[] arr, int maxLen) {
        assert maxLen >= 0 : "maxLem must be not negative.";

        int capacity = Math.min(arr.length << 1, maxLen);

        int lim = capacity >> 1;

        StringBuilder sb = new StringBuilder(capacity);

        for (int i = 0; i < lim; i++) {
            addByteAsHex(sb, arr[i]);
        }

        return sb.toString().toUpperCase();
    }

    /**
     * Returns hex representation of memory region.
     *
     * @param addr Pointer in memory.
     * @param len How much byte to read.
     */
    public static String toHexString(long addr, int len) {
        StringBuilder sb = new StringBuilder(len * 2);

        for (int i = 0; i < len; i++) {
            // Can not use getLong because on little-endian it produces wrong result.
            addByteAsHex(sb, GridUnsafe.getByte(addr + i));
        }

        return sb.toString();
    }

    /**
     * Returns hex representation of memory region.
     *
     * @param buf Buffer which content should be converted to string.
     */
    public static String toHexString(ByteBuffer buf) {
        StringBuilder sb = new StringBuilder(buf.capacity() * 2);

        for (int i = buf.position(); i < buf.limit(); i++) {
            // Can not use getLong because on little-endian it produces wrong result.
            addByteAsHex(sb, buf.get(i));
        }

        return sb.toString();
    }

    /**
     * Returns byte array represented by given hex string.
     *
     * @param s String containing a hex representation of bytes.
     * @return A byte array.
     */
    public static byte[] fromHexString(String s) {
        var len = s.length();

        assert (len & 1) == 0 : "length should be even";

        var data = new byte[len / 2];

        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }

        return data;
    }

    /**
     * Appends {@code byte} in hexadecimal format.
     *
     * @param sb String builder.
     * @param b Byte to add in hexadecimal format.
     */
    private static void addByteAsHex(StringBuilder sb, byte b) {
        sb.append(Integer.toHexString(MASK & b >>> 4)).append(Integer.toHexString(MASK & b));
    }

    /**
     * Returns a hex string representation of the given long value.
     *
     * @param val Value to convert to string.
     * @return Hex string.
     */
    public static String hexLong(long val) {
        return new IgniteStringBuilder(16).appendHex(val).toString();
    }

    /**
     * Returns a hex string representation of the given integer value.
     *
     * @param val Value to convert to string.
     * @return Hex string.
     */
    public static String hexInt(int val) {
        return new IgniteStringBuilder(8).appendHex(val).toString();
    }
}
