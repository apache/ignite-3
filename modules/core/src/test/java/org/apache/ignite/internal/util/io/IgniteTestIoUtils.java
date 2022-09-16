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

package org.apache.ignite.internal.util.io;

/**
 * IO test utilities.
 */
public final class IgniteTestIoUtils {

    /**
     * Gets short value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static short getShortByByteLittleEndian(byte[] arr) {
        return getShortByByteLittleEndian(arr, 0);
    }

    /**
     * Gets short value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static short getShortByByteLittleEndian(byte[] arr, int off) {
        return (short) ((arr[off] & 0xff) | arr[off + 1] << 8);
    }

    /**
     * Gets char value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static char getCharByByteLittleEndian(byte[] arr) {
        return getCharByByteLittleEndian(arr, 0);
    }

    /**
     * Gets char value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static char getCharByByteLittleEndian(byte[] arr, int off) {
        return (char) ((arr[off] & 0xff) | arr[off + 1] << 8);
    }

    /**
     * Gets integer value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static int getIntByByteLittleEndian(byte[] arr) {
        return getIntByByteLittleEndian(arr, 0);
    }

    /**
     * Gets integer value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static int getIntByByteLittleEndian(byte[] arr, int off) {
        return ((int) arr[off] & 0xff) | ((int) arr[off + 1] & 0xff) << 8
                | ((int) arr[off + 2] & 0xff) << 16 | ((int) arr[off + 3] & 0xff) << 24;
    }

    /**
     * Gets long value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static long getLongByByteLittleEndian(byte[] arr) {
        return getLongByByteLittleEndian(arr, 0);
    }

    /**
     * Gets long value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static long getLongByByteLittleEndian(byte[] arr, int off) {
        return ((long) arr[off] & 0xff) | ((long) arr[off + 1] & 0xff) << 8 | ((long) arr[off + 2] & 0xff) << 16
                | ((long) arr[off + 3] & 0xff) << 24 | ((long) arr[off + 4] & 0xff) << 32 | ((long) arr[off + 5] & 0xff) << 40
                | ((long) arr[off + 6] & 0xff) << 48 | ((long) arr[off + 7] & 0xff) << 56;
    }

    /**
     * Gets float value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static float getFloatByByteLittleEndian(byte[] arr) {
        return getFloatByByteLittleEndian(arr, 0);
    }

    /**
     * Gets float value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static float getFloatByByteLittleEndian(byte[] arr, int off) {
        return Float.intBitsToFloat(getIntByByteLittleEndian(arr, off));
    }

    /**
     * Gets double value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static double getDoubleByByteLittleEndian(byte[] arr) {
        return getDoubleByByteLittleEndian(arr, 0);
    }

    /**
     * Gets double value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static double getDoubleByByteLittleEndian(byte[] arr, int off) {
        return Double.longBitsToDouble(getLongByByteLittleEndian(arr, off));
    }

    /**
     * Ensure singleton.
     */
    private IgniteTestIoUtils() {
        // No-op.
    }
}
