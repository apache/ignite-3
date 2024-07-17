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

package org.apache.ignite.internal.metastorage.server.persistence;

import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.ignite.internal.metastorage.server.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for {@link RocksDbKeyValueStorage}.
 */
class RocksStorageUtils {
    /**
     * VarHandle that gives the access to the elements of a {@code byte[]} array viewed as if it were a {@code long[]} array. Byte order
     * must be big endian for a correct lexicographic order comparison.
     */
    private static final VarHandle LONG_ARRAY_HANDLE = MethodHandles.byteArrayViewVarHandle(
            long[].class,
            ByteOrder.BIG_ENDIAN
    );

    /**
     * Converts a long value to a byte array.
     *
     * @param value Value.
     * @return Byte array.
     */
    static byte[] longToBytes(long value) {
        var buffer = new byte[Long.BYTES];

        putLongToBytes(value, buffer, 0);

        return buffer;
    }

    /**
     * Puts a {@code long} value to the byte array at specified position.
     *
     * @param value Value.
     * @param array Byte array.
     * @param position Position to put long at.
     */
    static void putLongToBytes(long value, byte[] array, int position) {
        assert position + Long.BYTES <= array.length : "pos=" + position + ", arr.len=" + array.length;

        LONG_ARRAY_HANDLE.set(array, position, value);
    }

    /**
     * Converts a byte array to a long value.
     *
     * @param array Byte array.
     * @return Long value.
     */
    static long bytesToLong(byte[] array) {
        assert array.length == Long.BYTES;

        return bytesToLong(array, 0);
    }

    /**
     * Gets a {@code long} value from a byte array starting from the specified position.
     *
     * @param array Byte array.
     * @param offset Offset to read a value from.
     * @return {@code long} value.
     */
    static long bytesToLong(byte[] array, int offset) {
        assert offset + Long.BYTES <= array.length : "off=" + offset + ", arr.len=" + array.length;

        return (long) LONG_ARRAY_HANDLE.get(array, offset);
    }

    /**
     * Adds a revision to a key.
     *
     * @param revision Revision.
     * @param key      Key.
     * @return Key with a revision.
     */
    static byte[] keyToRocksKey(long revision, byte[] key) {
        var buffer = new byte[Long.BYTES + key.length];

        LONG_ARRAY_HANDLE.set(buffer, 0, revision);

        System.arraycopy(key, 0, buffer, Long.BYTES, key.length);

        return buffer;
    }

    /**
     * Gets a key from a key with a revision.
     *
     * @param rocksKey Key with a revision.
     * @return Key without a revision.
     */
    static byte[] rocksKeyToBytes(byte[] rocksKey) {
        // Copy bytes of the rocks key ignoring the revision (first 8 bytes)
        return Arrays.copyOfRange(rocksKey, Long.BYTES, rocksKey.length);
    }

    /**
     * Gets a revision from a key with a revision.
     *
     * @param rocksKey Key with a revision.
     * @return Revision.
     */
    static long revisionFromRocksKey(byte[] rocksKey) {
        return (long) LONG_ARRAY_HANDLE.get(rocksKey, 0);
    }

    /**
     * Builds a value from a byte array.
     *
     * @param valueBytes Value byte array.
     * @return Value.
     */
    static Value bytesToValue(byte[] valueBytes) {
        // At least an 8-byte update counter and a 1-byte boolean
        assert valueBytes.length > Long.BYTES;

        // Read an update counter (8-byte long) from the entry.
        long updateCounter = (long) LONG_ARRAY_HANDLE.get(valueBytes, 0);

        // Read a has-value flag (1 byte) from the entry.
        boolean hasValue = valueBytes[Long.BYTES] != 0;

        byte[] val;
        if (hasValue) { // Copy the value.
            val = Arrays.copyOfRange(valueBytes, Long.BYTES + 1, valueBytes.length);
        } else { // There is no value, mark it as a tombstone.
            val = TOMBSTONE;
        }

        return new Value(val, updateCounter);
    }

    /**
     * Adds an update counter and a tombstone flag to a value.
     *
     * @param value         Value byte array.
     * @param updateCounter Update counter.
     * @return Value with an update counter and a tombstone.
     */
    static byte[] valueToBytes(byte[] value, long updateCounter) {
        var bytes = new byte[Long.BYTES + Byte.BYTES + value.length];

        LONG_ARRAY_HANDLE.set(bytes, 0, updateCounter);

        bytes[Long.BYTES] = (byte) (value == TOMBSTONE ? 0 : 1);

        System.arraycopy(value, 0, bytes, Long.BYTES + Byte.BYTES, value.length);

        return bytes;
    }

    /**
     * Gets an array of longs from the byte array of longs.
     *
     * @param bytes Byte array of longs.
     * @return Array of longs.
     */
    static long[] getAsLongs(byte[] bytes) {
        // Value must be divisible by a size of a long, because it's a list of longs
        assert (bytes.length % Long.BYTES) == 0;

        int size = bytes.length / Long.BYTES;

        long[] res = new long[size];

        for (int i = 0; i < size; i++) {
            res[i] = (long) LONG_ARRAY_HANDLE.get(bytes, i * Long.BYTES);
        }

        return res;
    }

    /**
     * Add a long value to an array of longs that is represented by an array of bytes.
     *
     * @param bytes Byte array that represents an array of longs.
     * @param value New long value.
     * @return Byte array with a new value.
     */
    static byte[] appendLong(byte @Nullable [] bytes, long value) {
        if (bytes == null) {
            return longToBytes(value);
        }

        // Copy the current value
        var result = Arrays.copyOf(bytes, bytes.length + Long.BYTES);

        LONG_ARRAY_HANDLE.set(result, bytes.length, value);

        return result;
    }

    /**
     * Converts an array of {@code long} values to an array of bytes.
     *
     * @param values Array of values.
     * @param valuesOffset Offset in the array of values to start from.
     * @return Array of bytes.
     */
    static byte[] longsToBytes(long[] values, int valuesOffset) {
        assert valuesOffset < values.length : "off=" + valuesOffset + ", arr.len=" + values.length;

        var result = new byte[(values.length - valuesOffset) * Long.BYTES];

        for (int srcIdx = valuesOffset, dstIdx = 0; srcIdx < values.length; srcIdx++, dstIdx++) {
            long val = values[srcIdx];

            LONG_ARRAY_HANDLE.set(result, dstIdx * Long.BYTES, val);
        }

        return result;
    }
}
