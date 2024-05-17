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

package org.apache.ignite.internal.storage.rocksdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import org.apache.ignite.internal.storage.RowId;

/**
 * Helper class for storages based on RocksDB.
 */
public class RocksDbStorageUtils {
    /** Key byte order. */
    public static final ByteOrder KEY_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    /** {@link RowId} size in bytes. */
    public static final int ROW_ID_SIZE = Long.BYTES * 2;

    /** Partition ID size in bytes. */
    public static final int PARTITION_ID_SIZE = Short.BYTES;

    /** Index ID size in bytes. */
    public static final int INDEX_ID_SIZE = Integer.BYTES;

    /** Zone ID size in bytes. */
    public static final int ZONE_ID_SIZE = Integer.BYTES;

    /** Table ID size in bytes. */
    public static final int TABLE_ID_SIZE = Integer.BYTES;

    static void putRowIdUuid(ByteBuffer keyBuffer, UUID rowIdUuid) {
        assert keyBuffer.order() == KEY_BYTE_ORDER;

        keyBuffer.putLong(normalize(rowIdUuid.getMostSignificantBits()));
        keyBuffer.putLong(normalize(rowIdUuid.getLeastSignificantBits()));
    }

    static UUID getRowIdUuid(ByteBuffer keyBuffer, int offset) {
        return new UUID(normalize(keyBuffer.getLong(offset)), normalize(keyBuffer.getLong(offset + Long.BYTES)));
    }

    /**
     * Converts signed long into a new long value, that when written in Big Endian, will preserve the comparison order if compared
     * lexicographically as an array of unsigned bytes. For example, values {@code -1} and {@code 0}, when written in BE, will become
     * {@code 0xFF..F} and {@code 0x00..0}, and lose their ascending order.
     *
     * <p>Flipping the sign bit will change the situation: {@code -1 -> 0x7F..F} and {@code 0 -> 0x80..0}.
     */
    static long normalize(long value) {
        return value ^ (1L << 63);
    }

    /**
     * Creates a byte array, that uses the {@code prefix} as a prefix, and every other {@code int} values as a 4-bytes chunk in Big Endian.
     */
    public static byte[] createKey(byte[] prefix, int... values) {
        ByteBuffer buf = ByteBuffer.allocate(prefix.length + Integer.BYTES * values.length).order(KEY_BYTE_ORDER);

        buf.put(prefix);

        for (int value : values) {
            buf.putInt(value);
        }

        return buf.array();
    }
}
