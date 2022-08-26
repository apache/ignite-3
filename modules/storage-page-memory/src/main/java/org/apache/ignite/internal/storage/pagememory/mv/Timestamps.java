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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.hlc.HybridTimestamp.HYBRID_TIMESTAMP_SIZE;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import java.nio.ByteBuffer;
import org.apache.ignite.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Code to work with {@link HybridTimestamp}s.
 */
public class Timestamps {
    /**
     * Reads a {@link HybridTimestamp} value from memory.
     *
     * @param pageAddr Address where page data starts.
     * @param offset Offset to the timestamp value relative to pageAddr.
     */
    static @Nullable HybridTimestamp readTimestamp(long pageAddr, int offset) {
        long physical = getLong(pageAddr, offset);
        int logical = getInt(pageAddr, offset + Long.BYTES);

        if (physical == 0L && logical == 0) {
            return null;
        }

        return new HybridTimestamp(physical, logical);
    }

    /**
     * Writes a {@link HybridTimestamp} to memory starting at the given address + offset.
     *
     * @param addr Memory address.
     * @param offset Offset added to the address.
     * @param timestamp The timestamp to write.
     * @return Number of bytes written.
     */
    public static int writeTimestampToMemory(long addr, int offset, @Nullable HybridTimestamp timestamp) {
        if (timestamp == null) {
            putLong(addr, offset, 0L);

            putInt(addr, offset + Long.BYTES, 0);
        } else {
            putLong(addr, offset, timestamp.getPhysical());

            putInt(addr, offset + Long.BYTES, timestamp.getLogical());
        }

        return HYBRID_TIMESTAMP_SIZE;
    }

    /**
     * Writes a {@link HybridTimestamp} to a buffer.
     *
     * @param buffer Buffer to which to write.
     * @param timestamp The timestamp to write.
     */
    public static void writeTimestampToBuffer(ByteBuffer buffer, @Nullable HybridTimestamp timestamp) {
        if (timestamp == null) {
            buffer.putLong(0L)
                    .putInt(0);
        } else {
            buffer.putLong(timestamp.getPhysical())
                    .putInt(timestamp.getLogical());
        }
    }
}
