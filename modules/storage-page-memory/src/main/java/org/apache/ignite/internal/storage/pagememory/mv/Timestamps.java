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

import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.tx.Timestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Code to work with {@link Timestamp}s.
 */
public class Timestamps {
    /**
     * Reads a {@link Timestamp} value from memory.
     *
     * @param pageAddr Address where page data starts.
     * @param offset Offset to the timestamp value relative to pageAddr.
     */
    static @Nullable Timestamp readTimestamp(long pageAddr, int offset) {
        long nodeId = getLong(pageAddr, offset);
        long localTimestamp = getLong(pageAddr, offset + Long.BYTES);

        Timestamp timestamp = new Timestamp(localTimestamp, nodeId);
        if (timestamp.equals(RowVersion.NULL_TIMESTAMP)) {
            timestamp = null;
        }

        return timestamp;
    }

    /**
     * Writes a {@link Timestamp} to memory starting at the given address + offset.
     *
     * @param addr Memory address.
     * @param offset Offset added to the address.
     * @param timestamp The timestamp to write.
     * @return Number of bytes written.
     */
    public static int writeTimestampToMemory(long addr, int offset, @Nullable Timestamp timestamp) {
        Timestamp timestampForStorage = RowVersion.timestampForStorage(timestamp);

        putLong(addr, offset, timestampForStorage.getNodeId());
        putLong(addr, offset + Long.BYTES, timestampForStorage.getTimestamp());

        return 2 * Long.BYTES;
    }

    /**
     * Writes a {@link Timestamp} to a buffer.
     *
     * @param buffer Buffer to which to write.
     * @param timestamp The timestamp to write.
     */
    public static void writeTimestampToBuffer(ByteBuffer buffer, @Nullable Timestamp timestamp) {
        Timestamp timestampForStorage = RowVersion.timestampForStorage(timestamp);

        buffer.putLong(timestampForStorage.getNodeId());
        buffer.putLong(timestampForStorage.getTimestamp());
    }
}
