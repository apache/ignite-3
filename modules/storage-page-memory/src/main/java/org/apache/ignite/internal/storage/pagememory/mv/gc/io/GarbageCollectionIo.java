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

package org.apache.ignite.internal.storage.pagememory.mv.gc.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.pagememory.mv.HybridTimestamps;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GarbageCollectionRowVersion;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GarbageCollectionTree;

/**
 * Interface for {@link GarbageCollectionTree}-related IO.
 *
 * <p>Defines a following data layout:
 * <ul>
 *     <li>Row ID (16 bytes);</li>
 *     <li>Row timestamp (12 bytes).</li>
 * </ul>
 */
public interface GarbageCollectionIo {
    /** Offset of rowId's most significant bits, 8 bytes. */
    int ROW_ID_MSB_OFFSET = 0;

    /** Offset of rowId's least significant bits, 8 bytes. */
    int ROW_ID_LSB_OFFSET = ROW_ID_MSB_OFFSET + Long.BYTES;

    /** Offset of row timestamp, 12 bytes. */
    int ROW_TIMESTAMP_OFFSET = ROW_ID_LSB_OFFSET + Long.BYTES;

    /** Payload size in bytes. */
    int SIZE_IN_BYTES = ROW_TIMESTAMP_OFFSET + HybridTimestamps.SIZE_IN_BYTES;

    /**
     * Returns an offset of the element inside the page.
     *
     * @see BplusIo#offset(int)
     */
    int offset(int idx);

    /**
     * Stores a row version for garbage collection, copied from another page.
     *
     * @see BplusIo#store(long, int, BplusIo, long, int)
     */
    default void store(long dstPageAddr, int dstIdx, BplusIo<GarbageCollectionRowVersion> srcIo, long srcPageAddr, int srcIdx) {
        int dstOffset = offset(dstIdx);
        int srcOffset = offset(srcIdx);

        PageUtils.copyMemory(srcPageAddr, srcOffset, dstPageAddr, dstOffset, SIZE_IN_BYTES);
    }

    /**
     * Stores a row version for garbage collection chain in the page.
     *
     * @see BplusIo#storeByOffset(long, int, Object)
     */
    default void storeByOffset(long pageAddr, int off, GarbageCollectionRowVersion row) {
        RowId rowId = row.getRowId();

        putLong(pageAddr, off + ROW_ID_MSB_OFFSET, rowId.mostSignificantBits());
        putLong(pageAddr, off + ROW_ID_LSB_OFFSET, rowId.leastSignificantBits());

        HybridTimestamps.writeTimestampToMemory(pageAddr, off + ROW_TIMESTAMP_OFFSET, row.getTimestamp());
    }

    /**
     * Compare the row version for garbage collection from the page with passed row version, thus defining the order of element in the
     * {@link GarbageCollectionTree}.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param rowVersion Row version for garbage collection.
     * @return Comparison result.
     */
    default int compare(long pageAddr, int idx, GarbageCollectionRowVersion rowVersion) {
        int offset = offset(idx);

        int cmp = HybridTimestamps.readTimestamp(pageAddr, offset + ROW_TIMESTAMP_OFFSET).compareTo(rowVersion.getTimestamp());

        if (cmp != 0) {
            return cmp;
        }

        RowId rowId = rowVersion.getRowId();

        cmp = Long.compare(getLong(pageAddr, offset + ROW_ID_MSB_OFFSET), rowId.mostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        return Long.compare(getLong(pageAddr, offset + ROW_ID_LSB_OFFSET), rowId.leastSignificantBits());
    }

    /**
     * Reads a row version for garbage collection from the page.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param partitionId Partition id to enrich read partitionless links.
     */
    default GarbageCollectionRowVersion getRow(long pageAddr, int idx, int partitionId) {
        int offset = offset(idx);

        long rowIdMsb = getLong(pageAddr, offset + ROW_ID_MSB_OFFSET);
        long rowIdLsb = getLong(pageAddr, offset + ROW_ID_LSB_OFFSET);

        return new GarbageCollectionRowVersion(
                new RowId(partitionId, rowIdMsb, rowIdLsb),
                HybridTimestamps.readTimestamp(pageAddr, offset + ROW_TIMESTAMP_OFFSET)
        );
    }
}
