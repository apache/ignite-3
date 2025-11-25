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

package org.apache.ignite.internal.storage.pagememory.mv.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getShort;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.readPartitionless;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;
import static org.apache.ignite.internal.storage.pagememory.mv.VersionChain.NULL_UUID_COMPONENT;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChain;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainKey;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainTree;

/**
 * Interface for VersionChain B+Tree-related IO. Defines a following data layout:
 * <pre><code>
 * [rowId's UUID (16 bytes), txId (16 bytes), commitTableId (16 bytes), commitPartitionId (2 bytes), head link (6 bytes),
 * next link (6 bytes)]
 * </code></pre>
 */
public interface VersionChainIo {
    /** Offset of rowId's most significant bits, 8 bytes. */
    int ROW_ID_MSB_OFFSET = 0;

    /** Offset of rowId's least significant bits, 8 bytes. */
    int ROW_ID_LSB_OFFSET = ROW_ID_MSB_OFFSET + Long.BYTES;

    /** Offset of txId's most significant bits, 8 bytes. */
    int TX_ID_MSB_OFFSET = ROW_ID_LSB_OFFSET + Long.BYTES;

    /** Offset of txId's least significant bits, 8 bytes. */
    int TX_ID_LSB_OFFSET = TX_ID_MSB_OFFSET + Long.BYTES;

    /** Offset of commit table id. */
    int COMMIT_TABLE_ID = TX_ID_LSB_OFFSET + Long.BYTES;

    /** Offset of commit partition id. */
    int COMMIT_PARTITION_ID_OFFSET = COMMIT_TABLE_ID + Integer.BYTES;

    /** Offset of partitionless head link, 6 bytes. */
    int HEAD_LINK_OFFSET = COMMIT_PARTITION_ID_OFFSET + Short.BYTES;

    /** Offset of partitionless next link, 6 bytes. */
    int NEXT_LINK_OFFSET = HEAD_LINK_OFFSET + PARTITIONLESS_LINK_SIZE_BYTES;

    /** Payload size in bytes. */
    int SIZE_IN_BYTES = NEXT_LINK_OFFSET + PARTITIONLESS_LINK_SIZE_BYTES;

    /**
     * Returns an offset of the element inside the page.
     *
     * @see BplusIo#offset(int)
     */
    int offset(int idx);

    /**
     * Stores a version chain, copied from another page.
     *
     * @see BplusIo#store(long, int, BplusIo, long, int)
     */
    default void store(long dstPageAddr, int dstIdx, BplusIo<VersionChainKey> srcIo, long srcPageAddr, int srcIdx) {
        int dstOffset = offset(dstIdx);
        int srcOffset = srcIo.offset(srcIdx);

        PageUtils.copyMemory(srcPageAddr, srcOffset, dstPageAddr, dstOffset, SIZE_IN_BYTES);
    }

    /**
     * Stores a version chain in the page.
     *
     * @see BplusIo#storeByOffset(long, int, Object)
     */
    default void storeByOffset(long pageAddr, int off, VersionChainKey rowKey) {
        assert rowKey instanceof VersionChain;

        VersionChain row = (VersionChain) rowKey;

        RowId rowId = row.rowId();

        putLong(pageAddr, off + ROW_ID_MSB_OFFSET, rowId.mostSignificantBits());
        putLong(pageAddr, off + ROW_ID_LSB_OFFSET, rowId.leastSignificantBits());

        UUID txId = row.transactionId();
        Integer commitTableId = row.commitZoneId();
        int commitPartitionId = row.commitPartitionId();

        if (txId == null) {
            assert commitTableId == null;
            assert commitPartitionId == -1;

            putLong(pageAddr, off + TX_ID_MSB_OFFSET, NULL_UUID_COMPONENT);
            putLong(pageAddr, off + TX_ID_LSB_OFFSET, NULL_UUID_COMPONENT);
        } else {
            assert commitTableId != null;
            assert commitPartitionId >= 0;

            putLong(pageAddr, off + TX_ID_MSB_OFFSET, txId.getMostSignificantBits());
            putLong(pageAddr, off + TX_ID_LSB_OFFSET, txId.getLeastSignificantBits());

            putInt(pageAddr, off + COMMIT_TABLE_ID, commitTableId);
            putShort(pageAddr, off + COMMIT_PARTITION_ID_OFFSET, (short) commitPartitionId);
        }

        writePartitionless(pageAddr + off + HEAD_LINK_OFFSET, row.headLink());
        writePartitionless(pageAddr + off + NEXT_LINK_OFFSET, row.nextLink());
    }

    /**
     * Compare the version chain from the page with passed version chain, thus defining the order of element in the
     * {@link VersionChainTree}.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param versionChain Version chain.
     * @return Comparison result.
     */
    default int compare(long pageAddr, int idx, VersionChainKey versionChain) {
        RowId rowId = versionChain.rowId();

        int offset = offset(idx);

        int cmp = Long.compare(getLong(pageAddr, offset + ROW_ID_MSB_OFFSET), rowId.mostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        return Long.compare(getLong(pageAddr, offset + ROW_ID_LSB_OFFSET), rowId.leastSignificantBits());
    }

    /**
     * Reads a version chain from the page.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param partitionId Partition id to enrich read partitionless links.
     * @return Version chain.
     */
    default VersionChain getRow(long pageAddr, int idx, int partitionId) {
        int offset = offset(idx);

        long rowIdMsb = getLong(pageAddr, offset + ROW_ID_MSB_OFFSET);
        long rowIdLsb = getLong(pageAddr, offset + ROW_ID_LSB_OFFSET);

        RowId rowId = new RowId(partitionId, rowIdMsb, rowIdLsb);

        long txIdMsb = getLong(pageAddr, offset + TX_ID_MSB_OFFSET);
        long txIdLsb = getLong(pageAddr, offset + TX_ID_LSB_OFFSET);

        UUID txId = (txIdMsb == NULL_UUID_COMPONENT && txIdLsb == NULL_UUID_COMPONENT) ? null : new UUID(txIdMsb, txIdLsb);

        long headLink = readPartitionless(partitionId, pageAddr, offset + HEAD_LINK_OFFSET);
        long nextLink = readPartitionless(partitionId, pageAddr, offset + NEXT_LINK_OFFSET);

        if (txId != null) {
            int commitTableId = getInt(pageAddr, offset + COMMIT_TABLE_ID);

            int commitPartitionId = getShort(pageAddr, offset + COMMIT_PARTITION_ID_OFFSET) & 0xFFFF;

            return VersionChain.createUncommitted(rowId, txId, commitTableId, commitPartitionId, headLink, nextLink);
        }

        return VersionChain.createCommitted(rowId, headLink, nextLink);
    }
}
