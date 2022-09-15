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

package org.apache.ignite.internal.storage.pagememory.index.hash.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.readPartitionlessLink;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionlessLink;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.ReadIndexColumnsValue;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexRow;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexRowKey;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Interface for {@link HashIndexRow} B+Tree-related IO.
 *
 * <p>Defines a following data layout:
 * <ul>
 *     <li>Index columns hash - int (4 bytes);</li>
 *     <li>Index columns link - long (6 bytes);</li>
 *     <li>Row ID - {@link UUID} (16 bytes).</li>
 * </ul>
 */
public interface HashIndexTreeIo {
    /** Offset of the index columns hash (4 bytes). */
    int INDEX_COLUMNS_HASH_OFFSET = 0;

    /** Offset of the index column link (6 bytes). */
    int INDEX_COLUMNS_LINK_OFFSET = INDEX_COLUMNS_HASH_OFFSET + Integer.BYTES;

    /** Offset of rowId's most significant bits, 8 bytes. */
    int ROW_ID_MSB_OFFSET = INDEX_COLUMNS_LINK_OFFSET + PARTITIONLESS_LINK_SIZE_BYTES;

    /** Offset of rowId's least significant bits, 8 bytes. */
    int ROW_ID_LSB_OFFSET = ROW_ID_MSB_OFFSET + Long.BYTES;

    /** Payload size in bytes. */
    int SIZE_IN_BYTES = ROW_ID_LSB_OFFSET + Long.BYTES;

    /**
     * Returns an offset of the element inside the page.
     *
     * @see BplusIo#offset(int)
     */
    int offset(int idx);

    /**
     * Stores a hash index row, copied from another page.
     *
     * @see BplusIo#store(long, int, BplusIo, long, int)
     */
    default void store(long dstPageAddr, int dstIdx, BplusIo<HashIndexRowKey> srcIo, long srcPageAddr, int srcIdx) {
        int dstOffset = offset(dstIdx);
        int srcOffset = offset(srcIdx);

        PageUtils.copyMemory(srcPageAddr, srcOffset, dstPageAddr, dstOffset, SIZE_IN_BYTES);
    }

    /**
     * Stores a hash index row in the page.
     *
     * @see BplusIo#storeByOffset(long, int, Object)
     */
    default void storeByOffset(long pageAddr, int off, HashIndexRowKey rowKey) {
        assert rowKey instanceof HashIndexRow;

        HashIndexRow hashIndexRow = (HashIndexRow) rowKey;

        putInt(pageAddr, off + INDEX_COLUMNS_HASH_OFFSET, hashIndexRow.indexColumnsHash());

        writePartitionlessLink(pageAddr + off + INDEX_COLUMNS_LINK_OFFSET, hashIndexRow.indexColumns().link());

        RowId rowId = hashIndexRow.rowId();

        putLong(pageAddr, off + ROW_ID_MSB_OFFSET, rowId.mostSignificantBits());
        putLong(pageAddr, off + ROW_ID_LSB_OFFSET, rowId.leastSignificantBits());
    }

    /**
     * Compare the {@link HashIndexRowKey} from the page with passed {@link HashIndexRowKey}.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param rowKey Lookup index row key.
     * @return Comparison result.
     */
    default int compare(DataPageReader dataPageReader, int partitionId, long pageAddr, int idx, HashIndexRowKey rowKey)
            throws IgniteInternalCheckedException {
        assert rowKey instanceof HashIndexRow;

        HashIndexRow hashIndexRow = (HashIndexRow) rowKey;

        int off = offset(idx);

        int cmp = Integer.compare(getInt(pageAddr, off + INDEX_COLUMNS_HASH_OFFSET), hashIndexRow.indexColumnsHash());

        if (cmp != 0) {
            return cmp;
        }

        long link = readPartitionlessLink(partitionId, pageAddr, off + INDEX_COLUMNS_LINK_OFFSET);

        //TODO Add in-place compare in IGNITE-17536
        ReadIndexColumnsValue indexColumnsTraversal = new ReadIndexColumnsValue();

        dataPageReader.traverse(link, indexColumnsTraversal, null);

        ByteBuffer indexColumnsBuffer = ByteBuffer.wrap(indexColumnsTraversal.result());

        cmp = indexColumnsBuffer.compareTo(hashIndexRow.indexColumns().valueBuffer());

        if (cmp != 0) {
            return cmp;
        }

        long rowIdMsb = getLong(pageAddr, off + ROW_ID_MSB_OFFSET);

        cmp = Long.compare(rowIdMsb, hashIndexRow.rowId().mostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        long rowIdLsb = getLong(pageAddr, off + ROW_ID_LSB_OFFSET);

        return Long.compare(rowIdLsb, hashIndexRow.rowId().leastSignificantBits());
    }

    /**
     * Reads a hash index row value.
     *
     * @param dataPageReader Data page reader instance to read payload from data pages.
     * @param partitionId Partition id.
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @return Hash index row.
     * @throws IgniteInternalCheckedException If failed to read payload from data pages.
     */
    default HashIndexRow getRow(DataPageReader dataPageReader, int partitionId, long pageAddr, int idx)
            throws IgniteInternalCheckedException {
        int off = offset(idx);

        int hash = getInt(pageAddr, off + INDEX_COLUMNS_HASH_OFFSET);

        long link = readPartitionlessLink(partitionId, pageAddr, off + INDEX_COLUMNS_LINK_OFFSET);

        ReadIndexColumnsValue indexColumnsTraversal = new ReadIndexColumnsValue();

        dataPageReader.traverse(link, indexColumnsTraversal, null);

        ByteBuffer indexColumnsBuffer = ByteBuffer.wrap(indexColumnsTraversal.result());

        IndexColumns indexColumns = new IndexColumns(partitionId, link, indexColumnsBuffer);

        long rowIdMsb = getLong(pageAddr, off + ROW_ID_MSB_OFFSET);
        long rowIdLsb = getLong(pageAddr, off + ROW_ID_LSB_OFFSET);

        RowId rowId = new RowId(partitionId, rowIdMsb, rowIdLsb);

        return new HashIndexRow(hash, indexColumns, rowId);
    }
}
