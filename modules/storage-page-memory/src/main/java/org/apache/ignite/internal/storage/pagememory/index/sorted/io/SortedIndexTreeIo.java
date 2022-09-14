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

package org.apache.ignite.internal.storage.pagememory.index.sorted.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
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
import org.apache.ignite.internal.storage.pagememory.index.sorted.SortedIndexRow;
import org.apache.ignite.internal.storage.pagememory.index.sorted.SortedIndexRowKey;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Interface for {@link SortedIndexRow} B+Tree-related IO.
 *
 * <p>Defines a following data layout:
 * <ul>
 *     <li>Index columns link - long (6 bytes);</li>
 *     <li>Row ID - {@link UUID} (16 bytes).</li>
 * </ul>
 */
public interface SortedIndexTreeIo {
    /** Offset of the index column link (6 bytes). */
    int INDEX_COLUMNS_LINK_OFFSET = 0;

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
     * Stores a sorted index row, copied from another page.
     *
     * @see BplusIo#store(long, int, BplusIo, long, int)
     */
    default void store(long dstPageAddr, int dstIdx, BplusIo<SortedIndexRowKey> srcIo, long srcPageAddr, int srcIdx) {
        int dstOffset = offset(dstIdx);
        int srcOffset = offset(srcIdx);

        PageUtils.copyMemory(srcPageAddr, srcOffset, dstPageAddr, dstOffset, SIZE_IN_BYTES);
    }

    /**
     * Stores a sorted index row in the page.
     *
     * @see BplusIo#storeByOffset(long, int, Object)
     */
    default void storeByOffset(long pageAddr, int off, SortedIndexRowKey rowKey) {
        assert rowKey instanceof SortedIndexRow;

        SortedIndexRow sortedIndexRow = (SortedIndexRow) rowKey;

        writePartitionlessLink(pageAddr + off + INDEX_COLUMNS_LINK_OFFSET, sortedIndexRow.indexColumns().link());

        RowId rowId = sortedIndexRow.rowId();

        putLong(pageAddr, off + ROW_ID_MSB_OFFSET, rowId.mostSignificantBits());
        putLong(pageAddr, off + ROW_ID_LSB_OFFSET, rowId.leastSignificantBits());
    }

    /**
     * Compare the {@link SortedIndexRowKey} from the page with passed {@link SortedIndexRowKey}.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param rowKey Lookup index row key.
     * @return Comparison result.
     */
    default int compare(DataPageReader dataPageReader, int partitionId, long pageAddr, int idx, SortedIndexRowKey rowKey)
            throws IgniteInternalCheckedException {
        assert rowKey instanceof SortedIndexRow;

        SortedIndexRow sortedIndexRow = (SortedIndexRow) rowKey;

        int off = offset(idx);

        long link = readPartitionlessLink(partitionId, pageAddr, off + INDEX_COLUMNS_LINK_OFFSET);

        //TODO Add in-place compare in IGNITE-17671
        ReadIndexColumnsValue indexColumnsTraversal = new ReadIndexColumnsValue();

        dataPageReader.traverse(link, indexColumnsTraversal, null);

        ByteBuffer indexColumnsBuffer = ByteBuffer.wrap(indexColumnsTraversal.result());

        // TODO: IGNITE-17672 Compare by BinaryTuple
        int cmp = indexColumnsBuffer.compareTo(sortedIndexRow.indexColumns().valueBuffer());

        if (cmp != 0) {
            return cmp;
        }

        long rowIdMsb = getLong(pageAddr, off + ROW_ID_MSB_OFFSET);

        cmp = Long.compare(rowIdMsb, sortedIndexRow.rowId().mostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        long rowIdLsb = getLong(pageAddr, off + ROW_ID_LSB_OFFSET);

        return Long.compare(rowIdLsb, sortedIndexRow.rowId().leastSignificantBits());
    }

    /**
     * Reads a sorted index row value.
     *
     * @param dataPageReader Data page reader instance to read payload from data pages.
     * @param partitionId Partition id.
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @return Sorted index row.
     * @throws IgniteInternalCheckedException If failed to read payload from data pages.
     */
    default SortedIndexRow getRow(DataPageReader dataPageReader, int partitionId, long pageAddr, int idx)
            throws IgniteInternalCheckedException {
        int off = offset(idx);

        long link = readPartitionlessLink(partitionId, pageAddr, off + INDEX_COLUMNS_LINK_OFFSET);

        ReadIndexColumnsValue indexColumnsTraversal = new ReadIndexColumnsValue();

        dataPageReader.traverse(link, indexColumnsTraversal, null);

        ByteBuffer indexColumnsBuffer = ByteBuffer.wrap(indexColumnsTraversal.result());

        IndexColumns indexColumns = new IndexColumns(partitionId, link, indexColumnsBuffer);

        long rowIdMsb = getLong(pageAddr, off + ROW_ID_MSB_OFFSET);
        long rowIdLsb = getLong(pageAddr, off + ROW_ID_LSB_OFFSET);

        RowId rowId = new RowId(partitionId, rowIdMsb, rowIdLsb);

        return new SortedIndexRow(indexColumns, rowId);
    }
}
