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

package org.apache.ignite.internal.storage.pagememory.index.sorted.io;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getBytes;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getShort;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putByteBuffer;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.readPartitionless;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.canFullyInline;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.PartialBinaryTupleMatcher;
import org.apache.ignite.internal.schema.UnsafeByteBufferAccessor;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.ReadIndexColumnsValue;
import org.apache.ignite.internal.storage.pagememory.index.sorted.SortedIndexRow;
import org.apache.ignite.internal.storage.pagememory.index.sorted.SortedIndexRowKey;
import org.apache.ignite.internal.storage.pagememory.index.sorted.comparator.JitComparator;

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
    /** Item size without index columns in bytes. */
    int ITEM_SIZE_WITHOUT_COLUMNS = Short.BYTES // Inlined index columns size.
            + PARTITIONLESS_LINK_SIZE_BYTES // Index columns link.
            + 2 * Long.BYTES; // Row ID.

    /** Special value that is written to the Index Key, indicating the value has not been fully inlined into it. */
    short NOT_FULLY_INLINE = -1;

    /** Offset of the index columns size (2 bytes). */
    int SIZE_OFFSET = 0;

    /** Offset of the index columns tuple (N bytes). */
    int TUPLE_OFFSET = SIZE_OFFSET + Short.BYTES;

    /**
     * Returns offset of the index columns link (6 bytes).
     */
    default int linkOffset() {
        return TUPLE_OFFSET + indexColumnsInlineSize();
    }

    /**
     * Returns offset of rowId's the most significant bits (8 bytes).
     */
    default int rowIdMsbOffset() {
        return linkOffset() + PARTITIONLESS_LINK_SIZE_BYTES;
    }

    /**
     * Returns offset of rowId's least significant bits (8 bytes).
     */
    default int rowIdLsbOffset() {
        return rowIdMsbOffset() + Long.BYTES;
    }

    /**
     * Returns item size in bytes.
     *
     * @see BplusIo#getItemSize()
     */
    int getItemSize();

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
        int srcOffset = srcIo.offset(srcIdx);

        PageUtils.copyMemory(srcPageAddr, srcOffset, dstPageAddr, dstOffset, getItemSize());
    }

    /**
     * Stores a sorted index row in the page.
     *
     * @see BplusIo#storeByOffset(long, int, Object)
     */
    default void storeByOffset(long pageAddr, final int off, SortedIndexRowKey rowKey) {
        assert rowKey instanceof SortedIndexRow;

        SortedIndexRow row = (SortedIndexRow) rowKey;

        IndexColumns indexColumns = row.indexColumns();

        if (canFullyInline(indexColumns.valueSize(), indexColumnsInlineSize())) {
            assert indexColumns.link() == NULL_LINK : "Index columns are completely inline, they should not be in FreeList";

            putShort(pageAddr + off, SIZE_OFFSET, (short) indexColumns.valueSize());

            putByteBuffer(pageAddr + off, TUPLE_OFFSET, indexColumns.valueBuffer().rewind());
        } else {
            putShort(pageAddr + off, SIZE_OFFSET, NOT_FULLY_INLINE);

            ByteBuffer bufferToWrite = indexColumns.valueBuffer().rewind().duplicate().limit(indexColumnsInlineSize());

            putByteBuffer(pageAddr + off, TUPLE_OFFSET, bufferToWrite);

            writePartitionless(pageAddr + off + linkOffset(), indexColumns.link());
        }

        RowId rowId = row.rowId();

        putLong(pageAddr + off, rowIdMsbOffset(), rowId.mostSignificantBits());
        putLong(pageAddr + off, rowIdLsbOffset(), rowId.leastSignificantBits());
    }

    /**
     * Compare the {@link SortedIndexRowKey} from the page with passed {@link SortedIndexRowKey}.
     *
     * @param dataPageReader Data page reader.
     * @param binaryTupleComparator Comparator of index columns {@link BinaryTuple}s.
     * @param partialBinaryTupleComparator Matcher partial data of index columns {@link BinaryTuple}s.
     * @param partitionId Partition ID.
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param rowKey Lookup index row key.
     * @return Comparison result.
     * @throws IgniteInternalCheckedException If failed.
     */
    default int compare(
            DataPageReader dataPageReader,
            Comparator<ByteBuffer> binaryTupleComparator,
            PartialBinaryTupleMatcher partialBinaryTupleComparator,
            int partitionId,
            long pageAddr,
            int idx,
            SortedIndexRowKey rowKey
    ) throws IgniteInternalCheckedException {
        final int off = offset(idx);

        int indexColumnsSize = getShort(pageAddr + off, SIZE_OFFSET);

        ByteBuffer firstBinaryTupleBuffer;

        ByteBuffer secondBinaryTupleBuffer = rowKey.indexColumns().valueBuffer();

        if (indexColumnsSize == NOT_FULLY_INLINE) {
            ByteBuffer partialFirstBinaryTupleBuffer = wrapPointer(pageAddr + off + TUPLE_OFFSET, indexColumnsInlineSize());

            int firstCmp = partialBinaryTupleComparator.match(
                    partialFirstBinaryTupleBuffer.order(LITTLE_ENDIAN),
                    secondBinaryTupleBuffer
            );

            if (firstCmp != 0) {
                return firstCmp;
            }

            long link = readPartitionless(partitionId, pageAddr + off, linkOffset());

            ReadIndexColumnsValue indexColumnsTraversal = new ReadIndexColumnsValue();

            dataPageReader.traverse(link, indexColumnsTraversal, null);

            firstBinaryTupleBuffer = ByteBuffer.wrap(indexColumnsTraversal.result());
        } else {
            firstBinaryTupleBuffer = wrapPointer(pageAddr + off + TUPLE_OFFSET, indexColumnsSize);
        }

        int cmp = binaryTupleComparator.compare(firstBinaryTupleBuffer.order(LITTLE_ENDIAN), secondBinaryTupleBuffer);

        if (cmp != 0) {
            return cmp;
        }

        return compareRowId(pageAddr, rowKey, off);
    }

    /**
     * Compare the {@link SortedIndexRowKey} from the page with passed {@link SortedIndexRowKey}. This method is very similar to
     * {@link #compare(DataPageReader, Comparator, PartialBinaryTupleMatcher, int, long, int, SortedIndexRowKey)}. Combining them into a
     * single method would make it too bloated and hard to follow, assuming an optimal implementation of both methods.
     *
     * @param dataPageReader Data page reader.
     * @param comparator Comparator.
     * @param partitionId Partition ID.
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param rowKey Lookup index row key.
     * @return Comparison result.
     * @throws IgniteInternalCheckedException If failed.
     */
    default int compare(
            DataPageReader dataPageReader,
            JitComparator comparator,
            int partitionId,
            long pageAddr,
            int idx,
            SortedIndexRowKey rowKey
    ) throws IgniteInternalCheckedException {
        int off = offset(idx);

        int indexColumnsSize = getShort(pageAddr + off, SIZE_OFFSET);

        UnsafeByteBufferAccessor outerAccessor = rowKey.myAccessor;
        UnsafeByteBufferAccessor innerAccessor = rowKey.otherAccessor;
        int innerSize;

        if (indexColumnsSize == NOT_FULLY_INLINE) {
            innerSize = indexColumnsInlineSize();
            innerAccessor.reinit(pageAddr + off + TUPLE_OFFSET, innerSize);

            int cmp = comparator.compare(outerAccessor, outerAccessor.capacity(), innerAccessor, innerSize);

            if (cmp != 0) {
                return -cmp;
            }

            long link = readPartitionless(partitionId, pageAddr + off, linkOffset());

            ReadIndexColumnsValue indexColumnsTraversal = new ReadIndexColumnsValue();

            dataPageReader.traverse(link, indexColumnsTraversal, null);

            byte[] innerBytes = indexColumnsTraversal.result();
            innerSize = innerBytes.length;
            innerAccessor.reinit(innerBytes, 0, innerBytes.length);
        } else {
            innerSize = indexColumnsSize;
            innerAccessor.reinit(pageAddr + off + TUPLE_OFFSET, indexColumnsSize);
        }

        int cmp = comparator.compare(outerAccessor, outerAccessor.capacity(), innerAccessor, innerSize);

        if (cmp != 0) {
            return -cmp;
        }

        return compareRowId(pageAddr, rowKey, off);
    }

    private int compareRowId(long pageAddr, SortedIndexRowKey rowKey, int off) {
        assert rowKey instanceof SortedIndexRow : "Comparison with a binary tuple prefix returned 0. [rowKey=" + rowKey.getClass() + "]";

        SortedIndexRow row = (SortedIndexRow) rowKey;

        long rowIdMsb = getLong(pageAddr + off, rowIdMsbOffset());

        int cmp = Long.compare(rowIdMsb, row.rowId().mostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        long rowIdLsb = getLong(pageAddr + off, rowIdLsbOffset());

        return Long.compare(rowIdLsb, row.rowId().leastSignificantBits());
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
        final int off = offset(idx);

        int indexColumnsSize = getShort(pageAddr + off, SIZE_OFFSET);

        ByteBuffer indexColumnsBuffer;

        long link;

        if (indexColumnsSize == NOT_FULLY_INLINE) {
            link = readPartitionless(partitionId, pageAddr + off, linkOffset());

            ReadIndexColumnsValue indexColumnsTraversal = new ReadIndexColumnsValue();

            dataPageReader.traverse(link, indexColumnsTraversal, null);

            indexColumnsBuffer = ByteBuffer.wrap(indexColumnsTraversal.result());
        } else {
            indexColumnsBuffer = ByteBuffer.wrap(getBytes(pageAddr + off, TUPLE_OFFSET, indexColumnsSize));

            link = NULL_LINK;
        }

        IndexColumns indexColumns = new IndexColumns(partitionId, link, indexColumnsBuffer.order(LITTLE_ENDIAN));

        long rowIdMsb = getLong(pageAddr + off, rowIdMsbOffset());
        long rowIdLsb = getLong(pageAddr + off, rowIdLsbOffset());

        RowId rowId = new RowId(partitionId, rowIdMsb, rowIdLsb);

        return new SortedIndexRow(indexColumns, rowId);
    }

    /**
     * Returns the inline size for index columns in bytes.
     */
    default int indexColumnsInlineSize() {
        return getItemSize() - ITEM_SIZE_WITHOUT_COLUMNS;
    }
}
