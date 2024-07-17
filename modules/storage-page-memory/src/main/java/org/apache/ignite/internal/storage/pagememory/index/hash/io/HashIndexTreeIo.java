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

package org.apache.ignite.internal.storage.pagememory.index.hash.io;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getBytes;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getShort;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putByteBuffer;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.readPartitionless;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.canFullyInline;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.pagememory.index.InlineUtils;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.ReadIndexColumnsValue;
import org.apache.ignite.internal.storage.pagememory.index.hash.CompareIndexColumnsValue;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexRow;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexRowKey;

/**
 * Interface for {@link HashIndexRow} B+Tree-related IO.
 *
 * <p>Defines a following data layout:
 * <ul>
 *     <li>Index columns hash - int (4 bytes);</li>
 *     <li>Inlined index columns size - short (2 bytes), no more than the {@link InlineUtils#MAX_BINARY_TUPLE_INLINE_SIZE}, if positive
 *     then the index columns are fully inlined and this is their size, otherwise {@link #NOT_FULLY_INLINE} and their size is
 *     {@link #indexColumnsInlineSize()} and to get the index columns you need to use the link;</li>
 *     <li>Inlined index columns - N bytes;</li>
 *     <li>Index columns link - 6 bytes, if the index columns can be completely inlined, then those 6 bytes will be reused;</li>
 *     <li>Row ID - {@link UUID} (16 bytes).</li>
 * </ul>
 */
public interface HashIndexTreeIo {
    /** Item size without index columns in bytes. */
    int ITEM_SIZE_WITHOUT_COLUMNS = Integer.BYTES // Index columns hash.
            + Short.BYTES // Inlined index columns size.
            + PARTITIONLESS_LINK_SIZE_BYTES // Index columns link.
            + 2 * Long.BYTES; // Row ID.

    /** Special value that is written to the Index Key, indicating the value has not been fully inlined into it. */
    short NOT_FULLY_INLINE = -1;

    /** Offset of the index columns hash (4 bytes). */
    int HASH_OFFSET = 0;

    /** Offset of the index columns size (2 bytes). */
    int SIZE_OFFSET = HASH_OFFSET + Integer.BYTES;

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
     * Stores a hash index row, copied from another page.
     *
     * @see BplusIo#store(long, int, BplusIo, long, int)
     */
    default void store(long dstPageAddr, int dstIdx, BplusIo<HashIndexRowKey> srcIo, long srcPageAddr, int srcIdx) {
        int dstOffset = offset(dstIdx);
        int srcOffset = srcIo.offset(srcIdx);

        PageUtils.copyMemory(srcPageAddr, srcOffset, dstPageAddr, dstOffset, getItemSize());
    }

    /**
     * Stores a hash index row in the page.
     *
     * @see BplusIo#storeByOffset(long, int, Object)
     */
    default void storeByOffset(long pageAddr, final int off, HashIndexRowKey rowKey) {
        assert rowKey instanceof HashIndexRow;

        HashIndexRow row = (HashIndexRow) rowKey;

        putInt(pageAddr + off, HASH_OFFSET, row.indexColumnsHash());

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

        HashIndexRow row = (HashIndexRow) rowKey;

        final int off = offset(idx);

        int cmp = Integer.compare(getInt(pageAddr + off, HASH_OFFSET), row.indexColumnsHash());

        if (cmp != 0) {
            return cmp;
        }

        int indexColumnsSize = getShort(pageAddr + off, SIZE_OFFSET);

        if (indexColumnsSize == NOT_FULLY_INLINE) {
            indexColumnsSize = indexColumnsInlineSize();

            ByteBuffer indexColumnsBuffer = wrapPointer(pageAddr + off + TUPLE_OFFSET, indexColumnsSize);

            cmp = indexColumnsBuffer.compareTo(row.indexColumns().valueBuffer().rewind().duplicate().limit(indexColumnsSize));

            if (cmp != 0) {
                return cmp;
            }

            long link = readPartitionless(partitionId, pageAddr + off, linkOffset());

            CompareIndexColumnsValue compareIndexColumnsValue = new CompareIndexColumnsValue();

            dataPageReader.traverse(link, compareIndexColumnsValue, row.indexColumns().valueBuffer().rewind().duplicate());

            cmp = compareIndexColumnsValue.compareResult();
        } else {
            ByteBuffer indexColumnsBuffer = wrapPointer(pageAddr + off + TUPLE_OFFSET, indexColumnsSize);

            cmp = indexColumnsBuffer.compareTo(row.indexColumns().valueBuffer().rewind());
        }

        if (cmp != 0) {
            return cmp;
        }

        long rowIdMsb = getLong(pageAddr + off, rowIdMsbOffset());

        cmp = Long.compare(rowIdMsb, row.rowId().mostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        long rowIdLsb = getLong(pageAddr + off, rowIdLsbOffset());

        return Long.compare(rowIdLsb, row.rowId().leastSignificantBits());
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
        final int off = offset(idx);

        int hash = getInt(pageAddr + off, HASH_OFFSET);

        int indexColumnsSize = getShort(pageAddr + off, SIZE_OFFSET);

        byte[] indexColumnsBytes;

        long link;

        if (indexColumnsSize == NOT_FULLY_INLINE) {
            link = readPartitionless(partitionId, pageAddr + off, linkOffset());

            ReadIndexColumnsValue indexColumnsTraversal = new ReadIndexColumnsValue();

            dataPageReader.traverse(link, indexColumnsTraversal, null);

            indexColumnsBytes = indexColumnsTraversal.result();
        } else {
            indexColumnsBytes = getBytes(pageAddr + off, TUPLE_OFFSET, indexColumnsSize);

            link = NULL_LINK;
        }

        IndexColumns indexColumns = new IndexColumns(partitionId, link, ByteBuffer.wrap(indexColumnsBytes).order(LITTLE_ENDIAN));

        long rowIdMsb = getLong(pageAddr + off, rowIdMsbOffset());
        long rowIdLsb = getLong(pageAddr + off, rowIdLsbOffset());

        RowId rowId = new RowId(partitionId, rowIdMsb, rowIdLsb);

        return new HashIndexRow(hash, indexColumns, rowId);
    }

    /**
     * Returns the inline size for index columns in bytes.
     */
    default int indexColumnsInlineSize() {
        return getItemSize() - ITEM_SIZE_WITHOUT_COLUMNS;
    }
}
