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
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.pagememory.index.InlineUtils;
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
 *     <li>Inlined index columns size - short (2 bytes), no more than the {@link InlineUtils#MAX_BINARY_TUPLE_INLINE_SIZE}, the most
 *     significant (sign) bit is used to determine whether there is a link to the index columns;</li>
 *     <li>Inlined index columns - N bytes;</li>
 *     <li>Index columns link (optional)- 6 bytes, if the inlined index columns are the same size as the original index columns, no link is
 *     needed;</li>
 *     <li>Row ID - {@link UUID} (16 bytes).</li>
 * </ul>
 */
public interface HashIndexTreeIo {
    /** Item size without index columns in bytes. */
    int ITEM_SIZE_WITHOUT_COLUMNS = Integer.BYTES // Index columns hash.
            + Short.SIZE // Inlined index columns size.
            + PARTITIONLESS_LINK_SIZE_BYTES // Index column link.
            + 2 * Long.BYTES; // Row ID.

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
        int srcOffset = offset(srcIdx);

        PageUtils.copyMemory(srcPageAddr, srcOffset, dstPageAddr, dstOffset, getItemSize());
    }

    /**
     * Stores a hash index row in the page.
     *
     * @see BplusIo#storeByOffset(long, int, Object)
     */
    default void storeByOffset(long pageAddr, int off, HashIndexRowKey rowKey) {
        assert rowKey instanceof HashIndexRow;

        HashIndexRow row = (HashIndexRow) rowKey;

        putInt(pageAddr, off, row.indexColumnsHash());
        off += Integer.BYTES;

        IndexColumns indexColumns = row.indexColumns();

        if (indexColumns.valueSize() <= indexColumnsInlineSize() + PARTITIONLESS_LINK_SIZE_BYTES) {
            // TODO: IGNITE-17536 вот тут надо будет проверить что линки нет!

            putShort(pageAddr, off, (short) -indexColumns.valueSize());
            off += Short.BYTES;

            putByteBuffer(pageAddr, off, indexColumns.valueBuffer().rewind());

            off += indexColumns.valueSize();
        } else {
            putShort(pageAddr, off, (short) indexColumnsInlineSize());
            off += Short.BYTES;

            putByteBuffer(pageAddr, off, indexColumns.valueBuffer().rewind().slice().limit(indexColumnsInlineSize()));
            off += indexColumnsInlineSize();

            writePartitionless(pageAddr + off, indexColumns.link());
            off += PARTITIONLESS_LINK_SIZE_BYTES;
        }

        RowId rowId = row.rowId();

        putLong(pageAddr, off, rowId.mostSignificantBits());
        off += Long.BYTES;

        putLong(pageAddr, off, rowId.leastSignificantBits());
        off += Long.BYTES;
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

        int off = offset(idx);

        int cmp = Integer.compare(getInt(pageAddr, off), row.indexColumnsHash());
        off += Integer.BYTES;

        if (cmp != 0) {
            return cmp;
        }

        short indexColumnsSize = getShort(pageAddr, off);
        off += Short.BYTES;

        if (indexColumnsSize < 0) {
            ByteBuffer indexColumnsBuffer = wrapPointer(pageAddr + off, -indexColumnsSize);
            off += -indexColumnsSize;

            cmp = indexColumnsBuffer.compareTo(row.indexColumns().valueBuffer());

            if (cmp != 0) {
                return cmp;
            }
        } else {
            long link = readPartitionless(partitionId, pageAddr, off += indexColumnsSize);
            off += PARTITIONLESS_LINK_SIZE_BYTES;

            //TODO Add in-place compare in IGNITE-17536
            ReadIndexColumnsValue indexColumnsTraversal = new ReadIndexColumnsValue();

            dataPageReader.traverse(link, indexColumnsTraversal, null);

            ByteBuffer indexColumnsBuffer = ByteBuffer.wrap(indexColumnsTraversal.result());

            cmp = indexColumnsBuffer.compareTo(row.indexColumns().valueBuffer());

            if (cmp != 0) {
                return cmp;
            }
        }

        long rowIdMsb = getLong(pageAddr, off);
        off += Long.BYTES;

        cmp = Long.compare(rowIdMsb, row.rowId().mostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        long rowIdLsb = getLong(pageAddr, off);
        off += Long.BYTES;

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
        int off = offset(idx);

        int hash = getInt(pageAddr, off);
        off += Integer.BYTES;

        short indexColumnsSize = getShort(pageAddr, off);
        off += Short.BYTES;

        ByteBuffer indexColumnsBuffer;

        long link;

        if (indexColumnsSize < 0) {
            indexColumnsBuffer = ByteBuffer.wrap(getBytes(pageAddr, off, -indexColumnsSize));
            off += -indexColumnsSize;

            link = NULL_LINK;
        } else {
            link = readPartitionless(partitionId, pageAddr, off += indexColumnsSize);
            off += PARTITIONLESS_LINK_SIZE_BYTES;

            ReadIndexColumnsValue indexColumnsTraversal = new ReadIndexColumnsValue();

            dataPageReader.traverse(link, indexColumnsTraversal, null);

            indexColumnsBuffer = ByteBuffer.wrap(indexColumnsTraversal.result());
        }

        IndexColumns indexColumns = new IndexColumns(partitionId, link, indexColumnsBuffer);

        long rowIdMsb = getLong(pageAddr, off);
        off += Long.BYTES;

        long rowIdLsb = getLong(pageAddr, off);
        off += Long.BYTES;

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
