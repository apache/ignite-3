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

package org.apache.ignite.internal.storage.pagememory.index.meta.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getByte;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putByte;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta.IndexType;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaKey;

/**
 * Interface for {@link IndexMeta} B+Tree-related IO.
 *
 * <p>Defines a following data layout:
 * <ul>
 *     <li>Index ID - int (4 bytes);</li>
 *     <li>Index root page ID - long (8 bytes);</li>
 *     <li>Row ID uuid for which the index needs to be built - {@link UUID} (16 bytes).</li>
 * </ul>
 */
public interface IndexMetaIo {
    /** Offset of the index ID (4 bytes). */
    int INDEX_ID_OFFSET = 0;

    /** Offset of the Index Type (1 byte). */
    int INDEX_TYPE_OFFSET = INDEX_ID_OFFSET + Integer.BYTES;

    /** Index tree meta page id offset - long (8 bytes). */
    int INDEX_TREE_META_PAGE_ID_OFFSET = INDEX_TYPE_OFFSET + Byte.BYTES;

    /**
     * Offset of the {@link UUID#getMostSignificantBits() most significant bits} of Row ID uuid for which the index needs to be built (8
     * bytes).
     */
    int NEXT_ROW_ID_TO_BUILT_MSB_OFFSET = INDEX_TREE_META_PAGE_ID_OFFSET + Long.BYTES;

    /**
     * Offset of the {@link UUID#getLeastSignificantBits() least significant bits} of Row ID uuid for which the index needs to be built (8
     * bytes).
     */
    int NEXT_ROW_ID_TO_BUILT_LSB_OFFSET = NEXT_ROW_ID_TO_BUILT_MSB_OFFSET + Long.BYTES;

    /** Payload size in bytes. */
    int SIZE_IN_BYTES = Integer.BYTES // Index ID - int (4 bytes)
            + Byte.BYTES // Index type - byte (1 byte)
            + Long.BYTES // Index root page ID - long (8 bytes)
            + 2 * Long.BYTES; // Row ID uuid for which the index needs to be built - {@link UUID} (16 bytes)

    /**
     * Returns an offset of the element inside the page.
     *
     * @see BplusIo#offset(int)
     */
    int offset(int idx);

    /**
     * Compare the {@link IndexMeta} from the page with passed {@link IndexMeta}.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param indexMeta Lookup index meta.
     * @return Comparison result.
     */
    default int compare(long pageAddr, int idx, IndexMetaKey indexMeta) {
        int elementOffset = offset(idx);

        return Integer.compare(
                getInt(pageAddr, elementOffset + INDEX_ID_OFFSET),
                indexMeta.indexId()
        );
    }

    /**
     * Reads the index meta from the page.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     */
    default IndexMeta getRow(long pageAddr, int idx) {
        int elementOffset = offset(idx);

        int indexId = getInt(pageAddr, elementOffset + INDEX_ID_OFFSET);

        IndexType indexType = IndexType.deserialize(getByte(pageAddr, elementOffset + INDEX_TYPE_OFFSET));

        long indexTreeMetaPageId = getLong(pageAddr, elementOffset + INDEX_TREE_META_PAGE_ID_OFFSET);

        long nextRowIdUuidToBuiltMsb = getLong(pageAddr, elementOffset + NEXT_ROW_ID_TO_BUILT_MSB_OFFSET);
        long nextRowIdUuidToBuiltLsb = getLong(pageAddr, elementOffset + NEXT_ROW_ID_TO_BUILT_LSB_OFFSET);

        UUID nextRowIdUuid = nextRowIdUuidToBuiltMsb == 0L && nextRowIdUuidToBuiltLsb == 0L
                ? null
                : new UUID(nextRowIdUuidToBuiltMsb, nextRowIdUuidToBuiltLsb);

        return new IndexMeta(indexId, indexType, indexTreeMetaPageId, nextRowIdUuid);
    }

    /**
     * Stores the index meta, copied from another page.
     *
     * @see BplusIo#store(long, int, BplusIo, long, int)
     */
    default void store(long dstPageAddr, int dstIdx, BplusIo<IndexMetaKey> srcIo, long srcPageAddr, int srcIdx) {
        int dstOffset = offset(dstIdx);
        int srcOffset = srcIo.offset(srcIdx);

        PageUtils.copyMemory(srcPageAddr, srcOffset, dstPageAddr, dstOffset, SIZE_IN_BYTES);
    }

    /**
     * Stores the index meta in the page.
     *
     * @see BplusIo#storeByOffset(long, int, Object)
     */
    default void storeByOffset(long pageAddr, int off, IndexMetaKey rowKey) {
        assert rowKey instanceof IndexMeta : rowKey;

        IndexMeta row = (IndexMeta) rowKey;

        putInt(pageAddr, off + INDEX_ID_OFFSET, row.indexId());

        putByte(pageAddr, off + INDEX_TYPE_OFFSET, row.indexType().serialize());

        putLong(pageAddr, off + INDEX_TREE_META_PAGE_ID_OFFSET, row.metaPageId());

        UUID nextRowIdUuidToBuild = row.nextRowIdUuidToBuild();

        long nextRowIdUuidToBuildMsb = nextRowIdUuidToBuild == null ? 0L : nextRowIdUuidToBuild.getMostSignificantBits();
        long nextRowIdUuidToBuildLsb = nextRowIdUuidToBuild == null ? 0L : nextRowIdUuidToBuild.getLeastSignificantBits();

        putLong(pageAddr, off + NEXT_ROW_ID_TO_BUILT_MSB_OFFSET, nextRowIdUuidToBuildMsb);
        putLong(pageAddr, off + NEXT_ROW_ID_TO_BUILT_LSB_OFFSET, nextRowIdUuidToBuildLsb);
    }
}
