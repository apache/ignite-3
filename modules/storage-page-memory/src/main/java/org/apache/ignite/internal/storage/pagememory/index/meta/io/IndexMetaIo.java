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

import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;

/**
 * Interface for {@link IndexMeta} B+Tree-related IO.
 *
 * <p>Defines a following data layout:
 * <ul>
 *     <li>Index ID - {@link UUID} (16 bytes);</li>
 *     <li>Index root page ID - long (8 bytes).</li>
 * </ul>
 */
public interface IndexMetaIo {
    /** Offset of the {@link UUID#getMostSignificantBits() most significant bits} of the index ID (8 bytes). */
    int INDEX_ID_MSB_OFFSET = 0;

    /** Offset of the {@link UUID#getLeastSignificantBits() least significant bits} of the index ID (8 bytes). */
    int INDEX_ID_LSB_OFFSET = INDEX_ID_MSB_OFFSET + Long.BYTES;

    /** Index tree meta page id offset - long (8 bytes). */
    int INDEX_TREE_META_PAGE_ID_OFFSET = INDEX_ID_LSB_OFFSET + Long.BYTES;

    /** Payload size in bytes. */
    int SIZE_IN_BYTES = 2 * Long.BYTES /* Index ID - {@link UUID} (16 bytes) */ + Long.BYTES /* Index root page ID - long (8 bytes) */;

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
    default int compare(long pageAddr, int idx, IndexMeta indexMeta) {
        int elementOffset = offset(idx);

        int cmp = Long.compare(getLong(pageAddr, elementOffset + INDEX_ID_MSB_OFFSET), indexMeta.id().getMostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        return Long.compare(getLong(pageAddr, elementOffset + INDEX_ID_LSB_OFFSET), indexMeta.id().getLeastSignificantBits());
    }

    /**
     * Reads the index meta from the page.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     */
    default IndexMeta getRow(long pageAddr, int idx) {
        int elementOffset = offset(idx);

        long indexIdMsb = getLong(pageAddr, elementOffset + INDEX_ID_MSB_OFFSET);
        long indexIdLsb = getLong(pageAddr, elementOffset + INDEX_ID_LSB_OFFSET);

        long indexTreeMetaPageId = getLong(pageAddr, elementOffset + INDEX_TREE_META_PAGE_ID_OFFSET);

        return new IndexMeta(new UUID(indexIdMsb, indexIdLsb), indexTreeMetaPageId);
    }

    /**
     * Stores the index meta, copied from another page.
     *
     * @see BplusIo#store(long, int, BplusIo, long, int)
     */
    default void store(long dstPageAddr, int dstIdx, long srcPageAddr, int srcIdx) {
        int dstOffset = offset(dstIdx);
        int srcOffset = offset(srcIdx);

        PageUtils.copyMemory(srcPageAddr, srcOffset, dstPageAddr, dstOffset, SIZE_IN_BYTES);
    }

    /**
     * Stores the index meta in the page.
     *
     * @see BplusIo#storeByOffset(long, int, Object)
     */
    default void storeByOffset(long pageAddr, int off, IndexMeta row) {
        putLong(pageAddr, off + INDEX_ID_MSB_OFFSET, row.id().getMostSignificantBits());
        putLong(pageAddr, off + INDEX_ID_LSB_OFFSET, row.id().getLeastSignificantBits());

        putLong(pageAddr, off + INDEX_TREE_META_PAGE_ID_OFFSET, row.metaPageId());
    }
}
