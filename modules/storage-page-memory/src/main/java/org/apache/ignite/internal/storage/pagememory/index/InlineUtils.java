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

package org.apache.ignite.internal.storage.pagememory.index;

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.valueSizeToEntrySize;
import static org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo.CHILD_LINK_SIZE;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
import static org.apache.ignite.internal.util.Constants.KiB;

import java.math.BigDecimal;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor.StorageColumnDescriptor;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.VarlenNativeType;

/**
 * Helper class for index inlining.
 *
 * <p>Index inlining is an optimization that allows index rows (or prefix) to be compared in a {@link BplusTree} without loading them from a
 * {@link FreeList}.
 */
public class InlineUtils {
    /** Maximum inline size for a {@link BinaryTuple}, in bytes. */
    public static final int MAX_BINARY_TUPLE_INLINE_SIZE = 2 * KiB;

    /** Heuristic maximum inline size of a variable length column in bytes. */
    static final int MAX_VARLEN_INLINE_SIZE = 64;

    /** Heuristic maximum size of an entry in the {@link BinaryTuple} offset table, in bytes. */
    static final int MAX_BINARY_TUPLE_OFFSET_TABLE_ENTRY_SIZE = 2;

    /**
     * Minimum number of items in a {@link BplusInnerIo} so that the search in the B+tree does not lose much in performance due to its rapid
     * growth.
     */
    static final int MIN_INNER_PAGE_ITEM_COUNT = 2;

    /** Heuristic maximum inline size for {@link BigDecimal} column in bytes. */
    static final int BIG_NUMBER_INLINE_SIZE = 4;

    /**
     * Calculates inline size for column.
     *
     * @param nativeType Column type.
     * @return Inline size in bytes.
     */
    static int inlineSize(NativeType nativeType) {
        NativeTypeSpec spec = nativeType.spec();

        if (spec.fixedLength()) {
            return nativeType.sizeInBytes();
        }

        // Variable length columns.

        switch (spec) {
            case STRING:
            case BYTES:
                return Math.min(MAX_VARLEN_INLINE_SIZE, ((VarlenNativeType) nativeType).length());

            case DECIMAL:
                return Short.BYTES + BIG_NUMBER_INLINE_SIZE;

            default:
                throw new IllegalArgumentException("Unknown type " + spec);
        }
    }

    /**
     * Calculates inline size for {@link BinaryTuple}, given its format.
     *
     * @param indexDescriptor Index descriptor.
     * @return Inline size in bytes, no more than {@link #MAX_BINARY_TUPLE_INLINE_SIZE}.
     */
    static int binaryTupleInlineSize(StorageIndexDescriptor indexDescriptor) {
        List<? extends StorageColumnDescriptor> columns = indexDescriptor.columns();

        assert !columns.isEmpty();

        // Let's calculate the inline size for all columns.
        int columnsInlineSize = columns.stream().map(StorageColumnDescriptor::type).mapToInt(InlineUtils::inlineSize).sum();

        int inlineSize = BinaryTupleCommon.HEADER_SIZE
                + columns.size() * Math.min(MAX_BINARY_TUPLE_OFFSET_TABLE_ENTRY_SIZE, valueSizeToEntrySize(columnsInlineSize))
                + columnsInlineSize;

        return Math.min(inlineSize, MAX_BINARY_TUPLE_INLINE_SIZE);
    }

    /**
     * Calculates the inline size of {@link BinaryTuple} that will be stored in the {@link BplusInnerIo} and {@link BplusLeafIo} item.
     *
     * @param pageSize Page size in bytes.
     * @param itemHeaderSize Size of the item header that is stored in the {@link BplusInnerIo} and {@link BplusLeafIo}, in bytes.
     * @param indexDescriptor Index descriptor.
     * @return Inline size in bytes, no more than {@link #MAX_BINARY_TUPLE_INLINE_SIZE}.
     */
    public static int binaryTupleInlineSize(int pageSize, int itemHeaderSize, StorageIndexDescriptor indexDescriptor) {
        int maxInnerNodeItemSize = ((innerNodePayloadSize(pageSize) - CHILD_LINK_SIZE) / MIN_INNER_PAGE_ITEM_COUNT) - CHILD_LINK_SIZE;

        int binaryTupleInlineSize = Math.min(maxInnerNodeItemSize - itemHeaderSize, binaryTupleInlineSize(indexDescriptor));

        if (binaryTupleInlineSize >= MAX_BINARY_TUPLE_INLINE_SIZE) {
            return MAX_BINARY_TUPLE_INLINE_SIZE;
        }

        // If we have variable length columns and there is enough unused space in the innerNodes and leafNodes, then we can try increasing
        // the inline size for the BinaryTuple. Example: pageSize = 1024, binaryTupleInlineSize = 124, BplusLeafIo.HEADER_SIZE = 56.
        // In this case, the innerNode will fit 7 (with child links) items and 7 items in the leafNode, while 52 bytes will remain free for
        // the innerNode items, and 100 bytes for the leafNode items, which we could use. For a innerNode, we can add (52 / 7) = 7 bytes
        // for each item (with link), and for a leafNode, (100 / 7) = 14 bytes for each item, so we can safely use the 7 extra bytes for the
        // innerNode and leafNode per item.

        if (indexDescriptor.columns().stream().anyMatch(c -> !c.type().spec().fixedLength())) {
            int itemSize = binaryTupleInlineSize + itemHeaderSize;

            int innerNodeItemSize =
                    optimizeItemSize(innerNodePayloadSize(pageSize) - CHILD_LINK_SIZE, itemSize + CHILD_LINK_SIZE) - CHILD_LINK_SIZE;

            int leafNodeItemSize = optimizeItemSize(leafNodePayloadSize(pageSize), itemSize);

            int optimizedItemSize = Math.min(innerNodeItemSize, leafNodeItemSize);

            assert leafNodePayloadSize(pageSize) / itemSize == leafNodePayloadSize(pageSize) / optimizedItemSize;

            binaryTupleInlineSize = optimizedItemSize - itemHeaderSize;
        }

        return Math.min(binaryTupleInlineSize, MAX_BINARY_TUPLE_INLINE_SIZE);
    }

    /**
     * Returns number of bytes that can be used to store items and links to child nodes in an inner node.
     *
     * @param pageSize Page size in bytes.
     */
    static int innerNodePayloadSize(int pageSize) {
        return pageSize - BplusInnerIo.HEADER_SIZE;
    }

    /**
     * Returns number of bytes that can be used to store items in a leaf node.
     *
     * @param pageSize Page size in bytes.
     */
    static int leafNodePayloadSize(int pageSize) {
        return pageSize - BplusLeafIo.HEADER_SIZE;
    }

    /**
     * Optimizes the item size for a {@link BplusInnerIo} or {@link BplusLeafIo} if there is free space for each item.
     *
     * <p>We try to use the available memory on the page as much as possible.
     *
     * @param nodePayloadSize Payload size of a {@link BplusInnerIo} or {@link BplusLeafIo}, in bytes.
     * @param itemSize Size of the item in {@link BplusInnerIo} or {@link BplusLeafIo}, in bytes.
     * @return Size in bytes.
     */
    static int optimizeItemSize(int nodePayloadSize, int itemSize) {
        // Let's calculate how much space remains in the BplusInnerIo or the BplusLeafIo.
        int remainingNodePayloadSize = nodePayloadSize % itemSize;

        int nodeItemCount = nodePayloadSize / itemSize;

        // Let's calculate how many additional bytes can be added for each item of the BplusInnerIo and the BplusLeafIo.
        int additionalNodeItemBytes = remainingNodePayloadSize / nodeItemCount;

        return itemSize + additionalNodeItemBytes;
    }

    /**
     * Checks if index columns can be fully inlined.
     *
     * @param indexColumnsSize Index columns size in bytes.
     * @param inlineSize Inline size in bytes.
     */
    public static boolean canFullyInline(int indexColumnsSize, int inlineSize) {
        return indexColumnsSize <= inlineSize + PARTITIONLESS_LINK_SIZE_BYTES;
    }
}
