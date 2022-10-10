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
import static org.apache.ignite.internal.util.Constants.KiB;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.VarlenNativeType;
import org.apache.ignite.internal.storage.index.IndexDescriptor;
import org.apache.ignite.internal.storage.index.IndexDescriptor.ColumnDescriptor;

/**
 * Helper class for index inlining.
 *
 * 
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
    static final int MIN_INNER_NODE_ITEM_COUNT = 2;

    /** Heuristic maximum inline size for {@link BigDecimal} and {@link BigInteger} column in bytes. */
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
            case NUMBER:
                return 4;

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
    static int binaryTupleInlineSize(IndexDescriptor indexDescriptor) {
        List<? extends ColumnDescriptor> columns = indexDescriptor.columns();

        boolean hasNullColumns = columns.stream().anyMatch(ColumnDescriptor::nullable);

        // First, let's calculate the inline size for all columns.
        int columnsInlineSize = columns.stream().map(ColumnDescriptor::type).mapToInt(InlineUtils::inlineSize).sum();

        int inlineSize = BinaryTupleCommon.HEADER_SIZE
                + (hasNullColumns ? BinaryTupleCommon.nullMapSize(columns.size()) : 0)
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
    public static int binaryTupleInlineSize(int pageSize, int itemHeaderSize, IndexDescriptor indexDescriptor) {
        int maxInnerNodeItemSize = (innerNodePayloadSize(pageSize) / MIN_INNER_NODE_ITEM_COUNT) - BplusInnerIo.CHILD_LINK_SIZE;

        int binaryTupleInlineSize = Math.min(maxInnerNodeItemSize - itemHeaderSize, binaryTupleInlineSize(indexDescriptor));

        if (binaryTupleInlineSize >= MAX_BINARY_TUPLE_INLINE_SIZE) {
            return binaryTupleInlineSize;
        }

        // If we have variable length columns and there is enough unused space in the innerNodes and leafNodes, then we can try increasing
        // the inline size for the BinaryTuple. Example: pageSize = 1024, binaryTupleInlineSize = 124, BplusLeafIo.HEADER_SIZE = 56.
        // In this case, the innerNode will fit 7 (with child links) items and 7 items in the leafNode, while 52 bytes will remain free for
        // the innerNode items, and 100 bytes for the leafNode items, which we could use. For a innerNode, we can add (52 / 7) = 7 bytes
        // for each item (with link), and for a leafNode, (100 / 7) = 14 bytes for each item, so we can safely use the 7 extra bytes for the
        // innerNode and leafNode per item.

        if (indexDescriptor.columns().stream().anyMatch(c -> !c.type().spec().fixedLength())) {
            int itemSize = binaryTupleInlineSize + itemHeaderSize;

            // Let's calculate whether to remain a place in the BplusInnerIo and the BplusLeafIo.
            int remainingInnerNodePayloadSize = innerNodePayloadSize(pageSize) % (itemSize + BplusInnerIo.CHILD_LINK_SIZE);
            int remainingLeafNodePayloadSize = leafNodePayloadSize(pageSize) % itemSize;

            if (remainingInnerNodePayloadSize > 0 && remainingLeafNodePayloadSize > 0) {
                int innerNodeItemCount = innerNodePayloadSize(pageSize) / (itemSize + BplusInnerIo.CHILD_LINK_SIZE);
                int leafNodeItemCount = leafNodePayloadSize(pageSize) / itemSize;

                // Let's calculate how many additional bytes can be added for each item of the BplusInnerIo and the BplusLeafIo.
                int additionalInnerNodeItemBytes = remainingInnerNodePayloadSize / innerNodeItemCount;
                int additionalLeafNodeItemBytes = remainingLeafNodePayloadSize / leafNodeItemCount;

                if (additionalInnerNodeItemBytes > 0 && additionalLeafNodeItemBytes > 0) {
                    int minAdditionalItemBytes = Math.min(additionalInnerNodeItemBytes, additionalLeafNodeItemBytes);

                    // We need to make sure that by increasing the itemSize we do not reduce the number of items in the BplusLeafIo, this
                    // may affect its density (holes in one item may appear).

                    if (leafNodePayloadSize(pageSize) / (itemSize + minAdditionalItemBytes) == leafNodeItemCount) {
                        binaryTupleInlineSize = (itemSize + minAdditionalItemBytes) - itemHeaderSize;
                    }
                }
            }
        }

        return Math.min(binaryTupleInlineSize, MAX_BINARY_TUPLE_INLINE_SIZE);
    }

    /**
     * Returns number of bytes that can be used to store items and references to child nodes.
     *
     * @param pageSize Page size in bytes.
     */
    static int innerNodePayloadSize(int pageSize) {
        return pageSize - BplusInnerIo.HEADER_SIZE - BplusInnerIo.CHILD_LINK_SIZE;
    }

    /**
     * Returns number of bytes that can be used to store items.
     *
     * @param pageSize Page size in bytes.
     */
    static int leafNodePayloadSize(int pageSize) {
        return pageSize - BplusLeafIo.HEADER_SIZE;
    }
}
