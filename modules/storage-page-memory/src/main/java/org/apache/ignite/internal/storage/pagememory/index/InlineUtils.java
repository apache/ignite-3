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

import static java.util.function.Predicate.not;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
import static org.apache.ignite.internal.util.Constants.KiB;

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
 */
public class InlineUtils {
    /** Maximum inline size for a {@link BinaryTuple}, in bytes. */
    public static final int MAX_BINARY_TUPLE_INLINE_SIZE = 2 * KiB;

    /**
     * Calculates inline size for column.
     *
     * @param nativeType Column type.
     * @param maxVarlenInlineSize Maximum inline size of variable length column in bytes.
     * @return Inline size in bytes.
     */
    static int inlineSize(NativeType nativeType, int maxVarlenInlineSize) {
        NativeTypeSpec spec = nativeType.spec();

        if (spec.fixedLength()) {
            return nativeType.sizeInBytes();
        }

        // Variable length columns.

        switch (spec) {
            case STRING:
            case BYTES:
                return Math.min(maxVarlenInlineSize, ((VarlenNativeType) nativeType).length());

            case DECIMAL:
                // It can be converted to float for greater or lesser comparisons.
                return Float.BYTES;

            case NUMBER:
                // It can be converted to int for greater or lesser comparisons.
                return Integer.BYTES;

            default:
                throw new IllegalArgumentException("Unknown type " + spec);
        }
    }

    /**
     * Calculates inline size for {@link BinaryTuple}, given its format.
     *
     * @param indexDescriptor Index descriptor.
     * @param maxVarlenInlineSize Maximum inline size of variable length column in bytes.
     * @return Inline size in bytes, not more than the {@link #MAX_BINARY_TUPLE_INLINE_SIZE}.
     */
    static int binaryTupleInlineSize(IndexDescriptor indexDescriptor, int maxVarlenInlineSize) {
        List<? extends ColumnDescriptor> columns = indexDescriptor.columns();

        boolean hasNullColumns = columns.stream().anyMatch(ColumnDescriptor::nullable);

        // 2-byte size class covers most cases.
        int sizeClass = 2;

        if (columns.stream().allMatch(c -> c.type().spec().fixedLength())) {
            int totalFixedColumnSize = columns.stream().mapToInt(c -> c.type().sizeInBytes()).sum();

            sizeClass = Math.min(sizeClass, BinaryTupleCommon.flagsToEntrySize(BinaryTupleCommon.valueSizeToFlags(totalFixedColumnSize)));
        }

        int inlineSize = BinaryTupleCommon.HEADER_SIZE
                + (hasNullColumns ? BinaryTupleCommon.nullMapSize(columns.size()) : 0)
                + columns.size() * sizeClass;

        for (int i = 0; i < columns.size() && inlineSize < MAX_BINARY_TUPLE_INLINE_SIZE; i++) {
            inlineSize += inlineSize(columns.get(i).type(), maxVarlenInlineSize);
        }

        return Math.min(inlineSize, MAX_BINARY_TUPLE_INLINE_SIZE);
    }

    /**
     * Calculates the inline size for the {@link BinaryTuple} that will be stored in the index {@link BplusInnerIo} and {@link BplusLeafIo}.
     * Minimum number of items in a {@link BplusInnerIo} is {@code 2}, so that there is no significant performance drop.
     *
     * @param pageSize Page size in bytes.
     * @param indexBplusIoHeaderSize Index {@link BplusInnerIo} and {@link BplusLeafIo} header size in bytes.
     * @param indexDescriptor Index descriptor.
     * @param maxVarlenInlineSize Maximum inline size of variable length column in bytes.
     * @return Inline size in bytes, not more than the {@link #MAX_BINARY_TUPLE_INLINE_SIZE}.
     */
    public static int binaryTupleBplusIoInlineSize(
            int pageSize,
            int indexBplusIoHeaderSize,
            IndexDescriptor indexDescriptor,
            int maxVarlenInlineSize
    ) {
        int innerNodePayloadSize = pageSize - BplusInnerIo.HEADER_SIZE - PARTITIONLESS_LINK_SIZE_BYTES;
        int leafNodePayloadSize = pageSize - BplusLeafIo.HEADER_SIZE;

        // So that at least 2 items fit on the BplusInnerIo.
        int maxInnerNodeItemSize = (innerNodePayloadSize / 2) - PARTITIONLESS_LINK_SIZE_BYTES;

        int itemSize = Math.min(maxInnerNodeItemSize, binaryTupleInlineSize(indexDescriptor, maxVarlenInlineSize) + indexBplusIoHeaderSize);

        // If there is a variable length column.
        if (indexDescriptor.columns().stream().anyMatch(not(c -> c.type().spec().fixedLength()))) {
            // Let's try to increase itemSize for BplusInnerIo.
            int additionalInnerNodeItemSize = (innerNodePayloadSize % (itemSize + PARTITIONLESS_LINK_SIZE_BYTES))
                    / (innerNodePayloadSize / (itemSize + PARTITIONLESS_LINK_SIZE_BYTES));

            // Let's check if we can increase itemSize for BplusInnerIo so that the number of items for BplusLeafIo does not decrease.
            if (additionalInnerNodeItemSize > 0
                    && (leafNodePayloadSize / (itemSize + additionalInnerNodeItemSize)) >= leafNodePayloadSize / itemSize) {
                itemSize += additionalInnerNodeItemSize;
            }
        }

        return Math.min(itemSize - indexBplusIoHeaderSize, MAX_BINARY_TUPLE_INLINE_SIZE);
    }
}
