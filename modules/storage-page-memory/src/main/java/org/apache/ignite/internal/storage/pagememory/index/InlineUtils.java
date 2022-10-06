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

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.HEADER_SIZE;
import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.nullMapSize;
import static org.apache.ignite.internal.util.Constants.KiB;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.VarlenNativeType;
import org.apache.ignite.internal.storage.index.IndexDescriptor;
import org.apache.ignite.internal.storage.index.IndexDescriptor.ColumnDescriptor;

/**
 * Helper class for inline indexes.
 */
// TODO: IGNITE-17536 вот тут надо подсчет размера изменить
public class InlineUtils {
    /** Heuristic maximum inline size in bytes for storing entries in B+tree InnerNodes. */
    public static final int MAX_INLINE_SIZE = 2 * KiB;

    /** Heuristic default inline size in bytes for variable length columns, such as strings. */
    public static final int DEFAULT_VARLEN_COLUMN_INLINE_SIZE = 10;

    /** Heuristic inline size in bytes for big numbers, such as {@link BigDecimal} and {@link BigInteger}. */
    public static final int BIG_NUMBER_INLINE_SIZE = 4;

    /** Heuristic size class in bytes for offset table in {@link BinaryTuple}. */
    public static final int TABLE_OFFSET_CLASS_SIZE = 2;

    /**
     * Calculates the inline size for a {@link BinaryTuple} given its format.
     * <ul>
     *     <li>Header - 1 byte;</li>
     *     <li>Nullmap (if there are nullable columns) - (Number of columns * 7) / 8 bytes;</li>
     *     <li>Table offset - Number of columns * {@link #TABLE_OFFSET_CLASS_SIZE} bytes;</li>
     *     <li>Value area - total size of the columns in bytes.</li>
     * </ul>
     *
     * @param indexDescriptor Index descriptor.
     * @return Inline size in bytes, no more than the {@link #MAX_INLINE_SIZE}.
     */
    public static int calculateBinaryTupleInlineSize(IndexDescriptor indexDescriptor) {
        List<? extends ColumnDescriptor> columns = indexDescriptor.columns();

        boolean hasNullableColumns = columns.stream().anyMatch(ColumnDescriptor::nullable);

        int inlineSize = HEADER_SIZE // Header.
                + (hasNullableColumns ? nullMapSize(columns.size()) : 0) // Nullmap if present.
                + columns.size() * TABLE_OFFSET_CLASS_SIZE; // Table offset.

        for (int i = 0; i < columns.size() && inlineSize < MAX_INLINE_SIZE; i++) {
            inlineSize += inlineSize(columns.get(i).type());
        }

        return Math.min(MAX_INLINE_SIZE, inlineSize);
    }

    /**
     * Returns the inline size depending on the {@link NativeType}.
     *
     * @param nativeType Column native type.
     * @return Inline size in bytes.
     */
    static int inlineSize(NativeType nativeType) {
        if (nativeType.spec().fixedLength()) {
            return nativeType.sizeInBytes();
        }

        // Variable length columns.

        switch (nativeType.spec()) {
            case BYTES: {
                int length = ((VarlenNativeType) nativeType).length();

                return length == Integer.MAX_VALUE ? DEFAULT_VARLEN_COLUMN_INLINE_SIZE : length;
            }

            case STRING: {
                int length = ((VarlenNativeType) nativeType).length();

                return length == Integer.MAX_VALUE ? DEFAULT_VARLEN_COLUMN_INLINE_SIZE : length * 2;
            }

            case NUMBER:
            case DECIMAL:
                return BIG_NUMBER_INLINE_SIZE;

            default:
                throw new IllegalArgumentException("Unknown type " + nativeType.spec());
        }
    }

    /**
     * Returns IO versions for index page IO.
     *
     * @param <T> Index page IO type.
     * @param versions IO versions for each inline size, from {@code 1} to {@link #MAX_INLINE_SIZE} bytes.
     * @param inlineSize Inline size in bytes.
     */
    public static <T extends PageIo> IoVersions<T> ioVersions(List<IoVersions<T>> versions, int inlineSize) {
        assert versions.size() < MAX_INLINE_SIZE : versions.size();
        assert inlineSize > 0 && inlineSize <= MAX_INLINE_SIZE : inlineSize;

        return versions.get(inlineSize - 1);
    }
}
