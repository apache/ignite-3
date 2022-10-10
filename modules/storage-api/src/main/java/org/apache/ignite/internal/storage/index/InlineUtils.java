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

package org.apache.ignite.internal.storage.index;

import static org.apache.ignite.internal.util.Constants.KiB;

import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.VarlenNativeType;
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
     * @param undefinedVarlenInlineSize Inline size of an undefined (not set by the user) variable length column in bytes.
     * @return Inline size in bytes.
     */
    static int inlineSize(NativeType nativeType, int undefinedVarlenInlineSize) {
        NativeTypeSpec spec = nativeType.spec();

        if (spec.fixedLength()) {
            return nativeType.sizeInBytes();
        }

        // Variable length columns.

        switch (spec) {
            case STRING: {
                int length = ((VarlenNativeType) nativeType).length();

                // * 2 because characters take up 2 bytes (char).
                return length == Integer.MAX_VALUE ? undefinedVarlenInlineSize : length * 2;
            }

            case BYTES: {
                int length = ((VarlenNativeType) nativeType).length();

                return length == Integer.MAX_VALUE ? undefinedVarlenInlineSize : length;
            }

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
     * @return Inline size in bytes, not more than the {@link #MAX_BINARY_TUPLE_INLINE_SIZE}.
     */
    static int binaryTupleInlineSize(IndexDescriptor indexDescriptor) {
        List<? extends ColumnDescriptor> columns = indexDescriptor.columns();

        boolean hasNullColumns = columns.stream().anyMatch(ColumnDescriptor::nullable);

        // TODO: IGNITE-17855 вот тут надо подумать что если все колонки фиксированного размера, то можно класс ее посчиать получше

        int inlineSize = BinaryTupleCommon.HEADER_SIZE
                + (hasNullColumns ? BinaryTupleCommon.nullMapSize(columns.size()) : 0)
                + columns.size() * 2; // 2-byte size class covers most cases.

        for (int i = 0; i < columns.size() && inlineSize < MAX_BINARY_TUPLE_INLINE_SIZE; i++) {
            inlineSize += inlineSize(columns.get(i).type(), 10);
        }

        return Math.min(inlineSize, MAX_BINARY_TUPLE_INLINE_SIZE);
    }
}
