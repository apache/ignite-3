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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.PREFIX_FLAG;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.compareFieldValue;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.equalityFlag;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.isFlagSet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.type.NativeType;

/**
 * Comparator implementation for comparing {@link BinaryTuple}s on a per-column basis.
 *
 * <p>This comparator is used to compare BinaryTuples as well as {@link BinaryTuplePrefix}es. When comparing a tuple with a prefix,
 * the following logic is applied: if all N columns of the prefix match the first N columns of the tuple, they are considered equal.
 * Otherwise comparison result is determined by the first non-matching column.
 */
@SuppressWarnings("ComparatorNotSerializable")
public class BinaryTupleComparator implements Comparator<ByteBuffer> {
    private final List<CatalogColumnCollation>  columnCollations;
    private final List<NativeType> columnTypes;

    /**
     * Creates BinaryTuple comparator.
     *
     * @param columnCollations Columns collations.
     * @param columnTypes Column types in order, which is defined in BinaryTuple schema.
     */
    public BinaryTupleComparator(
            List<CatalogColumnCollation> columnCollations,
            List<NativeType> columnTypes
    ) {
        this.columnCollations = columnCollations;
        this.columnTypes = columnTypes;
    }

    @Override
    public int compare(ByteBuffer buffer1, ByteBuffer buffer2) {
        assert buffer1.order() == ByteOrder.LITTLE_ENDIAN;
        assert buffer2.order() == ByteOrder.LITTLE_ENDIAN;

        boolean isBuffer1Prefix = isFlagSet(buffer1, PREFIX_FLAG);
        boolean isBuffer2Prefix = isFlagSet(buffer2, PREFIX_FLAG);

        int numElements = columnTypes.size();

        BinaryTupleReader tuple1 = isBuffer1Prefix ? new BinaryTuplePrefix(numElements, buffer1)
                : new BinaryTuple(numElements, buffer1, UnsafeByteBufferAccessor::new);
        BinaryTupleReader tuple2 = isBuffer2Prefix ? new BinaryTuplePrefix(numElements, buffer2)
                : new BinaryTuple(numElements, buffer2, UnsafeByteBufferAccessor::new);

        int columnsToCompare = Math.min(tuple1.elementCount(), tuple2.elementCount());

        assert columnsToCompare <= numElements;

        for (int i = 0; i < columnsToCompare; i++) {
            int res = compareField(i, tuple1, tuple2);

            if (res != 0) {
                return res;
            }
        }

        // We use the EQUALITY FLAG to determine the outcome of the comparison operation: if the flag is set, the prefix is considered
        // larger than the tuple and if the flag is not set, the prefix is considered smaller than the tuple. This is needed to include
        // or exclude the scan bounds.
        if (isBuffer1Prefix == isBuffer2Prefix) {
            return 0;
        } else if (isBuffer1Prefix) {
            return equalityFlag(buffer1);
        } else {
            return -equalityFlag(buffer2);
        }
    }

    /**
     * Compares two tuples by column using given column index.
     */
    private int compareField(int colIdx, BinaryTupleReader tuple1, BinaryTupleReader tuple2) {
        CatalogColumnCollation collation = columnCollations.get(colIdx);

        boolean tuple1HasNull = tuple1.hasNullValue(colIdx);
        boolean tuple2HasNull = tuple2.hasNullValue(colIdx);

        if (tuple1HasNull && tuple2HasNull) {
            return 0;
        } else if (tuple1HasNull || tuple2HasNull) {
            return collation.nullsFirst() == tuple1HasNull ? -1 : 1;
        }

        NativeType nativeType = columnTypes.get(colIdx);

        int res = compareFieldValue(nativeType.spec(), tuple1, tuple2, colIdx);

        return collation.asc() ? res : -res;
    }
}
