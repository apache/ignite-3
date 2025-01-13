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

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.EQUALITY_FLAG;
import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.PREFIX_FLAG;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;

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

        BinaryTupleReader tuple1 = isBuffer1Prefix ? new BinaryTuplePrefix(numElements, buffer1) : new BinaryTuple(numElements, buffer1);
        BinaryTupleReader tuple2 = isBuffer2Prefix ? new BinaryTuplePrefix(numElements, buffer2) : new BinaryTuple(numElements, buffer2);

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

        int res = compareField(nativeType.spec(), tuple1, colIdx, tuple2, colIdx);

        return collation.asc() ? res : -res;
    }

    /**
     * Compares individual fields of two tuples using ascending order.
     */
    @SuppressWarnings("DataFlowIssue")
    private static int compareField(NativeTypeSpec typeSpec, BinaryTupleReader tuple1, int index1, BinaryTupleReader tuple2, int index2) {
        switch (typeSpec) {
            case INT8:
            case BOOLEAN:
                return Byte.compare(tuple1.byteValue(index1), tuple2.byteValue(index2));

            case INT16:
                return Short.compare(tuple1.shortValue(index1), tuple2.shortValue(index2));

            case INT32:
                return Integer.compare(tuple1.intValue(index1), tuple2.intValue(index2));

            case INT64:
                return Long.compare(tuple1.longValue(index1), tuple2.longValue(index2));

            case FLOAT:
                return Float.compare(tuple1.floatValue(index1), tuple2.floatValue(index2));

            case DOUBLE:
                return Double.compare(tuple1.doubleValue(index1), tuple2.doubleValue(index2));

            case BYTES:
                return Arrays.compareUnsigned(tuple1.bytesValue(index1), tuple2.bytesValue(index2));

            case UUID:
                return tuple1.uuidValue(index1).compareTo(tuple2.uuidValue(index2));

            case STRING:
                return tuple1.stringValue(index1).compareTo(tuple2.stringValue(index2));

            case DECIMAL:
                BigDecimal numeric1 = tuple1.decimalValue(index1, Integer.MIN_VALUE);
                BigDecimal numeric2 = tuple2.decimalValue(index2, Integer.MIN_VALUE);

                return numeric1.compareTo(numeric2);

            case TIMESTAMP:
                return tuple1.timestampValue(index1).compareTo(tuple2.timestampValue(index2));

            case DATE:
                return tuple1.dateValue(index1).compareTo(tuple2.dateValue(index2));

            case TIME:
                return tuple1.timeValue(index1).compareTo(tuple2.timeValue(index2));

            case DATETIME:
                return tuple1.dateTimeValue(index1).compareTo(tuple2.dateTimeValue(index2));

            default:
                throw new IllegalArgumentException(format("Unsupported column type in binary tuple comparator. [type={}]", typeSpec));
        }
    }

    private static boolean isFlagSet(ByteBuffer tuple, int flag) {
        return (tuple.get(0) & flag) != 0;
    }

    private static int equalityFlag(ByteBuffer tuple) {
        return isFlagSet(tuple, EQUALITY_FLAG) ? 1 : -1;
    }
}
