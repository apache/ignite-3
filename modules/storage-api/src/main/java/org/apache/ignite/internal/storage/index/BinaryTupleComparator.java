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

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.EQUALITY_FLAG;
import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.PREFIX_FLAG;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.type.NativeTypeSpec;

/**
 * Comparator implementation for comparing {@link BinaryTuple}s on a per-column basis.
 *
 * <p>This comparator is used to compare BinaryTuples as well as {@link BinaryTuplePrefix}es. When comparing a tuple with a prefix,
 * the following logic is applied: if all N columns of the prefix match the first N columns of the tuple, they are considered equal.
 * Otherwise comparison result is determined by the first non-matching column.
 */
public class BinaryTupleComparator implements Comparator<ByteBuffer> {
    private final List<StorageSortedIndexColumnDescriptor> columns;

    /**
     * Creates a comparator for a Sorted Index identified by the given columns descriptors.
     */
    public BinaryTupleComparator(List<StorageSortedIndexColumnDescriptor> columns) {
        this.columns = columns;
    }

    @Override
    public int compare(ByteBuffer buffer1, ByteBuffer buffer2) {
        assert buffer1.order() == ByteOrder.LITTLE_ENDIAN;
        assert buffer2.order() == ByteOrder.LITTLE_ENDIAN;

        boolean isBuffer1Prefix = isFlagSet(buffer1, PREFIX_FLAG);
        boolean isBuffer2Prefix = isFlagSet(buffer2, PREFIX_FLAG);

        int numElements = columns.size();

        BinaryTupleReader tuple1 = isBuffer1Prefix ? new BinaryTuplePrefix(numElements, buffer1) : new BinaryTuple(numElements, buffer1);
        BinaryTupleReader tuple2 = isBuffer2Prefix ? new BinaryTuplePrefix(numElements, buffer2) : new BinaryTuple(numElements, buffer2);

        int columnsToCompare = Math.min(tuple1.elementCount(), tuple2.elementCount());

        assert columnsToCompare <= columns.size();

        for (int i = 0; i < columnsToCompare; i++) {
            StorageSortedIndexColumnDescriptor columnDescriptor = columns.get(i);

            int compare = compareField(tuple1, tuple2, i);

            if (compare != 0) {
                return columnDescriptor.asc() ? compare : -compare;
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
     * Compares individual fields of two tuples.
     */
    private int compareField(BinaryTupleReader tuple1, BinaryTupleReader tuple2, int index) {
        boolean tuple1HasNull = tuple1.hasNullValue(index);
        boolean tuple2HasNull = tuple2.hasNullValue(index);

        // TODO IGNITE-15141: Make null-order configurable.
        if (tuple1HasNull && tuple2HasNull) {
            return 0;
        } else if (tuple1HasNull) {
            return 1;
        } else if (tuple2HasNull) {
            return -1;
        }

        StorageSortedIndexColumnDescriptor columnDescriptor = columns.get(index);

        NativeTypeSpec typeSpec = columnDescriptor.type().spec();

        switch (typeSpec) {
            case INT8:
            case BOOLEAN:
                return Byte.compare(tuple1.byteValue(index), tuple2.byteValue(index));

            case INT16:
                return Short.compare(tuple1.shortValue(index), tuple2.shortValue(index));

            case INT32:
                return Integer.compare(tuple1.intValue(index), tuple2.intValue(index));

            case INT64:
                return Long.compare(tuple1.longValue(index), tuple2.longValue(index));

            case FLOAT:
                return Float.compare(tuple1.floatValue(index), tuple2.floatValue(index));

            case DOUBLE:
                return Double.compare(tuple1.doubleValue(index), tuple2.doubleValue(index));

            case BYTES:
                return Arrays.compare(tuple1.bytesValue(index), tuple2.bytesValue(index));

            case UUID:
                return tuple1.uuidValue(index).compareTo(tuple2.uuidValue(index));

            case STRING:
                return tuple1.stringValue(index).compareTo(tuple2.stringValue(index));

            case DECIMAL:
                return tuple1.decimalValue(index, Integer.MIN_VALUE).compareTo(tuple2.decimalValue(index, Integer.MIN_VALUE));

            case TIMESTAMP:
                return tuple1.timestampValue(index).compareTo(tuple2.timestampValue(index));

            case DATE:
                return tuple1.dateValue(index).compareTo(tuple2.dateValue(index));

            case TIME:
                return tuple1.timeValue(index).compareTo(tuple2.timeValue(index));

            case DATETIME:
                return tuple1.dateTimeValue(index).compareTo(tuple2.dateTimeValue(index));

            default:
                throw new IllegalArgumentException(format(
                        "Unsupported column type in binary tuple comparator. [index={}, type={}]",
                        index, columnDescriptor.type()
                ));
        }
    }

    private static boolean isFlagSet(ByteBuffer tuple, int flag) {
        return (tuple.get(0) & flag) != 0;
    }

    private static int equalityFlag(ByteBuffer tuple) {
        return isFlagSet(tuple, EQUALITY_FLAG) ? 1 : -1;
    }
}
