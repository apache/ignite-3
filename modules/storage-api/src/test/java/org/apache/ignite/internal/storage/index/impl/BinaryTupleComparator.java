/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.index.impl;

import java.util.Arrays;
import java.util.Comparator;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;

/**
 * Comparator implementation for comparting {@link BinaryTuple}s on a per-column basis.
 */
class BinaryTupleComparator implements Comparator<BinaryTuple> {
    private final SortedIndexDescriptor descriptor;

    private final int prefixLength;

    private BinaryTupleComparator(SortedIndexDescriptor descriptor, int prefixLength) {
        if (prefixLength > descriptor.indexColumns().size()) {
            throw new IllegalArgumentException("Invalid prefix length: " + prefixLength);
        }

        this.descriptor = descriptor;
        this.prefixLength = prefixLength;
    }

    /**
     * Creates a comparator for a Sorted Index identified by the given descriptor.
     */
    static BinaryTupleComparator newComparator(SortedIndexDescriptor descriptor) {
        return new BinaryTupleComparator(descriptor, descriptor.indexColumns().size());
    }

    /**
     * Similar to {@link #newComparator} but creates a comparator that only compares first {@code prefixLength} index columns.
     */
    static BinaryTupleComparator newPrefixComparator(SortedIndexDescriptor descriptor, int prefixLength) {
        return new BinaryTupleComparator(descriptor, prefixLength);
    }

    @Override
    public int compare(BinaryTuple tuple1, BinaryTuple tuple2) {
        return compare(tuple1, tuple2, 1, 0);
    }

    /**
     * Compares a given tuple with the configured prefix.
     *
     * @param tuple1 Tuple to compare.
     * @param tuple2 Tuple to compare.
     * @param direction Sort direction: {@code -1} means sorting in reversed order, {@code 1} means  sorting in the natural order.
     * @param equals Value that should be returned if the provided tuple exactly matches the prefix.
     * @return the value {@code 0} if the given row starts with the configured prefix;
     *         a value less than {@code 0} if the row's prefix is smaller than the prefix; and
     *         a value greater than {@code 0} if the row's prefix is larger than the prefix.
     */
    public int compare(BinaryTuple tuple1, BinaryTuple tuple2, int direction, int equals) {
        for (int i = 0; i < prefixLength; i++) {
            ColumnDescriptor columnDescriptor = descriptor.indexColumns().get(i);

            int compare = compareField(tuple1, tuple2, i);

            if (compare != 0) {
                return direction * (columnDescriptor.asc() ? compare : -compare);
            }
        }

        return equals;
    }

    /**
     * Compares individual fields of two tuples.
     */
    private int compareField(BinaryTuple tuple1, BinaryTuple tuple2, int index) {
        boolean tuple1HasNull = tuple1.hasNullValue(index);
        boolean tuple2HasNull = tuple2.hasNullValue(index);

        if (tuple1HasNull && tuple2HasNull) {
            return 0;
        } else if (tuple1HasNull) {
            return -1;
        } else if (tuple2HasNull) {
            return 1;
        }

        ColumnDescriptor columnDescriptor = descriptor.indexColumns().get(index);

        NativeTypeSpec typeSpec = columnDescriptor.type().spec();

        switch (typeSpec) {
            case INT8:
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

            case BITMASK:
                return Arrays.compare(tuple1.bitmaskValue(index).toLongArray(), tuple2.bitmaskValue(index).toLongArray());

            // all other types implement Comparable
            case DECIMAL:
            case UUID:
            case STRING:
            case NUMBER:
            case TIMESTAMP:
            case DATE:
            case TIME:
            case DATETIME:
                return ((Comparable) typeSpec.objectValue(tuple1, index)).compareTo(typeSpec.objectValue(tuple2, index));

            default:
                throw new IllegalArgumentException(String.format(
                        "Unsupported column schema for creating a sorted index. Column name: %s, column type: %s",
                        columnDescriptor.name(), columnDescriptor.type()
                ));
        }
    }
}
