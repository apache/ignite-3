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
import java.util.BitSet;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Class for comparing a {@link BinaryTuple} representing an Index Row with a given prefix of index columns.
 */
class PrefixComparator {
    private final SortedIndexDescriptor descriptor;

    private final @Nullable Object[] prefix;

    /**
     * Creates a new prefix comparator.
     *
     * @param descriptor Index Descriptor of the enclosing index.
     * @param prefix Prefix to compare the incoming tuples against.
     */
    PrefixComparator(SortedIndexDescriptor descriptor, IndexRowPrefix prefix) {
        assert descriptor.indexColumns().size() >= prefix.prefixColumnValues().length;

        this.descriptor = descriptor;
        this.prefix = prefix.prefixColumnValues();
    }

    /**
     * Compares a given tuple with the configured prefix.
     *
     * @param tuple Tuple to compare.
     * @param direction Sort direction: {@code -1} means sorting in reversed order, {@code 1} means  sorting in the natural order.
     * @param equals Value that should be returned if the provided tuple exactly matches the prefix.
     * @return the value {@code 0} if the given row starts with the configured prefix;
     *         a value less than {@code 0} if the row's prefix is smaller than the prefix; and
     *         a value greater than {@code 0} if the row's prefix is larger than the prefix.
     */
    int compare(BinaryTuple tuple, int direction, int equals) {
        for (int i = 0; i < prefix.length; ++i) {
            ColumnDescriptor columnDescriptor = descriptor.indexColumns().get(i);

            int compare = compareFields(tuple, i, columnDescriptor.type(), prefix[i]);

            if (compare != 0) {
                return direction * (columnDescriptor.asc() ? compare : -compare);
            }
        }

        return equals;
    }

    /**
     * Compares a particular column of a {@code row} with the given value.
     */
    private static int compareFields(BinaryTuple tuple, int index, NativeType type, @Nullable Object value) {
        boolean nullRow = tuple.hasNullValue(index);

        if (nullRow && value == null) {
            return 0;
        } else if (nullRow) {
            return -1;
        } else if (value == null) {
            return 1;
        }

        switch (type.spec()) {
            case INT8:
                return Byte.compare(tuple.byteValue(index), (Byte) value);

            case INT16:
                return Short.compare(tuple.shortValue(index), (Short) value);

            case INT32:
                return Integer.compare(tuple.intValue(index), (Integer) value);

            case INT64:
                return Long.compare(tuple.longValue(index), (Long) value);

            case FLOAT:
                return Float.compare(tuple.floatValue(index), (Float) value);

            case DOUBLE:
                return Double.compare(tuple.doubleValue(index), (Double) value);

            case BYTES:
                return Arrays.compare(tuple.bytesValue(index), (byte[]) value);

            case BITMASK:
                return Arrays.compare(tuple.bitmaskValue(index).toLongArray(), ((BitSet) value).toLongArray());

            // all other types implement Comparable
            case DECIMAL:
            case UUID:
            case STRING:
            case NUMBER:
            case TIMESTAMP:
            case DATE:
            case TIME:
            case DATETIME:
                return ((Comparable) type.spec().objectValue(tuple, index)).compareTo(value);

            default:
                throw new AssertionError("Unknown type spec: " + type.spec());
        }
    }
}
