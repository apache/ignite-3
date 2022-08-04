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

import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingDouble;
import static java.util.Comparator.comparingInt;
import static java.util.Comparator.comparingLong;
import static org.apache.ignite.internal.storage.index.impl.ComparatorUtils.comparingNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;

/**
 * Comparator implementation for comparting {@link BinaryTuple}s on a per-column basis.
 */
class BinaryTupleComparator implements Comparator<ByteBuffer> {
    /**
     * Actual comparator implementation.
     */
    private final Comparator<ByteBuffer> delegate;

    /** Index descriptor. */
    private final SortedIndexDescriptor indexDescriptor;

    /**
     * Creates a comparator for a Sorted Index identified by the given descriptor.
     */
    BinaryTupleComparator(SortedIndexDescriptor descriptor, BinaryTupleSchema schema) {
        this.indexDescriptor = descriptor;

        this.delegate = comparing(bytes -> new BinaryTuple(schema, bytes), binaryTupleComparator());
    }

    /**
     * Creates a comparator that compares two {@link BinaryTuple}s by comparing individual columns.
     */
    private Comparator<BinaryTuple> binaryTupleComparator() {
        return IntStream.range(0, indexDescriptor.indexColumns().size())
                .mapToObj(i -> {
                    ColumnDescriptor columnDescriptor = indexDescriptor.indexColumns().get(i);

                    Comparator<BinaryTuple> fieldComparator = tupleFieldComparator(i, columnDescriptor.type());

                    if (columnDescriptor.nullable()) {
                        fieldComparator = comparingNull(
                                tuple -> tuple.hasNullValue(i) ? null : tuple,
                                fieldComparator
                        );
                    }

                    return columnDescriptor.asc() ? fieldComparator : fieldComparator.reversed();
                })
                .reduce(Comparator::thenComparing)
                .orElseThrow();
    }

    /**
     * Creates a comparator for comparing table columns.
     */
    private Comparator<BinaryTuple> tupleFieldComparator(int index, NativeType type) {
        switch (type.spec()) {
            case INT8:
                return (tuple1, tuple2) -> {
                    byte value1 = tuple1.byteValue(index);
                    byte value2 = tuple2.byteValue(index);

                    return Byte.compare(value1, value2);
                };

            case INT16:
                return (tuple1, tuple2) -> {
                    short value1 = tuple1.shortValue(index);
                    short value2 = tuple2.shortValue(index);

                    return Short.compare(value1, value2);
                };

            case INT32:
                return comparingInt(tuple -> tuple.intValue(index));

            case INT64:
                return comparingLong(tuple -> tuple.longValue(index));

            case FLOAT:
                return (tuple1, tuple2) -> {
                    float value1 = tuple1.floatValue(index);
                    float value2 = tuple2.floatValue(index);

                    return Float.compare(value1, value2);
                };

            case DOUBLE:
                return comparingDouble(tuple -> tuple.doubleValue(index));

            case BYTES:
                return comparing(tuple -> tuple.bytesValue(index), Arrays::compare);

            case BITMASK:
                return comparing(tuple -> tuple.bitmaskValue(index).toLongArray(), Arrays::compare);

            // all other types implement Comparable
            case DECIMAL:
            case UUID:
            case STRING:
            case NUMBER:
            case TIMESTAMP:
            case DATE:
            case TIME:
            case DATETIME:
                return comparing(tuple -> (Comparable) type.spec().objectValue(tuple, index));

            default:
                ColumnDescriptor columnDescriptor = indexDescriptor.indexColumns().get(index);

                throw new IllegalArgumentException(String.format(
                        "Unsupported column schema for creating a sorted index. Column name: %s, column type: %s",
                        columnDescriptor.name(), columnDescriptor.type()
                ));
        }
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2) {
        return delegate.compare(o1, o2);
    }
}
