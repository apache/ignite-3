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

package org.apache.ignite.internal.storage.index.impl;

import static java.util.Comparator.comparing;
import static org.apache.ignite.internal.schema.SchemaTestUtils.generateRandomValue;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;

/**
 * Convenience wrapper over an Index row.
 */
public class TestIndexRow implements IndexRow, Comparable<TestIndexRow> {
    /**
     * Values used to create the Index row.
     */
    private final Object[] columns;

    private final IndexRow row;

    private final SortedIndexStorage indexStorage;

    private final BinaryTupleRowSerializer serializer;

    /** Constructor. */
    public TestIndexRow(SortedIndexStorage storage, BinaryTupleRowSerializer serializer, IndexRow row, Object[] columns) {
        this.indexStorage = storage;
        this.serializer = serializer;
        this.row = row;
        this.columns = columns;
    }

    /**
     * Creates an row with random column values that satisfies the given schema.
     */
    public static TestIndexRow randomRow(SortedIndexStorage indexStorage, int partitionId) {
        Object[] columns = indexStorage.indexDescriptor().columns().stream()
                .map(StorageSortedIndexColumnDescriptor::type)
                .map(type -> generateRandomValue(ThreadLocalRandom.current(), type))
                .toArray();

        var rowId = new RowId(partitionId);

        var serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        IndexRow row = serializer.serializeRow(columns, rowId);

        return new TestIndexRow(indexStorage, serializer, row, columns);
    }

    /**
     * Class representing an Index Key prefix.
     */
    public static class TestIndexPrefix {
        private final BinaryTuplePrefix prefix;

        private final Object[] prefixColumns;

        TestIndexPrefix(BinaryTuplePrefix prefix, Object[] prefixColumns) {
            this.prefix = prefix;
            this.prefixColumns = prefixColumns;
        }

        public BinaryTuplePrefix prefix() {
            return prefix;
        }

        public Object[] prefixColumns() {
            return prefixColumns;
        }
    }

    /**
     * Creates an Index Key prefix of the given length.
     */
    public TestIndexPrefix prefix(int length) {
        Object[] prefixColumns = Arrays.copyOf(columns, length);

        return new TestIndexPrefix(serializer.serializeRowPrefix(prefixColumns), prefixColumns);
    }

    @Override
    public BinaryTuple indexColumns() {
        return row.indexColumns();
    }

    @Override
    public RowId rowId() {
        return row.rowId();
    }

    @Override
    public int compareTo(TestIndexRow o) {
        int sizeCompare = Integer.compare(columns.length, o.columns.length);

        if (sizeCompare != 0) {
            return sizeCompare;
        }

        return compareColumns(columns, o.columns);
    }

    /**
     * Compares the row with the given row prefix.
     */
    public int compareTo(TestIndexPrefix prefix) {
        Object[] prefixColumns = prefix.prefixColumns();

        assert prefixColumns.length <= columns.length;

        return compareColumns(columns, prefixColumns);
    }

    private int compareColumns(Object[] a, Object[] b) {
        for (int i = 0; i < Math.min(a.length, b.length); ++i) {
            Comparator<Object> comparator = comparator(a[i].getClass());

            int compare = comparator.compare(a[i], b[i]);

            if (compare != 0) {
                boolean asc = indexStorage.indexDescriptor().columns().get(i).asc();

                return asc ? compare : -compare;
            }
        }

        return 0;
    }

    /**
     * Compares values generated by {@link SchemaTestUtils#generateRandomValue}.
     */
    private static Comparator<Object> comparator(Class<?> type) {
        if (Comparable.class.isAssignableFrom(type)) {
            return comparingNull(Comparable.class::cast, Comparator.naturalOrder());
        } else if (type.isArray()) {
            return comparingNull(Function.identity(), comparing(TestIndexRow::toBoxedArray, Arrays::compare));
        } else if (BitSet.class.isAssignableFrom(type)) {
            return comparingNull(BitSet.class::cast, comparing(BitSet::toLongArray, Arrays::compare));
        } else {
            throw new IllegalArgumentException("Non comparable class: " + type);
        }
    }

    /**
     * Creates a comparator similar to {@link Comparator#comparing(Function, Comparator)}, but allows the key extractor functions
     * to return {@code null}.
     *
     * <p>Null values are always treated as smaller than the non-null values.
     */
    private static <T, U> Comparator<T> comparingNull(Function<? super T, ? extends U> keyExtractor, Comparator<? super U> keyComparator) {
        return (o1, o2) -> {
            U key1 = keyExtractor.apply(o1);
            U key2 = keyExtractor.apply(o2);

            if (key1 == key2) {
                return 0;
            } else if (key1 == null) {
                return 1;
            } else if (key2 == null) {
                return -1;
            } else {
                return keyComparator.compare(key1, key2);
            }
        };
    }

    /**
     * Creates a new array of boxed primitives if the given Object is an array of primitives or simply copies the array otherwise.
     */
    private static Comparable[] toBoxedArray(Object array) {
        return IntStream.range(0, Array.getLength(array))
                .mapToObj(i -> Array.get(array, i))
                .map(Comparable.class::cast)
                .toArray(Comparable[]::new);
    }
}
