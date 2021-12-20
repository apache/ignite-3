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

package org.apache.ignite.internal.storage.rocksdb.index;

import static java.util.Comparator.comparing;
import static org.apache.ignite.internal.schema.SchemaTestUtils.generateRandomValue;
import static org.apache.ignite.internal.storage.rocksdb.index.ComparatorUtils.comparingNull;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.index.IndexBinaryRow;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * Convenience wrapper over an Index row.
 */
class IndexRowWrapper implements Comparable<IndexRowWrapper> {
    /**
     * Values used to create the Index row.
     */
    private final Tuple row;

    private final IndexRow idxRow;

    private final SortedIndexDescriptor desc;

    IndexRowWrapper(SortedIndexStorage storage, IndexBinaryRow idxRow, Tuple row) {
        this.desc = storage.indexDescriptor();
        this.idxRow = new IndexRowImpl(idxRow, desc);
        this.row = row;
    }

    /**
     * Creates an Entry with a random key that satisfies the given schema and a random value.
     */
    static IndexRowWrapper randomRow(SortedIndexStorage indexStorage) {
        var random = new Random();

        Tuple row = Tuple.create();

        indexStorage.indexDescriptor().columns().stream()
                .map(SortedIndexColumnDescriptor::column)
                .forEach(column -> row.set(column.name(), generateRandomValue(random, column.type())));

        var primaryKey = new ByteArraySearchRow(randomBytes(random, 25));

        IndexBinaryRow idxRow = indexStorage.indexRowFactory().createIndexRow(row, primaryKey);

        return new IndexRowWrapper(indexStorage, idxRow, row);
    }

    /**
     * Creates an Index Key prefix of the given length.
     */
    IndexRowPrefix prefix(int length) {
        return new IndexRowPrefix() {
            @Override
            public int length() {
                return length;
            }

            @Override
            public Object value(int idxColOrder) {
                return idxRow.value(idxColOrder);
            }

            @Override
            public int columnsCount() {
                return length;
            }

            @Override
            public byte[] rowBytes() {
                return idxRow.rowBytes();
            }

            @Override
            public SearchRow primaryKey() {
                return idxRow.primaryKey();
            }
        };
    }

    IndexBinaryRow row() {
        return idxRow;
    }

    @Override
    public int compareTo(@NotNull IndexRowWrapper o) {
        int sizeCompare = Integer.compare(row.columnCount(), o.row.columnCount());

        if (sizeCompare != 0) {
            return sizeCompare;
        }

        for (int i = 0; i < row.columnCount(); ++i) {
            Comparator<Object> comparator = comparator(row.value(i).getClass());

            int compare = comparator.compare(row.value(i), o.row.value(i));

            if (compare != 0) {
                boolean asc = desc.columns().get(i).asc();

                return asc ? compare : -compare;
            }
        }

        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexRowWrapper that = (IndexRowWrapper) o;
        return idxRow.equals(that.idxRow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idxRow);
    }

    /**
     * Compares values generated by {@link SchemaTestUtils#generateRandomValue}.
     */
    private static Comparator<Object> comparator(Class<?> type) {
        if (Comparable.class.isAssignableFrom(type)) {
            return comparingNull(Comparable.class::cast, Comparator.naturalOrder());
        } else if (type.isArray()) {
            return comparingNull(Function.identity(), comparing(IndexRowWrapper::toBoxedArray, Arrays::compare));
        } else if (BitSet.class.isAssignableFrom(type)) {
            return comparingNull(BitSet.class::cast, comparing(BitSet::toLongArray, Arrays::compare));
        } else {
            throw new IllegalArgumentException("Non comparable class: " + type);
        }
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
