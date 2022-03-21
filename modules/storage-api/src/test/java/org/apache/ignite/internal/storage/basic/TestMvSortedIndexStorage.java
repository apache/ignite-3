/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.basic;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.IntPredicate;
import java.util.function.ToIntFunction;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.PrefixComparator;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class TestMvSortedIndexStorage implements SortedIndexStorage {
    private final SortedIndexDescriptor descriptor;

    private final NavigableSet<BinaryRow> index;

    private final Map<Integer, TestMvPartitionStorage> pk;

    private final int partitions;

    protected TestMvSortedIndexStorage(SortedIndexDescriptor descriptor, Map<Integer, TestMvPartitionStorage> pk, int partitions) {
        this.descriptor = descriptor;
        this.pk = pk;
        this.partitions = partitions;

        index = new ConcurrentSkipListSet<>((l, r) -> {
            int cmp = compareColumns(l, r);

            if (cmp != 0) {
                return cmp;
            }

            return l.keySlice().compareTo(r.keySlice());
        });
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    private int compareColumns(BinaryRow l, BinaryRow r) {
        List<ColumnDescriptor> columnDescriptors = descriptor.indexRowColumns();

        Row lRow = new Row(descriptor.asSchemaDescriptor(), l);

        Object[] lTuple = convert(lRow, columnDescriptors, null);

        return new PrefixComparator(descriptor, () -> lTuple).compare(r);
    }

    public void append(BinaryRow row) {
        index.add(row);
    }

    public void remove(BinaryRow row) {
        index.remove(row);
    }

    public boolean matches(BinaryRow aborted, BinaryRow existing) {
        return compareColumns(aborted, existing) == 0;
    }

    @Override
    public Cursor<IndexRowEx> scan(
            @Nullable IndexRowPrefix lowerBound,
            @Nullable IndexRowPrefix upperBound,
            byte flags,
            Timestamp timestamp,
            @Nullable BitSet columnsProjection,
            @Nullable IntPredicate partitionFilter
    ) {
        if ((flags & BACKWARDS) != 0) {
            throw new IllegalArgumentException("flags");
        }

        ToIntFunction<BinaryRow> lowerCmp = lowerBound == null ? row -> -1 : new PrefixComparator(descriptor, lowerBound)::compare;
        ToIntFunction<BinaryRow> upperCmp = upperBound == null ? row -> -1 : new PrefixComparator(descriptor, upperBound)::compare;

        boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
        boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

        Iterator<IndexRowEx> iterator = index.stream()
                .dropWhile(binaryRow -> {
                    int cmp = lowerCmp.applyAsInt(binaryRow);

                    return includeLower && cmp < 0 || !includeLower && cmp <= 0;
                })
                .takeWhile(binaryRow -> {
                    int cmp = upperCmp.applyAsInt(binaryRow);

                    return includeUpper && cmp >= 0 || !includeUpper && cmp > 0;
                })
                .filter(binaryRow -> {
                    int partition = binaryRow.hash() % partitions;

                    // This code has been copy-pasted.
                    if (partition < 0) {
                        partition = -partition;
                    }

                    if (partitionFilter != null && !partitionFilter.test(partition)) {
                        return false;
                    }

                    TestMvPartitionStorage partitionStorage = pk.get(partition);

                    if (partitionStorage == null) {
                        return false;
                    }

                    BinaryRow pk = partitionStorage.read(binaryRow, timestamp);

                    return pk != null && matches(binaryRow, pk);
                })
                .map(binaryRow -> {
                    Object[] tuple = convert(
                            new Row(descriptor.asSchemaDescriptor(), binaryRow),
                            descriptor.indexRowColumns(),
                            columnsProjection
                    );

                    return (IndexRowEx) new IndexRowEx() {
                        @Override
                        public BinaryRow pk() {
                            return new KeyBinaryRow(binaryRow);
                        }

                        @Override
                        public Object value(int idx) {
                            return tuple[idx];
                        }
                    };
                })
                .iterator();

        return Cursor.fromIterator(iterator);
    }

    @NotNull
    private Object[] convert(Row row, List<ColumnDescriptor> columnDescriptors, BitSet projection) {
        int columns = projection == null ? columnDescriptors.size() : projection.cardinality();

        Object[] tuple = new Object[columns];

        for (int i = 0, j = 0; i < columnDescriptors.size(); i++) {
            if (projection != null && !projection.get(i)) {
                continue;
            }

            ColumnDescriptor columnDescriptor = columnDescriptors.get(i);

            Object columnValue = row.value(columnDescriptor.column().schemaIndex());

            tuple[j++] = columnValue;
        }

        return tuple;
    }

    private static final ByteBuffer NULL_VALUE = ByteBuffer.wrap(new byte[0]).order(ByteOrder.LITTLE_ENDIAN);

    private static class KeyBinaryRow implements BinaryRow {
        private final BinaryRow fullRow;

        private KeyBinaryRow(BinaryRow fullRow) {
            this.fullRow = fullRow;
        }

        @Override
        public int schemaVersion() {
            return 0;
        }

        @Override
        public boolean hasValue() {
            return false;
        }

        @Override
        public int hash() {
            return fullRow.hash();
        }

        @Override
        public ByteBuffer keySlice() {
            return fullRow.keySlice();
        }

        @Override
        public ByteBuffer valueSlice() {
            return NULL_VALUE;
        }

        @Override
        public void writeTo(OutputStream stream) throws IOException {
            stream.write(bytes());
        }

        @Override
        public byte[] bytes() {
            return Arrays.copyOf(fullRow.bytes(), BinaryRow.HEADER_SIZE + keySlice().limit());
        }
    }
}
