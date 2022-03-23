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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.IntPredicate;
import java.util.function.ToIntFunction;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.PrefixComparator;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexMvStorage;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV sorted index storage.
 */
public class TestMvSortedIndexStorage implements SortedIndexMvStorage {
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

    /** {@inheritDoc} */
    @Override
    public boolean supportsBackwardsScan() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsIndexOnlyScan() {
        return false;
    }

    private int compareColumns(BinaryRow l, BinaryRow r) {
        Object[] leftTuple = convert(l);

        return new PrefixComparator(descriptor, () -> leftTuple).compare(r);
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

    /** {@inheritDoc} */
    @Override
    public Cursor<IndexRowEx> scan(
            @Nullable IndexRowPrefix lowerBound,
            @Nullable IndexRowPrefix upperBound,
            byte flags,
            Timestamp timestamp,
            @Nullable IntPredicate partitionFilter
    ) {
        boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
        boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

        NavigableSet<BinaryRow> index = this.index;

        // Swap bounds and flip index for backwards scan.
        if ((flags & BACKWARDS) != 0) {
            index = index.descendingSet();

            boolean tempBoolean = includeLower;
            includeLower = includeUpper;
            includeUpper = tempBoolean;

            IndexRowPrefix tempBound = lowerBound;
            lowerBound = upperBound;
            upperBound = tempBound;
        }

        ToIntFunction<BinaryRow> lowerCmp = lowerBound == null ? row -> -1 : new PrefixComparator(descriptor, lowerBound)::compare;
        ToIntFunction<BinaryRow> upperCmp = upperBound == null ? row -> -1 : new PrefixComparator(descriptor, upperBound)::compare;

        boolean includeLower0 = includeLower;
        boolean includeUpper0 = includeUpper;

        Iterator<IndexRowEx> iterator = index.stream()
                .dropWhile(binaryRow -> {
                    int cmp = lowerCmp.applyAsInt(binaryRow);

                    return includeLower0 && cmp < 0 || !includeLower0 && cmp <= 0;
                })
                .takeWhile(binaryRow -> {
                    int cmp = upperCmp.applyAsInt(binaryRow);

                    return includeUpper0 && cmp >= 0 || !includeUpper0 && cmp > 0;
                })
                .map(binaryRow -> {
                    int partition = binaryRow.hash() % partitions;

                    if (partition < 0) {
                        partition = -partition;
                    }

                    if (partitionFilter != null && !partitionFilter.test(partition)) {
                        return null;
                    }

                    TestMvPartitionStorage partitionStorage = pk.get(partition);

                    if (partitionStorage == null) {
                        return null;
                    }

                    BinaryRow pk = partitionStorage.read(binaryRow, timestamp);

                    return matches(binaryRow, pk) ? pk : null;
                })
                .filter(Objects::nonNull)
                .map(binaryRow -> {
                    Object[] tuple = convert(binaryRow);

                    return (IndexRowEx) new IndexRowEx() {
                        @Override
                        public BinaryRow row() {
                            return binaryRow;
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

    private Object[] convert(BinaryRow binaryRow) {
        List<ColumnDescriptor> columnDescriptors = descriptor.indexRowColumns();

        int columns = columnDescriptors.size();

        Object[] tuple = new Object[columns];

        Row row = new Row(descriptor.asSchemaDescriptor(), binaryRow);

        for (int i = 0; i < columns; i++) {
            ColumnDescriptor columnDescriptor = columnDescriptors.get(i);

            Object columnValue = row.value(columnDescriptor.column().schemaIndex());

            tuple[i] = columnValue;
        }

        return tuple;
    }
}
