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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.IntPredicate;
import java.util.function.ToIntFunction;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.configuration.SchemaDescriptorConverter;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.basic.TestMvPartitionStorage.TestRowId;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.PrefixComparator;
import org.apache.ignite.internal.storage.index.SortedIndexMvStorage;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV sorted index storage.
 */
public class TestSortedIndexMvStorage implements SortedIndexMvStorage {
    private final NavigableSet<Pair<BinaryRow, TestRowId>> index;

    private final SchemaDescriptor descriptor;

    private final Map<Integer, TestMvPartitionStorage> pk;

    private final int partitions;

    private final IndexColumnView[] indexColumns;

    private final int[] columnIndexes;

    private final NativeType[] nativeTypes;

    /**
     * Constructor.
     */
    public TestSortedIndexMvStorage(
            String name,
            TableView tableCfg,
            SchemaDescriptor descriptor,
            Map<Integer, TestMvPartitionStorage> pk
    ) {
        this.descriptor = descriptor;

        this.pk = pk;

        partitions = tableCfg.partitions();

        index = new ConcurrentSkipListSet<>(
                ((Comparator<Pair<BinaryRow, TestRowId>>) (p1, p2) -> {
                    return compareColumns(p1.getFirst(), p2.getFirst());
                })
                .thenComparing(pair -> pair.getSecond().uuid)
        );

        // Init columns.
        NamedListView<? extends ColumnView> tblColumns = tableCfg.columns();

        TableIndexView idxCfg = tableCfg.indices().get(name);

        assert idxCfg instanceof SortedIndexView;

        SortedIndexView sortedIdxCfg = (SortedIndexView) idxCfg;

        NamedListView<? extends IndexColumnView> columns = sortedIdxCfg.columns();

        int length = columns.size();

        this.indexColumns = new IndexColumnView[length];
        this.columnIndexes = new int[length];
        this.nativeTypes = new NativeType[length];

        for (int i = 0; i < length; i++) {
            IndexColumnView idxColumn = columns.get(i);

            indexColumns[i] = idxColumn;

            int columnIndex = tblColumns.namedListKeys().indexOf(idxColumn.name());

            columnIndexes[i] = columnIndex;

            nativeTypes[i] = SchemaDescriptorConverter.convert(SchemaConfigurationConverter.convert(tblColumns.get(columnIndex).type()));
        }
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
        Row leftRow = new Row(descriptor, l);
        Row rightRow = new Row(descriptor, r);

        for (int i = 0; i < indexColumns.length; i++) {
            int columnIndex = columnIndexes[i];

            int cmp = PrefixComparator.compareColumns(leftRow, columnIndex, nativeTypes[i].spec(), rightRow.value(columnIndex));

            if (cmp != 0) {
                return indexColumns[i].asc() ? cmp : -cmp;
            }
        }

        return 0;
    }

    public void append(BinaryRow row, RowId rowId) {
        index.add(new Pair<>(row, (TestRowId) rowId));
    }

    public void remove(BinaryRow row, RowId rowId) {
        index.remove(new Pair<>(row, (TestRowId) rowId));
    }

    public boolean matches(BinaryRow aborted, BinaryRow existing) {
        return existing != null && compareColumns(aborted, existing) == 0;
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<IndexRowEx> scan(
            @Nullable IndexRowPrefix lowerBound,
            @Nullable IndexRowPrefix upperBound,
            int flags,
            UUID txId,
            @Nullable IntPredicate partitionFilter
    ) {
        return scan(lowerBound, upperBound, flags, null, txId, partitionFilter);
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<IndexRowEx> scan(
            @Nullable IndexRowPrefix lowerBound,
            @Nullable IndexRowPrefix upperBound,
            int flags,
            Timestamp timestamp,
            @Nullable IntPredicate partitionFilter
    ) {
        return scan(lowerBound, upperBound, flags, timestamp, null, partitionFilter);
    }

    private Cursor<IndexRowEx> scan(
            @Nullable IndexRowPrefix lowerBound,
            @Nullable IndexRowPrefix upperBound,
            int flags,
            Timestamp timestamp,
            UUID txId,
            @Nullable IntPredicate partitionFilter
    ) {
        assert timestamp != null ^ txId != null;

        boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
        boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

        NavigableSet<Pair<BinaryRow, TestRowId>> index = this.index;
        int direction = 1;

        // Swap bounds and flip index for backwards scan.
        if ((flags & BACKWARDS) != 0) {
            index = index.descendingSet();
            direction = -1;

            boolean tempBoolean = includeLower;
            includeLower = includeUpper;
            includeUpper = tempBoolean;

            IndexRowPrefix tempBound = lowerBound;
            lowerBound = upperBound;
            upperBound = tempBound;
        }

        ToIntFunction<BinaryRow> lowerCmp = lowerBound == null ? row -> 1 : boundComparator(lowerBound, direction, includeLower ? 0 : -1);
        ToIntFunction<BinaryRow> upperCmp = upperBound == null ? row -> -1 : boundComparator(upperBound, direction, includeUpper ? 0 : 1);

        Iterator<IndexRowEx> iterator = index.stream()
                .dropWhile(p -> lowerCmp.applyAsInt(p.getFirst()) < 0)
                .takeWhile(p -> upperCmp.applyAsInt(p.getFirst()) <= 0)
                .map(p -> {
                    int partition = p.getSecond().partitionId();

                    if (partitionFilter != null && !partitionFilter.test(partition)) {
                        return null;
                    }

                    TestMvPartitionStorage partitionStorage = pk.get(partition);

                    if (partitionStorage == null) {
                        return null;
                    }

                    try {
                        BinaryRow pk = timestamp != null
                                ? partitionStorage.read(p.getSecond(), timestamp)
                                : partitionStorage.read(p.getSecond(), txId);

                        return matches(p.getFirst(), pk) ? pk : null;
                    } catch (TxIdMismatchException e) {
                        // False-positive, old comitted value found that's already been updated in another transaction.
                        // See "org.apache.ignite.internal.storage.AbstractSortedIndexMvStorageTest.textScanFiltersMismatchedRows"
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .map(binaryRow -> {
                    Row row = new Row(descriptor, binaryRow);

                    return (IndexRowEx) new IndexRowEx() {
                        @Override
                        public BinaryRow row() {
                            return binaryRow;
                        }

                        @Override
                        public Object value(int idx) {
                            return row.value(columnIndexes[idx]);
                        }
                    };
                })
                .iterator();

        return Cursor.fromIterator(iterator);
    }

    private ToIntFunction<BinaryRow> boundComparator(IndexRowPrefix bound, int direction, int equals) {
        return binaryRow -> {
            Object[] values = bound.prefixColumnValues();

            Row row = new Row(descriptor, binaryRow);

            for (int i = 0; i < values.length; i++) {
                int columnIndex = columnIndexes[i];

                int cmp = PrefixComparator.compareColumns(row, columnIndex, nativeTypes[i].spec(), values[i]);

                if (cmp != 0) {
                    return direction * (indexColumns[i].asc() ? cmp : -cmp);
                }
            }

            return equals;
        };
    }
}
