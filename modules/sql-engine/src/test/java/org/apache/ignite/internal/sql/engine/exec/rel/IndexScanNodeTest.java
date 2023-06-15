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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.index.ColumnCollation;
import org.apache.ignite.internal.index.HashIndex;
import org.apache.ignite.internal.index.Index;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.index.SortedIndexDescriptor;
import org.apache.ignite.internal.index.SortedIndexImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.table.InternalTable;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test {@link IndexScanNode} execution.
 */
public class IndexScanNodeTest extends AbstractExecutionTest {

    /**
     * Sorted index.
     */
    @Test
    public void testSortedIndex() {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
                .add("C1", SqlTypeName.INTEGER)
                .build();

        Tester tester = new Tester(rowType);

        tester.addData(0, new Object[]{4}, new Object[]{5});
        tester.addData(2, new Object[]{1}, new Object[]{2});

        tester.sortedIndex("C1");

        tester.expectResult(new Object[]{1}, new Object[]{2}, new Object[]{4}, new Object[]{5});
    }

    /**
     * Hash index.
     */
    @Test
    public void testHashIndex() {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
                .add("C1", SqlTypeName.INTEGER)
                .build();

        Tester tester = new Tester(rowType);

        tester.addData(0, new Object[]{2}, new Object[]{1});
        tester.addData(2, new Object[]{0});

        tester.hashIndex("C1");

        tester.expectResult(new Object[]{2}, new Object[]{1}, new Object[]{0});
    }

    private class Tester {

        private final TestScannableTable<Object[]> scannableTable = new TestScannableTable<>(Function.identity());

        private final RelDataType rowType;

        private final ExecutionContext<Object[]> ctx = executionContext();

        private final List<PartitionWithTerm> partitions = new ArrayList<>();

        private IndexScanNode<Object[]> indexNode;

        Tester(RelDataType rowType) {
            this.rowType = rowType;
        }

        void addData(int partId, Object[]... rows) {
            scannableTable.setPartitionData(partId, rows);

            partitions.removeIf(p -> p.partId() == partId);
            partitions.add(new PartitionWithTerm(partId, 1L));
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        void sortedIndex(String columnName) {
            int idx = -1;
            List<RelDataTypeField> fieldList = rowType.getFieldList();

            for (int i = 0; i < fieldList.size(); i++) {
                if (fieldList.get(i).getName().equals(columnName)) {
                    idx = i;
                    break;
                }
            }

            int j = idx;

            Comparator<Object[]> cmp = (a, b) -> {
                Comparable o1 = (Comparable<?>) a[j];
                Comparable o2 = (Comparable<?>) b[j];
                return o1.compareTo(o2);
            };

            indexNode = new IndexBuilder(rowType)
                    .addColumn(columnName)
                    .setComparator(cmp)
                    .build(ctx, scannableTable, partitions);
        }

        void hashIndex(String columnName) {
            indexNode = new IndexBuilder(rowType)
                    .addColumn(columnName)
                    .build(ctx, scannableTable, partitions);
        }

        void expectResult(Object[]... rows) {
            RootNode<Object[]> root = new RootNode<>(ctx);

            root.register(indexNode);

            int cnt = 0;

            List<Object[]> actual = new ArrayList<>();
            while (root.hasNext()) {
                Object[] row = root.next();
                ++cnt;
                actual.add(row);
            }

            assertEquals(rows.length, cnt, "row count");

            root.close();

            for (int i = 0; i < rows.length; i++) {
                Object[] expectedRow = rows[i];
                Object[] actualRow = actual.get(i);

                assertArrayEquals(expectedRow, actualRow, "Row#" + i);
            }
        }
    }

    private static class TestTable extends AbstractPlannerTest.TestTable {
        TestTable(RelDataType rowType) {
            super(rowType);
        }

        @Override
        public IgniteDistribution distribution() {
            return IgniteDistributions.broadcast();
        }
    }

    private static final class IndexBuilder {

        private final RelDataType rowType;

        private final List<Map.Entry<String, ColumnCollation>> indexColumns = new ArrayList<>();

        private Comparator<Object[]> comparator;

        IndexBuilder(RelDataType rowType) {
            this.rowType = rowType;
        }

        IndexBuilder addColumn(String name) {
            indexColumns.add(Map.entry(name, ColumnCollation.ASC_NULLS_LAST));
            return this;
        }

        IndexBuilder setComparator(Comparator<Object[]> comparator) {
            this.comparator = comparator;
            return this;
        }

        IndexScanNode<Object[]> build(ExecutionContext<Object[]> ctx,
                ScannableTable scannableTable,
                List<PartitionWithTerm> partitions) {

            Index<?> index;

            if (comparator == null) {
                index = newHashIndex();
            } else {
                index = newSortedIndex();
            }

            IgniteIndex schemaIndex = new IgniteIndex(index);
            RowFactory<Object[]> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);
            TestTable table = new TestTable(rowType);
            SingleRangeIterable conditions = new SingleRangeIterable(new Object[]{}, null, false, false);

            return new IndexScanNode<>(ctx, rowFactory, schemaIndex, scannableTable, table.descriptor(), partitions,
                    comparator, conditions, null, null, null);
        }

        private Index<?> newHashIndex() {
            IndexDescriptor descriptor = new IndexDescriptor("IDX", indexColumns.stream().map(Entry::getKey).collect(Collectors.toList()));

            return new HashIndex(1, Mockito.mock(InternalTable.class), descriptor);
        }

        private Index<?> newSortedIndex() {
            List<String> columnNames = indexColumns.stream().map(Entry::getKey).collect(Collectors.toList());
            List<ColumnCollation> columnCollations = indexColumns.stream().map(Entry::getValue).collect(Collectors.toList());
            SortedIndexDescriptor descriptor = new SortedIndexDescriptor("IDX", columnNames, columnCollations);

            return new SortedIndexImpl(1, Mockito.mock(InternalTable.class), descriptor);
        }
    }

    static class TestScannableTable<T> implements ScannableTable {

        private final Map<Integer, List<T>> partitionedData = new ConcurrentHashMap<>();

        private final Function<T, ?> rowConverter;

        TestScannableTable(Function<T, ?> rowConverter) {
            this.rowConverter = rowConverter;
        }

        void setPartitionData(int partitionId, T... rows) {
            partitionedData.put(partitionId, List.of(rows));
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm, RowFactory<RowT> rowFactory,
                @Nullable BitSet requiredColumns) {

            throw new UnsupportedOperationException("Not supported");
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
                RowFactory<RowT> rowFactory, int indexId, String indexName, List<String> columns,
                @Nullable RangeCondition<RowT> cond, @Nullable BitSet requiredColumns) {

            List<T> list = partitionedData.get(partWithTerm.partId());
            return new ScanPublisher<>(list, ctx, rowFactory);
        }

        @Override
        public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
                RowFactory<RowT> rowFactory, int indexId, String indexName, List<String> columns,
                RowT key, @Nullable BitSet requiredColumns) {

            return newPublisher(ctx, partWithTerm, rowFactory);
        }

        private <RowT> ScanPublisher<RowT> newPublisher(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
                RowFactory<RowT> rowFactory) {

            int partId = partWithTerm.partId();
            List<T> list = partitionedData.get(partId);
            Objects.requireNonNull(list, "No data for partition " + partId);

            return new ScanPublisher<>(list, ctx, rowFactory);
        }

        private final class ScanPublisher<R> implements Publisher<R> {

            final List<T> rows;

            final ExecutionContext<R> ctx;

            final RowFactory<R> rowFactory;

            ScanPublisher(List<T> rows, ExecutionContext<R> ctx, RowFactory<R> rowFactory) {
                this.rows = rows;
                this.ctx = ctx;
                this.rowFactory = rowFactory;
            }

            @Override
            public void subscribe(Subscriber<? super R> subscriber) {
                subscriber.onSubscribe(new Subscription() {
                    int off = 0;
                    boolean completed = false;

                    @Override
                    public void request(long n) {
                        int start = off;
                        int end = Math.min(start + (int) n, rows.size());

                        off = end;

                        for (int i = start; i < end; i++) {
                            T row = rows.get(i);
                            R internalRow = (R) rowConverter.apply(row);
                            subscriber.onNext(internalRow);
                        }

                        if (off >= rows.size() && !completed) {
                            completed = true;

                            subscriber.onComplete();
                        }
                    }

                    @Override
                    public void cancel() {
                        // No-op.
                    }
                });
            }
        }
    }
}
