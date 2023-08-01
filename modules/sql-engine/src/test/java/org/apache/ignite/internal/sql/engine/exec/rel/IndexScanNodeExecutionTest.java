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
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
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
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest.TestTableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchemaIndex;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.table.InternalTable;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test {@link IndexScanNode} execution.
 */
public class IndexScanNodeExecutionTest extends AbstractExecutionTest {

    /**
     * Sorted index scan execution.
     */
    @Test
    public void testSortedIndex() {
        Index<?> index = newSortedIndex();

        ExecutionContext<Object[]> ctx = executionContext();

        Tester tester = new Tester(ctx);

        TestScannableTable<Object[]> scannableTable = new TestScannableTable<>();
        scannableTable.setPartitionData(0, new Object[]{4}, new Object[]{5});
        scannableTable.setPartitionData(2, new Object[]{1}, new Object[]{2});

        Comparator<Object[]> cmp = Comparator.comparing(row -> (Comparable<Object>) row[0]);

        IndexScanNode<Object[]> node = tester.createSortedIndex(index, scannableTable, cmp);
        List<Object[]> result = tester.execute(node);

        validateResult(result, List.of(new Object[]{1}, new Object[]{2}, new Object[]{4}, new Object[]{5}));
    }

    /**
     * Hash index lookup execution.
     */
    @Test
    public void testHashIndex() {
        Index<?> index = newHashIndex();

        ExecutionContext<Object[]> ctx = executionContext();

        Tester tester = new Tester(ctx);

        TestScannableTable<Object[]> scannableTable = new TestScannableTable<>();
        scannableTable.setPartitionData(0, new Object[]{2}, new Object[]{1});
        scannableTable.setPartitionData(2, new Object[]{0});

        IndexScanNode<Object[]> node = tester.createHashIndex(index, scannableTable);
        List<Object[]> result = tester.execute(node);

        validateResult(result, List.of(new Object[]{2}, new Object[]{1}, new Object[]{0}));
    }

    private class Tester {

        private final ExecutionContext<Object[]> ctx;

        Tester(ExecutionContext<Object[]> ctx) {
            this.ctx = ctx;
        }

        IndexScanNode<Object[]> createSortedIndex(Index<?> index, TestScannableTable<?> scannableTable, Comparator<Object[]> cmp) {
            return createIndexNode(ctx, index, scannableTable, cmp);
        }

        IndexScanNode<Object[]> createHashIndex(Index<?> index, TestScannableTable<?> scannableTable) {
            return createIndexNode(ctx, index, scannableTable, null);
        }

        List<Object[]> execute(IndexScanNode<Object[]> indexNode) {
            RootNode<Object[]> root = new RootNode<>(ctx);

            root.register(indexNode);

            List<Object[]> actual = new ArrayList<>();
            while (root.hasNext()) {
                Object[] row = root.next();
                actual.add(row);
            }

            root.close();

            return actual;
        }
    }

    static void validateResult(List<Object[]> actual, List<Object[]> expected) {
        assertEquals(expected.size(), actual.size(), "row count");

        for (int i = 0; i < expected.size(); i++) {
            Object[] expectedRow = expected.get(i);
            Object[] actualRow = actual.get(i);

            assertArrayEquals(expectedRow, actualRow, "Row#" + i);
        }
    }

    private Index<?> newHashIndex() {
        IndexDescriptor descriptor = new IndexDescriptor("IDX", List.of("C1"));

        return new HashIndex(1, Mockito.mock(InternalTable.class), descriptor);
    }

    private Index<?> newSortedIndex() {
        List<String> columnNames = List.of("C1");
        List<ColumnCollation> columnCollations = List.of(ColumnCollation.ASC_NULLS_LAST);
        SortedIndexDescriptor descriptor = new SortedIndexDescriptor("IDX", columnNames, columnCollations);

        return new SortedIndexImpl(1, Mockito.mock(InternalTable.class), descriptor);
    }

    private IndexScanNode<Object[]> createIndexNode(ExecutionContext<Object[]> ctx, Index<?> index,
            TestScannableTable<?> scannableTable, @Nullable Comparator<Object[]> comparator) {

        RelDataTypeFactory.Builder rowTypeBuilder = new Builder(Commons.typeFactory());

        for (String column : index.descriptor().columns()) {
            rowTypeBuilder = rowTypeBuilder.add(column, SqlTypeName.INTEGER);
        }

        RelDataType rowType = rowTypeBuilder.build();

        TableDescriptor tableDescriptor = new TestTableDescriptor(IgniteDistributions::single, rowType);

        //TODO: avoid IgniteIndex creation.
        IgniteIndex idx = new IgniteIndex(index);
        RelCollation collations = TraitUtils.createCollation(idx.columns(), idx.collations(), tableDescriptor);
        IgniteDistribution distribution = tableDescriptor.distribution();

        IgniteSchemaIndex schemaIndex = new IgniteSchemaIndex(index.id(), index.name(), idx.type(), distribution, collations);
        RowFactory<Object[]> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);
        SingleRangeIterable<Object[]> conditions = new SingleRangeIterable<>(new Object[]{}, null, false, false);
        List<PartitionWithTerm> partitions = scannableTable.getPartitions();

        return new IndexScanNode<>(ctx, rowFactory, schemaIndex, scannableTable, tableDescriptor, partitions,
                comparator, conditions, null, null, null);
    }

    static class TestScannableTable<T> implements ScannableTable {

        private final Map<Integer, List<T>> partitionedData = new ConcurrentHashMap<>();

        void setPartitionData(int partitionId, T... rows) {
            partitionedData.put(partitionId, List.of(rows));
        }

        List<PartitionWithTerm> getPartitions() {
            return new TreeSet<>(partitionedData.keySet())
                    .stream()
                    .map(k -> new PartitionWithTerm(k, 2L))
                    .collect(Collectors.toList());
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
                RowFactory<RowT> rowFactory, int indexId, List<Integer> columns,
                @Nullable RangeCondition<RowT> cond, @Nullable BitSet requiredColumns) {

            List<T> list = partitionedData.get(partWithTerm.partId());
            return new ScanPublisher<>(list, ctx, rowFactory);
        }

        @Override
        public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
                RowFactory<RowT> rowFactory, int indexId, List<Integer> columns,
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
                            subscriber.onNext((R) row);
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
