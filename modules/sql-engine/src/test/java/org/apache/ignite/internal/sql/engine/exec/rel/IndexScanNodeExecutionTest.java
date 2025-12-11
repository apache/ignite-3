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

import static org.apache.ignite.internal.sql.engine.util.Commons.IN_BUFFER_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.PartitionProvider;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest.TestTableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.NativeTypes.RowTypeBuilder;
import org.apache.ignite.internal.type.StructNativeType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test {@link IndexScanNode} execution.
 */
public class IndexScanNodeExecutionTest extends AbstractExecutionTest<Object[]> {

    /**
     * Sorted index scan execution.
     */
    @Test
    public void testSortedIndex() {
        List<String> columns = List.of("C1");
        List<Collation> collations = List.of(IgniteIndex.Collation.ASC_NULLS_LAST);

        TableDescriptor tableDescriptor = createTableDescriptor(columns);
        IgniteIndex indexDescriptor = createSortedIndexDescriptor(columns, collations, tableDescriptor);

        ExecutionContext<Object[]> ctx = executionContext();

        Tester tester = new Tester(ctx);

        TestScannableTable<Object[]> scannableTable = new TestScannableTable<>();
        scannableTable.setPartitionData(0, new Object[]{4}, new Object[]{5});
        scannableTable.setPartitionData(2, new Object[]{1}, new Object[]{2});

        Comparator<Object[]> cmp = Comparator.comparing(row -> (Comparable<Object>) row[0]);

        IndexScanNode<Object[]> node = tester.createSortedIndex(indexDescriptor, tableDescriptor, scannableTable, cmp);
        List<Object[]> result = tester.execute(node);

        validateResult(result, List.of(new Object[]{1}, new Object[]{2}, new Object[]{4}, new Object[]{5}));
    }

    /**
     * Hash index lookup execution.
     */
    @Test
    public void testHashIndex() {
        List<String> columns = List.of("C1");

        TableDescriptor tableDescriptor = createTableDescriptor(columns);
        IgniteIndex indexDescriptor = createHashIndexDescriptor(columns, tableDescriptor);

        ExecutionContext<Object[]> ctx = executionContext();

        Tester tester = new Tester(ctx);

        TestScannableTable<Object[]> scannableTable = new TestScannableTable<>();
        scannableTable.setPartitionData(0, new Object[]{2}, new Object[]{1});
        scannableTable.setPartitionData(2, new Object[]{0});

        IndexScanNode<Object[]> node = tester.createHashIndex(indexDescriptor, tableDescriptor, scannableTable);
        List<Object[]> result = tester.execute(node);

        validateResult(result, List.of(new Object[]{2}, new Object[]{1}, new Object[]{0}));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void indexScanNodeWithVariousBufferSize(boolean sorted) {
        // Buffer of size 1.
        int bufferSize = 1;
        checkIndexScan(sorted, bufferSize, 1, 0);
        checkIndexScan(sorted, bufferSize, 1, 1);
        checkIndexScan(sorted, bufferSize, 5, 1);

        // Partitions of size 1, which amount is close to buffer size.
        bufferSize = 10;
        checkIndexScan(sorted, bufferSize, bufferSize - 1, 1);
        checkIndexScan(sorted, bufferSize, bufferSize, 1);
        checkIndexScan(sorted, bufferSize, bufferSize + 1, 1);

        // Default buffer size.
        bufferSize = IN_BUFFER_SIZE;
        checkIndexScan(sorted, bufferSize, 1, 0);
        checkIndexScan(sorted, bufferSize, 1, 1);
        checkIndexScan(sorted, bufferSize, 1, bufferSize - 1);
        checkIndexScan(sorted, bufferSize, 1, bufferSize);
        checkIndexScan(sorted, bufferSize, 1, bufferSize + 1);
        checkIndexScan(sorted, bufferSize, 1, 2 * bufferSize);
    }

    private void checkIndexScan(boolean sorted, int bufferSize, int partitionsCount, int partDataSize) {
        List<String> columns = List.of("C1");
        List<Collation> collations = List.of(IgniteIndex.Collation.ASC_NULLS_LAST);

        ExecutionContext<Object[]> ctx = executionContext(bufferSize);

        List<PartitionWithConsistencyToken> partitions = IntStream.range(0, partitionsCount)
                .mapToObj(i -> new PartitionWithConsistencyToken(1, 42L))
                .collect(Collectors.toList());

        SingleRangeIterable<Object[]> conditions = new SingleRangeIterable<>(new Object[]{}, null, false, false);

        StructNativeType schema = NativeTypes.rowBuilder().addField("C1", NativeTypes.INT32, false).build();
        RowFactory<Object[]> rowFactory = ctx.rowFactoryFactory().create(schema);

        TableDescriptor tableDescriptor = createTableDescriptor(columns);
        IgniteIndex indexDescriptor = sorted
                ? createSortedIndexDescriptor(columns, collations, tableDescriptor)
                : createHashIndexDescriptor(columns, tableDescriptor);

        Comparator<Object[]> comparator = sorted
                ? Comparator.comparing(row -> (Comparable<Object>) row[0])
                : null;

        ScannableTable scannableIndex = sorted
                ? TestBuilders.indexRangeScan(DataProvider.fromRow(new Object[]{42}, partDataSize))
                : TestBuilders.indexLookup(DataProvider.fromRow(new Object[]{42}, partDataSize));

        IndexScanNode<Object[]> scanNode = new IndexScanNode<>(ctx, rowFactory, indexDescriptor, scannableIndex, tableDescriptor,
                c -> partitions, comparator, conditions, null, null, null);
        RootNode<Object[]> rootNode = new RootNode<>(ctx);

        rootNode.register(scanNode);

        long count = StreamSupport.stream(Spliterators.spliteratorUnknownSize(rootNode, Spliterator.ORDERED), false).count();

        assertEquals((long) partDataSize * partitionsCount, count);
    }

    private static TableDescriptor createTableDescriptor(List<String> columns) {
        Builder rowTypeBuilder = new Builder(Commons.typeFactory());

        for (String column : columns) {
            rowTypeBuilder = rowTypeBuilder.add(column, SqlTypeName.INTEGER);
        }

        RelDataType rowType = rowTypeBuilder.build();

        return new TestTableDescriptor(IgniteDistributions::single, rowType);
    }

    private static IgniteIndex createHashIndexDescriptor(
            List<String> columns,
            TableDescriptor tableDescriptor
    ) {
        RelCollation collation = TraitUtils.createCollation(columns, null, tableDescriptor);
        IgniteDistribution distribution = tableDescriptor.distribution();

        return new IgniteIndex(1, "IDX", IgniteIndex.Type.HASH, distribution, collation);
    }

    private static IgniteIndex createSortedIndexDescriptor(
            List<String> columns,
            List<Collation> collations,
            TableDescriptor tableDescriptor
    ) {
        RelCollation collation = TraitUtils.createCollation(columns, collations, tableDescriptor);
        IgniteDistribution distribution = tableDescriptor.distribution();

        return new IgniteIndex(1, "IDX", Type.SORTED, distribution, collation);
    }

    private static class Tester {

        private final ExecutionContext<Object[]> ctx;

        Tester(ExecutionContext<Object[]> ctx) {
            this.ctx = ctx;
        }

        IndexScanNode<Object[]> createSortedIndex(IgniteIndex indexDescriptor, TableDescriptor tableDescriptor,
                TestScannableTable<?> scannableTable, Comparator<Object[]> cmp) {
            return createIndexNode(ctx, indexDescriptor, tableDescriptor, scannableTable, cmp);
        }

        IndexScanNode<Object[]> createHashIndex(IgniteIndex desc, TableDescriptor tableDescriptor,
                TestScannableTable<?> scannableTable) {
            return createIndexNode(ctx, desc, tableDescriptor, scannableTable, null);
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

    private static IndexScanNode<Object[]> createIndexNode(ExecutionContext<Object[]> ctx, IgniteIndex indexDescriptor,
            TableDescriptor tableDescriptor, TestScannableTable<?> scannableTable, @Nullable Comparator<Object[]> comparator) {

        RowTypeBuilder rowSchemaBuilder = NativeTypes.rowBuilder();

        int idx = 0;
        for (RelFieldCollation ignored : indexDescriptor.collation().getFieldCollations()) {
            rowSchemaBuilder = rowSchemaBuilder.addField("C" + idx++, NativeTypes.INT32, true);
        }

        StructNativeType rowSchema = rowSchemaBuilder.build();

        RowFactory<Object[]> rowFactory = ctx.rowFactoryFactory().create(rowSchema);
        SingleRangeIterable<Object[]> conditions = new SingleRangeIterable<>(new Object[]{}, null, false, false);
        List<PartitionWithConsistencyToken> partitions = scannableTable.getPartitions();
        PartitionProvider<Object[]> partitionProvider = PartitionProvider.fromPartitions(partitions);

        return new IndexScanNode<>(ctx, rowFactory, indexDescriptor, scannableTable, tableDescriptor, partitionProvider,
                comparator, conditions, null, null, null);
    }

    static class TestScannableTable<T> implements ScannableTable {

        private final Map<Integer, List<T>> partitionedData = new ConcurrentHashMap<>();

        void setPartitionData(int partitionId, T... rows) {
            partitionedData.put(partitionId, List.of(rows));
        }

        List<PartitionWithConsistencyToken> getPartitions() {
            return new TreeSet<>(partitionedData.keySet())
                    .stream()
                    .map(k -> new PartitionWithConsistencyToken(k, 2L))
                    .collect(Collectors.toList());
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> Publisher<RowT> scan(
                ExecutionContext<RowT> ctx,
                PartitionWithConsistencyToken partWithConsistencyToken,
                RowFactory<RowT> rowFactory,
                int @Nullable [] requiredColumns
        ) {

            throw new UnsupportedOperationException("Not supported");
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> Publisher<RowT> indexRangeScan(
                ExecutionContext<RowT> ctx,
                PartitionWithConsistencyToken partWithConsistencyToken,
                RowFactory<RowT> rowFactory,
                int indexId,
                List<String> columns,
                @Nullable RangeCondition<RowT> cond,
                int @Nullable [] requiredColumns
        ) {

            List<T> list = partitionedData.get(partWithConsistencyToken.partId());
            return new ScanPublisher<>(list, ctx, rowFactory);
        }

        @Override
        public <RowT> Publisher<RowT> indexLookup(
                ExecutionContext<RowT> ctx,
                PartitionWithConsistencyToken partWithConsistencyToken,
                RowFactory<RowT> rowFactory,
                int indexId,
                List<String> columns,
                RowT key,
                int @Nullable [] requiredColumns
        ) {

            return newPublisher(ctx, partWithConsistencyToken, rowFactory);
        }

        @Override
        public <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(
                ExecutionContext<RowT> ctx,
                InternalTransaction explicitTx,
                RowFactory<RowT> rowFactory,
                RowT key,
                int @Nullable [] requiredColumns
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Long> estimatedSize() {
            throw new UnsupportedOperationException();
        }

        private <RowT> ScanPublisher<RowT> newPublisher(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithToken,
                RowFactory<RowT> rowFactory) {

            int partId = partWithToken.partId();
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

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }
}
