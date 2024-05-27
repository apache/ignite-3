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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS_OR_EQUAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.ScannableTableImpl;
import org.apache.ignite.internal.sql.engine.exec.TableRowConverter;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link ScannableTableImpl}. We check that scan/lookup operations call appropriate methods
 * of the underlying APIs with required arguments.
 */
@ExtendWith(MockitoExtension.class)
public class ScannableTableSelfTest extends BaseIgniteAbstractTest {

    private static final IgniteTypeFactory TYPE_FACTORY = Commons.typeFactory();

    @Mock(lenient = true)
    private InternalTable internalTable;

    @Mock(lenient = true)
    private ExecutionContext<Object[]> ctx;

    @Mock
    private BinaryRow binaryRow;

    /**
     * Table scan.
     */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testTableScan(NoOpTransaction tx) {
        TestInput data = new TestInput();
        data.addRow(binaryRow);

        Tester tester = new Tester(data);

        int partitionId = 1;
        long consistencyToken = 2;

        ResultCollector collector = tester.tableScan(partitionId, consistencyToken, tx);

        if (tx.isReadOnly()) {
            HybridTimestamp timestamp = tx.readTimestamp();
            ClusterNode clusterNode = tx.clusterNode();

            verify(internalTable).scan(partitionId, tx.id(), timestamp, clusterNode, tx.coordinatorId());
        } else {
            ClusterNode clusterNode = tx.clusterNode();

            verify(internalTable).scan(
                    partitionId,
                    tx.id(),
                    tx.zoneCommitPartition(),
                    tx.coordinatorId(),
                    new PrimaryReplica(clusterNode, consistencyToken),
                    null,
                    null,
                    null,
                    0,
                    null
            );
        }

        data.sendRows();
        data.done();

        collector.expectRow(binaryRow);
        collector.expectCompleted();
    }

    /**
     * Table scan error propagation.
     */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testTableScanError(NoOpTransaction tx) {
        TestInput input = new TestInput();
        input.addRow(binaryRow);

        Tester tester = new Tester(input);

        int partitionId = 1;
        long consistencyToken = 2;

        ResultCollector collector = tester.tableScan(partitionId, consistencyToken, tx);

        input.sendRows();

        RuntimeException err = new RuntimeException("err");
        input.sendError(err);

        collector.expectRow(binaryRow);
        collector.expectError(err);
    }

    /**
     * Index scan with different bounds.
     */
    @ParameterizedTest
    @MethodSource("indexScanParameters")
    public void testIndexScan(NoOpTransaction tx, Bound lower, Bound upper) {
        TestInput input = new TestInput();
        input.addRow(binaryRow);

        Tester tester = new Tester(input);

        int partitionId = 1;
        long consistencyToken = 2;
        int indexId = 3;
        Object[] lowerValue = lower == Bound.NONE ? null : new Object[]{1};
        Object[] upperValue = upper == Bound.NONE ? null : new Object[]{10};
        TestRangeCondition<Object[]> condition = new TestRangeCondition<>();
        condition.setLower(lower, lowerValue);
        condition.setUpper(upper, upperValue);

        int flags = condition.toFlags();

        ResultCollector collector = tester.indexScan(partitionId, consistencyToken, tx, indexId, condition);

        if (tx.isReadOnly()) {
            HybridTimestamp timestamp = tx.readTimestamp();
            ClusterNode clusterNode = tx.clusterNode();

            verify(internalTable).scan(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(timestamp),
                    eq(clusterNode),
                    eq(indexId),
                    condition.lowerValue != null ? any(BinaryTuplePrefix.class) : isNull(),
                    condition.upperValue != null ? any(BinaryTuplePrefix.class) : isNull(),
                    eq(flags),
                    isNull(),
                    eq(tx.coordinatorId())
            );
        } else {
            PrimaryReplica primaryReplica = new PrimaryReplica(ctx.localNode(), consistencyToken);

            verify(internalTable).scan(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(tx.zoneCommitPartition()),
                    anyString(),
                    eq(primaryReplica),
                    eq(indexId),
                    condition.lowerValue != null ? any(BinaryTuplePrefix.class) : isNull(),
                    condition.upperValue != null ? any(BinaryTuplePrefix.class) : isNull(),
                    eq(flags),
                    isNull()
            );
        }

        input.sendRows();
        input.done();

        collector.expectRow(binaryRow);
        collector.expectCompleted();
    }

    private static Stream<Arguments> indexScanParameters() {
        List<Arguments> params = new ArrayList<>();

        for (Bound leftBound : Bound.values()) {
            for (Bound rightBound : Bound.values()) {
                params.add(Arguments.of(NoOpTransaction.readOnly("RO"), leftBound, rightBound));
                params.add(Arguments.of(NoOpTransaction.readWrite("RW"), leftBound, rightBound));
            }
        }

        return params.stream();
    }

    /**
     * Index scan with specified required columns.
     */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testIndexScanWithRequiredColumns(NoOpTransaction tx) {
        TestInput input = new TestInput();
        input.addRow(binaryRow);

        Tester tester = new Tester(input);
        tester.requiredFields = new BitSet();
        tester.requiredFields.set(1);

        int partitionId = 1;
        long consistencyToken = 2;
        int indexId = 3;

        TestRangeCondition<Object[]> condition = new TestRangeCondition<>();
        // Set any valid bounds, they are not of our interest here.
        condition.setLower(Bound.INCLUSIVE, new Object[]{0});

        ResultCollector collector = tester.indexScan(partitionId, consistencyToken, tx, indexId, condition);

        if (tx.isReadOnly()) {
            HybridTimestamp timestamp = tx.readTimestamp();
            ClusterNode clusterNode = tx.clusterNode();

            verify(internalTable).scan(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(timestamp),
                    eq(clusterNode),
                    eq(indexId),
                    nullable(BinaryTuplePrefix.class),
                    nullable(BinaryTuplePrefix.class),
                    anyInt(),
                    eq(tester.requiredFields),
                    eq(tx.coordinatorId())
            );
        } else {
            PrimaryReplica primaryReplica = new PrimaryReplica(ctx.localNode(), consistencyToken);

            verify(internalTable).scan(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(tx.zoneCommitPartition()),
                    anyString(),
                    eq(primaryReplica),
                    eq(indexId),
                    nullable(BinaryTuplePrefix.class),
                    nullable(BinaryTuplePrefix.class),
                    anyInt(),
                    eq(tester.requiredFields)
            );
        }

        input.sendRows();
        input.done();

        collector.expectRow(binaryRow);
        collector.expectCompleted();
    }

    /**
     * Index scan error propagation.
     */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testIndexScanError(NoOpTransaction tx) {
        TestInput input = new TestInput();
        input.addRow(binaryRow);

        Tester tester = new Tester(input);
        tester.requiredFields = new BitSet();
        tester.requiredFields.set(1);

        int partitionId = 1;
        long consistencyToken = 2;
        int indexId = 3;
        TestRangeCondition<Object[]> condition = new TestRangeCondition<>();
        // Set any valid bounds, they are not of our interest here.
        condition.setLower(Bound.INCLUSIVE, new Object[]{0});

        ResultCollector collector = tester.indexScan(partitionId, consistencyToken, tx, indexId, condition);

        input.sendRows();

        RuntimeException err = new RuntimeException("err");
        input.sendError(err);

        collector.expectError(err);
        collector.expectRow(binaryRow);
    }

    /**
     * Index scan - invalid condition.
     */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testIndexScanInvalidCondition(NoOpTransaction tx) {
        TestInput input = new TestInput();
        input.addRow(binaryRow, 1);

        Tester tester = new Tester(input);

        int partitionId = 1;
        long consistencyToken = 2;
        int indexId = 3;
        TestRangeCondition<Object[]> condition = new TestRangeCondition<>();
        // Bound columns != input columns.
        condition.setLower(Bound.INCLUSIVE, new Object[]{1, 2});

        AssertionError err = assertThrows(AssertionError.class,
                () -> tester.indexScan(partitionId, consistencyToken, tx, indexId, condition));
        assertEquals("Invalid range condition", err.getMessage());

        verifyNoInteractions(internalTable);
    }

    /**
     * Index scan - index bound includes some of columns.
     */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testIndexScanPartialCondition(NoOpTransaction tx) {
        // 4 column input table.
        TestInput input = new TestInput(4);
        // 3 column index.
        input.indexColumns.set(0);
        input.indexColumns.set(1);
        input.indexColumns.set(2);
        input.addRow(binaryRow, 1, 2, 3, 4);

        Tester tester = new Tester(input);

        int partitionId = 1;
        long consistencyToken = 2;
        int indexId = 3;
        TestRangeCondition<Object[]> condition = new TestRangeCondition<>();
        condition.setLower(Bound.INCLUSIVE, new Object[]{1, 2});

        ArgumentCaptor<BinaryTuplePrefix> prefix = ArgumentCaptor.forClass(BinaryTuplePrefix.class);

        ResultCollector collector = tester.indexScan(partitionId, consistencyToken, tx, indexId, condition);

        if (tx.isReadOnly()) {
            HybridTimestamp timestamp = tx.readTimestamp();
            ClusterNode clusterNode = tx.clusterNode();

            verify(internalTable).scan(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(timestamp),
                    eq(clusterNode),
                    eq(indexId),
                    prefix.capture(),
                    nullable(BinaryTuplePrefix.class),
                    anyInt(),
                    eq(tester.requiredFields),
                    eq(tx.coordinatorId())
            );
        } else {
            PrimaryReplica primaryReplica = new PrimaryReplica(ctx.localNode(), consistencyToken);

            verify(internalTable).scan(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(tx.zoneCommitPartition()),
                    anyString(),
                    eq(primaryReplica),
                    eq(indexId),
                    prefix.capture(),
                    nullable(BinaryTuplePrefix.class),
                    anyInt(),
                    eq(tester.requiredFields)
            );
        }

        input.sendRows();
        input.done();

        collector.expectCompleted();

        BinaryTuplePrefix lowerBound = prefix.getValue();
        assertEquals(2, lowerBound.elementCount());
    }

    /**
     * Index lookup.
     */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testIndexLookup(NoOpTransaction tx) {
        TestInput input = new TestInput();
        input.addRow(binaryRow);

        Tester tester = new Tester(input);

        int partitionId = 1;
        long consistencyToken = 2;
        int indexId = 3;
        Object[] key = {1};

        ResultCollector collector = tester.indexLookUp(partitionId, consistencyToken, tx, indexId, key);

        if (tx.isReadOnly()) {
            verify(internalTable).lookup(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(tx.readTimestamp()),
                    eq(tx.clusterNode()),
                    eq(indexId),
                    any(BinaryTuple.class),
                    isNull(),
                    eq(tx.coordinatorId())
            );
        } else {
            PrimaryReplica primaryReplica = new PrimaryReplica(ctx.localNode(), consistencyToken);

            verify(internalTable).lookup(
                    eq(partitionId),
                    eq(tx.id()),
                    any(),
                    anyString(),
                    eq(primaryReplica),
                    eq(indexId),
                    any(BinaryTuple.class),
                    isNull()
            );
        }

        input.sendRows();
        input.done();

        collector.expectRow(binaryRow);
        collector.expectCompleted();
    }

    /**
     * Index lookup with specified required columns.
     */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testIndexLookupWithRequiredColumns(NoOpTransaction tx) {
        TestInput input = new TestInput();
        input.addRow(binaryRow);

        Tester tester = new Tester(input);
        tester.requiredFields = new BitSet();
        tester.requiredFields.set(1);

        int partitionId = 1;
        long consistencyToken = 2;
        int indexId = 3;
        Object[] key = {1};

        ResultCollector collector = tester.indexLookUp(partitionId, consistencyToken, tx, indexId, key);

        if (tx.isReadOnly()) {
            verify(internalTable).lookup(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(tx.readTimestamp()),
                    eq(tx.clusterNode()),
                    eq(indexId),
                    any(BinaryTuple.class),
                    eq(null),
                    eq(tx.coordinatorId())
            );
        } else {
            PrimaryReplica primaryReplica = new PrimaryReplica(ctx.localNode(), consistencyToken);

            verify(internalTable).lookup(
                    eq(partitionId),
                    eq(tx.id()),
                    any(),
                    anyString(),
                    eq(primaryReplica),
                    eq(indexId),
                    any(BinaryTuple.class),
                    eq(null)
            );
        }

        input.sendRows();
        input.done();

        collector.expectCompleted();
        collector.expectRow(binaryRow);
    }

    /**
     * Index lookup - error propagation.
     */
    @ParameterizedTest
    @MethodSource("transactions")
    public void testIndexLookupError(NoOpTransaction tx) {
        TestInput input = new TestInput();
        input.addRow(binaryRow);

        Tester tester = new Tester(input);

        int partitionId = 1;
        long consistencyToken = 2;
        int indexId = 3;
        Object[] key = {1};

        ResultCollector collector = tester.indexLookUp(partitionId, consistencyToken, tx, indexId, key);

        input.sendRows();

        RuntimeException err = new RuntimeException("Broken");
        input.sendError(err);

        collector.expectRow(binaryRow);
        collector.expectError(err);
    }

    private static Stream<Arguments> transactions() {
        return Stream.of(
                Arguments.of(Named.of("Read-only transaction", NoOpTransaction.readOnly("RO"))),
                Arguments.of(Named.of("Read-write transaction", NoOpTransaction.readWrite("RW")))
        );
    }

    private class Tester {

        final ScannableTable scannableTable;

        final TestInput input;

        final RowCollectingTableRwoConverter rowConverter;

        BitSet requiredFields;

        Tester(TestInput input) {
            this.input = input;
            rowConverter = new RowCollectingTableRwoConverter(input);
            scannableTable = new ScannableTableImpl(internalTable, rf -> rowConverter);
        }

        ResultCollector tableScan(int partitionId, long consistencyToken, NoOpTransaction tx) {
            when(ctx.txAttributes()).thenReturn(TxAttributes.fromTx(tx));
            when(ctx.localNode()).thenReturn(tx.clusterNode());

            if (tx.isReadOnly()) {
                doAnswer(invocation -> input.publisher).when(internalTable)
                        .scan(anyInt(), any(UUID.class), any(HybridTimestamp.class), any(ClusterNode.class), anyString());
            } else {
                doAnswer(invocation -> input.publisher).when(internalTable).scan(
                        anyInt(),
                        any(UUID.class),
                        any(ZonePartitionId.class),
                        any(String.class),
                        any(PrimaryReplica.class),
                        isNull(),
                        isNull(),
                        isNull(),
                        eq(0),
                        isNull()
                );
            }

            RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;
            RowFactory<Object[]> rowFactory = rowHandler.factory(input.rowSchema);

            Publisher<Object[]> publisher = scannableTable.scan(
                    ctx,
                    new PartitionWithConsistencyToken(partitionId, consistencyToken), rowFactory, null
            );

            return new ResultCollector(publisher, rowConverter);
        }

        ResultCollector indexScan(
                int partitionId,
                long consistencyToken,
                NoOpTransaction tx,
                int indexId,
                TestRangeCondition<Object[]> condition
        ) {

            when(ctx.txAttributes()).thenReturn(TxAttributes.fromTx(tx));
            when(ctx.localNode()).thenReturn(tx.clusterNode());

            if (tx.isReadOnly()) {
                doAnswer(i -> input.publisher).when(internalTable).scan(
                        anyInt(),
                        any(UUID.class),
                        any(HybridTimestamp.class),
                        any(ClusterNode.class),
                        any(Integer.class),
                        nullable(BinaryTuplePrefix.class),
                        nullable(BinaryTuplePrefix.class),
                        anyInt(),
                        nullable(BitSet.class),
                        anyString());
            } else {
                doAnswer(i -> input.publisher).when(internalTable).scan(
                        anyInt(),
                        any(UUID.class),
                        any(ZonePartitionId.class),
                        any(String.class),
                        any(PrimaryReplica.class),
                        any(Integer.class),
                        nullable(BinaryTuplePrefix.class),
                        nullable(BinaryTuplePrefix.class),
                        anyInt(),
                        nullable(BitSet.class));
            }

            RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;
            RowFactory<Object[]> rowFactory = rowHandler.factory(input.rowSchema);
            RangeCondition<Object[]> rangeCondition = condition.asRangeCondition();
            List<String> indexColumns = input.getIndexColumns();

            Publisher<Object[]> publisher = scannableTable.indexRangeScan(
                    ctx,
                    new PartitionWithConsistencyToken(partitionId, consistencyToken),
                    rowFactory,
                    indexId,
                    indexColumns,
                    rangeCondition,
                    requiredFields
            );

            return new ResultCollector(publisher, rowConverter);
        }

        ResultCollector indexLookUp(int partitionId, long consistencyToken, NoOpTransaction tx,
                int indexId, Object[] key) {

            when(ctx.txAttributes()).thenReturn(TxAttributes.fromTx(tx));
            when(ctx.localNode()).thenReturn(tx.clusterNode());

            if (tx.isReadOnly()) {
                doAnswer(i -> input.publisher).when(internalTable).lookup(
                        anyInt(),
                        any(UUID.class),
                        any(HybridTimestamp.class),
                        any(ClusterNode.class),
                        any(Integer.class),
                        nullable(BinaryTuple.class),
                        nullable(BitSet.class),
                        anyString());
            } else {
                doAnswer(i -> input.publisher).when(internalTable).lookup(
                        anyInt(),
                        any(UUID.class),
                        any(ZonePartitionId.class),
                        any(String.class),
                        any(PrimaryReplica.class),
                        any(Integer.class),
                        nullable(BinaryTuple.class),
                        nullable(BitSet.class));
            }

            RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;
            RowFactory<Object[]> rowFactory = rowHandler.factory(input.rowSchema);
            List<String> indexColumns = input.getIndexColumns();

            Publisher<Object[]> publisher = scannableTable.indexLookup(
                    ctx,
                    new PartitionWithConsistencyToken(partitionId, consistencyToken),
                    rowFactory,
                    indexId,
                    indexColumns,
                    key,
                    requiredFields
            );

            return new ResultCollector(publisher, rowConverter);
        }
    }

    // Input data.
    static class TestInput {

        final SubmissionPublisher<BinaryRow> publisher = new SubmissionPublisher<>(Runnable::run, Integer.MAX_VALUE);

        final Map<BinaryRow, Object[]> data = new HashMap<>();

        final List<BinaryRow> rows = new ArrayList<>();

        final RelDataType rowType;

        final RowSchema rowSchema;

        final BitSet indexColumns = new BitSet();

        TestInput() {
            this(1);
        }

        TestInput(int columnCount) {
            Builder builder = new Builder(TYPE_FACTORY);
            RowSchema.Builder rowSchema = RowSchema.builder();

            for (int i = 1; i <= columnCount; i++) {
                builder.add("C" + i, SqlTypeName.INTEGER);
                rowSchema.addField(NativeTypes.INT32);
            }

            indexColumns.set(0);

            rowType = builder.build();
            this.rowSchema = rowSchema.build();
        }

        void addRow(BinaryRow row) {
            Object[] cols = IntStream.generate(() -> 1).limit(rowType.getFieldCount()).boxed().toArray();

            addRow(row, cols);
        }

        void addRow(BinaryRow row, Object... values) {
            int fieldCount = rowType.getFieldCount();
            if (fieldCount != values.length) {
                throw new IllegalArgumentException(format("Expected {} columns but got {}", fieldCount, values.length));
            }

            data.put(row, new Object[]{values});
            rows.add(row);
        }

        void sendRows() {
            for (BinaryRow row : rows) {
                publisher.submit(row);
            }
        }

        void done() {
            publisher.close();
        }

        void sendError(Throwable t) {
            publisher.closeExceptionally(t);
        }

        private List<String> getIndexColumns() {
            List<String> columns = new ArrayList<>();

            indexColumns.stream().forEach(i -> {
                RelDataTypeField field = rowType.getFieldList().get(i);
                columns.add(field.getName());
            });

            return columns;
        }
    }

    // Collects rows received from an input source.
    static class RowCollectingTableRwoConverter implements TableRowConverter {

        final TestInput testInput;

        final List<BinaryRow> converted = new ArrayList<>();

        RowCollectingTableRwoConverter(TestInput testData) {
            this.testInput = testData;
        }

        @Override
        public <RowT> BinaryRowEx toFullRow(ExecutionContext<RowT> ectx, RowT row) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <RowT> BinaryRowEx toKeyRow(ExecutionContext<RowT> ectx, RowT row) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow tableRow, RowFactory<RowT> factory) {
            Object[] convertedRow = testInput.data.get(tableRow);
            if (convertedRow == null) {
                throw new IllegalArgumentException("Unexpected row: " + tableRow);
            }

            converted.add(tableRow);
            return (RowT) convertedRow;
        }
    }

    static class ResultCollector {

        final Publisher<?> input;

        final CountDownLatch done = new CountDownLatch(1);

        final AtomicReference<Throwable> err = new AtomicReference<>();

        final RowCollectingTableRwoConverter rowConverter;

        ResultCollector(Publisher<?> input, RowCollectingTableRwoConverter rowConverter) {
            this.input = input;
            this.rowConverter = rowConverter;

            input.subscribe(new Subscriber<Object>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Object ignore) {
                    // do nothing - we collect received items in TableRowConverter.
                }

                @Override
                public void onError(Throwable t) {
                    err.set(t);
                    done.countDown();
                }

                @Override
                public void onComplete() {
                    done.countDown();
                }
            });
        }

        void expectError(Throwable t) {
            try {
                done.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            assertSame(t, err.get(), "Unexpected error");
        }

        void expectCompleted() {
            try {
                done.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            assertNull(err.get(), "Expected no error");
        }

        void expectRow(BinaryRow row) {
            List<BinaryRow> result = rowConverter.converted;

            if (!result.contains(row)) {
                fail(format("Unexpected binary row: {}. Converted: {}", row, result));
            }
        }
    }

    /**
     * Index bounds.
     */
    enum Bound {
        INCLUSIVE,
        EXCLUSIVE,
        NONE;

        static int toFlags(Bound lower, Bound upper) {
            if (lower == NONE && upper == NONE) {
                return LESS_OR_EQUAL | GREATER_OR_EQUAL;
            } else {
                int flags = 0;
                flags |= lower.bit(true);
                flags |= upper.bit(false);
                return flags;
            }
        }

        int bit(boolean lower) {
            switch (this) {
                case INCLUSIVE:
                    return lower ? GREATER_OR_EQUAL : LESS_OR_EQUAL;
                case EXCLUSIVE:
                case NONE:
                    return 0;
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    /**
     * Utility class to build {@link RangeCondition}.
     */
    static final class TestRangeCondition<T> {

        private Bound lowerBoundType;

        private T lowerValue;

        private Bound upperBoundType;

        private T upperValue;

        void setLower(Bound bound, T value) {
            if (bound == Bound.NONE && value != null) {
                throw new IllegalArgumentException("NONE bound with no value");
            }
            lowerBoundType = bound;
            lowerValue = value;
        }

        void setUpper(Bound bound, T value) {
            if (bound == Bound.NONE && value != null) {
                throw new IllegalArgumentException("NONE bound with no value");
            }
            upperBoundType = bound;
            upperValue = value;
        }

        int toFlags() {
            return Bound.toFlags(lowerBoundType, upperBoundType);
        }

        @Nullable
        RangeCondition<T> asRangeCondition() {
            if (lowerValue == null && upperValue == null) {
                return null;
            } else {
                return new RangeCondition<>() {
                    @Override
                    public T lower() {
                        return lowerValue;
                    }

                    @Override
                    public T upper() {
                        return upperValue;
                    }

                    @Override
                    public boolean lowerInclude() {
                        return lowerBoundType == Bound.INCLUSIVE;
                    }

                    @Override
                    public boolean upperInclude() {
                        return upperBoundType == Bound.INCLUSIVE;
                    }
                };
            }
        }
    }
}
