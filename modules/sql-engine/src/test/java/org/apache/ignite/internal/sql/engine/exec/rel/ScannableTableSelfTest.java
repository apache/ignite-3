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

import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.sql.engine.exec.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.ScannableTableImpl;
import org.apache.ignite.internal.sql.engine.exec.TableRowConverter;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest.TestTableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link ScannableTableImpl}. We check that scan/lookup operations call appropriate methods
 * of the underlying APIs with required arguments.
 */
@ExtendWith(MockitoExtension.class)
public class ScannableTableSelfTest {

    private static final NoOpTransaction RO_TX = new NoOpTransaction("RO", true);

    private static final NoOpTransaction RW_TX = new NoOpTransaction("RO", false);

    private static final IgniteTypeFactory TYPE_FACTORY = Commons.typeFactory();

    @Mock
    private InternalTable internalTable;

    @Mock(lenient = true)
    private ExecutionContext<Object[]> ctx;

    @Mock
    private BinaryRow binaryRow;

    /**
     * Table scan.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTableScan(boolean ro) {
        NoOpTransaction tx = ro ? RO_TX : RW_TX;

        Tester tester = new Tester();

        int partitionId = 1;
        long term = 2;

        SubmissionPublisher<BinaryRow> input = newSingleThreadedSubmissionPublisher();

        TestPublisher<Object[]> publisher = tester.tableScan(input, partitionId, term, tx);

        if (ro) {
            HybridTimestamp timestamp = tx.readTimestamp();
            ClusterNode clusterNode = tx.clusterNode();

            verify(internalTable).scan(partitionId, timestamp, clusterNode);
        } else {
            ClusterNode clusterNode = tx.clusterNode();

            verify(internalTable).scan(partitionId, tx.id(), new PrimaryReplica(clusterNode, term), null, null, null, 0, null);
        }

        input.submit(binaryRow);
        input.close();

        publisher.expectRow(binaryRow);
        publisher.expectCompleted();
    }

    /**
     * Table scan error propagation.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTableScanError(boolean ro) {
        NoOpTransaction tx = ro ? RO_TX : RW_TX;

        Tester tester = new Tester();

        int partitionId = 1;
        long term = 2;

        SubmissionPublisher<BinaryRow> input = newSingleThreadedSubmissionPublisher();

        TestPublisher<Object[]> publisher = tester.tableScan(input, partitionId, term, tx);

        input.submit(binaryRow);

        RuntimeException err = new RuntimeException("err");
        input.closeExceptionally(err);

        publisher.expectRow(binaryRow);
        publisher.expectError(err);
    }

    /**
     * Index scan with different bounds.
     */
    @ParameterizedTest
    @CsvSource({
            // RO
            "true, INCLUSIVE, NONE",
            "true, EXCLUSIVE, NONE",
            "true, NONE, INCLUSIVE",
            "true, NONE, EXCLUSIVE",
            "true, INCLUSIVE, INCLUSIVE",
            "true, INCLUSIVE, EXCLUSIVE",
            "true, EXCLUSIVE, INCLUSIVE",
            "true, EXCLUSIVE, EXCLUSIVE",
            // RW
            "false, INCLUSIVE, NONE",
            "false, EXCLUSIVE, NONE",
            "false, NONE, INCLUSIVE",
            "false, NONE, EXCLUSIVE",
            "false, INCLUSIVE, INCLUSIVE",
            "false, INCLUSIVE, EXCLUSIVE",
            "false, EXCLUSIVE, INCLUSIVE",
            "false, EXCLUSIVE, EXCLUSIVE",
    })
    public void testIndexScan(boolean ro, Bound lower, Bound upper) {
        NoOpTransaction tx = ro ? RO_TX : RW_TX;

        Tester tester = new Tester();

        int partitionId = 1;
        long term = 2;
        int indexId = 3;

        Object[] lowerValue = lower == Bound.NONE ? null : new Object[]{1};
        Object[] upperValue = upper == Bound.NONE ? null : new Object[]{10};

        TestRangeCondition<Object[]> condition = new TestRangeCondition<>();
        condition.setBounds(lower, lowerValue, upper, upperValue);
        int flags = condition.toFlags();

        SubmissionPublisher<BinaryRow> input = newSingleThreadedSubmissionPublisher();

        TestPublisher<Object[]> publisher = tester.indexScan(input, partitionId, term, tx, indexId, condition);

        if (ro) {
            HybridTimestamp timestamp = tx.readTimestamp();
            ClusterNode clusterNode = tx.clusterNode();

            verify(internalTable).scan(
                    eq(partitionId),
                    eq(timestamp),
                    eq(clusterNode),
                    eq(indexId),
                    condition.lowerValue != null ? any(BinaryTuplePrefix.class) : isNull(),
                    condition.upperValue != null ? any(BinaryTuplePrefix.class) : isNull(),
                    eq(flags),
                    isNull()
            );
        } else {
            PrimaryReplica primaryReplica = new PrimaryReplica(ctx.localNode(), term);

            verify(internalTable).scan(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(primaryReplica),
                    eq(indexId),
                    condition.lowerValue != null ? any(BinaryTuplePrefix.class) : isNull(),
                    condition.upperValue != null ? any(BinaryTuplePrefix.class) : isNull(),
                    eq(flags),
                    isNull()
            );
        }

        input.submit(binaryRow);
        input.close();

        publisher.expectRow(binaryRow);
        publisher.expectCompleted();
    }

    /**
     * Index scan with specified required columns.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexScanWithRequiredColumns(boolean ro) {
        NoOpTransaction tx = ro ? RO_TX : RW_TX;

        Tester tester = new Tester();

        int partitionId = 1;
        long term = 2;
        int indexId = 3;

        SubmissionPublisher<BinaryRow> input = newSingleThreadedSubmissionPublisher();

        tester.requiredFields = new BitSet();
        tester.requiredFields.set(1);

        TestRangeCondition<Object[]> condition = new TestRangeCondition<>();
        // Set any valid bounds, they are not of our interest here.
        condition.setBounds(Bound.INCLUSIVE, new Object[]{0}, Bound.NONE, null);

        TestPublisher<Object[]> publisher = tester.indexScan(input, partitionId, term, tx, indexId, condition);

        if (ro) {
            HybridTimestamp timestamp = tx.readTimestamp();
            ClusterNode clusterNode = tx.clusterNode();

            verify(internalTable).scan(
                    eq(partitionId),
                    eq(timestamp),
                    eq(clusterNode),
                    eq(indexId),
                    nullable(BinaryTuplePrefix.class),
                    nullable(BinaryTuplePrefix.class),
                    anyInt(),
                    eq(tester.requiredFields)
            );
        } else {
            PrimaryReplica primaryReplica = new PrimaryReplica(ctx.localNode(), term);

            verify(internalTable).scan(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(primaryReplica),
                    eq(indexId),
                    nullable(BinaryTuplePrefix.class),
                    nullable(BinaryTuplePrefix.class),
                    anyInt(),
                    eq(tester.requiredFields)
            );
        }

        input.submit(binaryRow);
        input.close();

        publisher.expectRow(binaryRow);
        publisher.expectCompleted();
    }

    /**
     * Index scan error propagation.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexScanError(boolean ro) {
        NoOpTransaction tx = ro ? RO_TX : RW_TX;

        Tester tester = new Tester();

        int partitionId = 1;
        long term = 2;
        int indexId = 3;

        SubmissionPublisher<BinaryRow> input = newSingleThreadedSubmissionPublisher();

        tester.requiredFields = new BitSet();
        tester.requiredFields.set(1);

        TestRangeCondition<Object[]> condition = new TestRangeCondition<>();
        // Set any valid bounds, they are not of our interest here.
        condition.setBounds(Bound.INCLUSIVE, new Object[]{0}, Bound.NONE, null);

        TestPublisher<Object[]> publisher = tester.indexScan(input, partitionId, term, tx, indexId, condition);

        input.submit(binaryRow);

        RuntimeException err = new RuntimeException("err");
        input.closeExceptionally(err);

        publisher.expectError(err);
        publisher.expectRow(binaryRow);
    }

    /**
     * Index lookup.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexLookup(boolean ro) {
        NoOpTransaction tx = ro ? RO_TX : RW_TX;

        Tester tester = new Tester();

        int partitionId = 1;
        long term = 2;
        int indexId = 3;

        SubmissionPublisher<BinaryRow> input = newSingleThreadedSubmissionPublisher();

        Object[] key = {1};

        TestPublisher<Object[]> publisher = tester.indexLookUp(input, partitionId, term, tx, indexId, key);

        if (ro) {
            verify(internalTable).lookup(
                    eq(partitionId),
                    eq(tx.readTimestamp()),
                    eq(tx.clusterNode()),
                    eq(indexId),
                    any(BinaryTuple.class),
                    isNull()
            );
        } else {
            PrimaryReplica primaryReplica = new PrimaryReplica(ctx.localNode(), term);

            verify(internalTable).lookup(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(primaryReplica),
                    eq(indexId),
                    any(BinaryTuple.class),
                    isNull()
            );
        }

        input.submit(binaryRow);
        input.close();

        publisher.expectRow(binaryRow);
        publisher.expectCompleted();
    }

    /**
     * Index lookup with specified required columns.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexLookupWithRequiredColumns(boolean ro) {
        NoOpTransaction tx = ro ? RO_TX : RW_TX;

        Tester tester = new Tester();

        int partitionId = 1;
        long term = 2;
        int indexId = 3;

        SubmissionPublisher<BinaryRow> input = newSingleThreadedSubmissionPublisher();

        tester.requiredFields = new BitSet();
        tester.requiredFields.set(1);

        Object[] key = {1};

        TestPublisher<Object[]> publisher = tester.indexLookUp(input, partitionId, term, tx, indexId, key);

        if (ro) {
            verify(internalTable).lookup(
                    eq(partitionId),
                    eq(tx.readTimestamp()),
                    eq(tx.clusterNode()),
                    eq(indexId),
                    any(BinaryTuple.class),
                    eq(tester.requiredFields)
            );
        } else {
            PrimaryReplica primaryReplica = new PrimaryReplica(ctx.localNode(), term);

            verify(internalTable).lookup(
                    eq(partitionId),
                    eq(tx.id()),
                    eq(primaryReplica),
                    eq(indexId),
                    any(BinaryTuple.class),
                    eq(tester.requiredFields)
            );
        }

        input.submit(binaryRow);
        input.close();

        publisher.expectCompleted();
        publisher.expectRow(binaryRow);
    }

    /**
     * Index lookup - error propagation.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexLookupError(boolean ro) {
        NoOpTransaction tx = ro ? RO_TX : RW_TX;

        Tester tester = new Tester();

        int partitionId = 1;
        long term = 2;
        int indexId = 3;

        SubmissionPublisher<BinaryRow> input = newSingleThreadedSubmissionPublisher();

        Object[] key = {1};

        TestPublisher<Object[]> publisher = tester.indexLookUp(input, partitionId, term, tx, indexId, key);

        input.submit(binaryRow);

        RuntimeException err = new RuntimeException("Broken");
        input.closeExceptionally(err);

        publisher.expectRow(binaryRow);
        publisher.expectError(err);
    }

    private class Tester {

        final RelDataType rowType;

        final TableDescriptor tableDescriptor;

        final ScannableTable scannableTable;

        // TableRowConvert that collects converted rows.
        final RowCollectingTableRwoConverter rowConverter = new RowCollectingTableRwoConverter();

        BitSet requiredFields;

        Tester() {
            rowType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                    .add("C1", SqlTypeName.INTEGER)
                    .build();

            tableDescriptor = new TestTableDescriptor(IgniteDistributions::single, rowType);
            scannableTable = new ScannableTableImpl(internalTable, rowConverter, tableDescriptor);
        }

        TestPublisher<Object[]> tableScan(SubmissionPublisher<?> input, int partitionId, long term, NoOpTransaction tx) {
            when(ctx.txAttributes()).thenReturn(TxAttributes.fromTx(tx));
            when(ctx.localNode()).thenReturn(tx.clusterNode());

            if (tx.isReadOnly()) {
                doAnswer(invocation -> input).when(internalTable)
                        .scan(anyInt(), any(HybridTimestamp.class), any(ClusterNode.class));
            } else {
                doAnswer(invocation -> input).when(internalTable)
                        .scan(anyInt(), any(UUID.class), any(PrimaryReplica.class), isNull(), isNull(), isNull(), eq(0), isNull());
            }

            RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;
            RowFactory<Object[]> rowFactory = rowHandler.factory(TYPE_FACTORY, rowType);

            Publisher<Object[]> publisher = scannableTable.scan(ctx, new PartitionWithTerm(partitionId, term), rowFactory, null);

            return new TestPublisher<>(publisher, requiredFields, rowConverter);
        }

        TestPublisher<Object[]> indexScan(SubmissionPublisher<?> input, int partitionId, long term, NoOpTransaction tx,
                int indexId, TestRangeCondition<Object[]> condition) {

            when(ctx.txAttributes()).thenReturn(TxAttributes.fromTx(tx));
            when(ctx.localNode()).thenReturn(tx.clusterNode());

            if (tx.isReadOnly()) {
                doAnswer(i -> input).when(internalTable).scan(
                        anyInt(),
                        any(HybridTimestamp.class),
                        any(ClusterNode.class),
                        any(Integer.class),
                        nullable(BinaryTuplePrefix.class),
                        nullable(BinaryTuplePrefix.class),
                        anyInt(),
                        nullable(BitSet.class));
            } else {
                doAnswer(i -> input).when(internalTable).scan(
                        anyInt(),
                        any(UUID.class),
                        any(PrimaryReplica.class),
                        any(Integer.class),
                        nullable(BinaryTuplePrefix.class),
                        nullable(BinaryTuplePrefix.class),
                        anyInt(),
                        nullable(BitSet.class));
            }

            RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;
            RowFactory<Object[]> rowFactory = rowHandler.factory(TYPE_FACTORY, rowType);
            RangeCondition<Object[]> rangeCondition = condition.asRangeCondition();

            Publisher<Object[]> publisher = scannableTable.indexRangeScan(ctx, new PartitionWithTerm(partitionId, term), rowFactory,
                    indexId, "TEST_IDX", List.of("C1"), rangeCondition, requiredFields);

            return new TestPublisher<>(publisher, requiredFields, rowConverter);
        }

        TestPublisher<Object[]> indexLookUp(SubmissionPublisher<?> input, int partitionId, long term, NoOpTransaction tx,
                int indexId, Object[] key) {

            when(ctx.txAttributes()).thenReturn(TxAttributes.fromTx(tx));
            when(ctx.localNode()).thenReturn(tx.clusterNode());

            if (tx.isReadOnly()) {
                doAnswer(i -> input).when(internalTable).lookup(
                        anyInt(),
                        any(HybridTimestamp.class),
                        any(ClusterNode.class),
                        any(Integer.class),
                        nullable(BinaryTuple.class),
                        nullable(BitSet.class));
            } else {
                doAnswer(i -> input).when(internalTable).lookup(
                        anyInt(),
                        any(UUID.class),
                        any(PrimaryReplica.class),
                        any(Integer.class),
                        nullable(BinaryTuple.class),
                        nullable(BitSet.class));
            }

            RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;
            RowFactory<Object[]> rowFactory = rowHandler.factory(TYPE_FACTORY, rowType);

            Publisher<Object[]> publisher = scannableTable.indexLookup(ctx, new PartitionWithTerm(partitionId, term), rowFactory,
                    indexId, "TEST_IDX", List.of("C1"), key, requiredFields);

            return new TestPublisher<>(publisher, requiredFields, rowConverter);
        }
    }

    static <T> SubmissionPublisher<T> newSingleThreadedSubmissionPublisher() {
        return new SubmissionPublisher<>(Runnable::run, Integer.MAX_VALUE);
    }

    static class RowCollectingTableRwoConverter implements TableRowConverter {

        final List<Map.Entry<BinaryRow, BitSet>> converted = new ArrayList<>();

        @Override
        public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow row, RowFactory<RowT> factory, @Nullable BitSet requiredColumns) {
            converted.add(new SimpleEntry<>(row, requiredColumns));
            return (RowT) new Object[]{0};
        }

        void expectConverted(BinaryRow row, @Nullable BitSet requiredFields) {
            if (!converted.contains(new SimpleEntry<>(row, requiredFields))) {
                fail(format("Unexpected binary row/required fields: {}/{}. Converted: {}", row, requiredFields, converted));
            }
        }
    }

    // Checks that publisher handles onError and onComplete
    static class TestPublisher<T> {

        final Publisher<T> input;

        final CountDownLatch done = new CountDownLatch(1);

        final AtomicReference<Throwable> err = new AtomicReference<>();

        final RowCollectingTableRwoConverter rowConverter;

        final BitSet requiredFields;

        TestPublisher(Publisher<T> input, BitSet requiredFields, RowCollectingTableRwoConverter rowConverter) {
            this.input = input;
            this.rowConverter = rowConverter;
            this.requiredFields = requiredFields;

            input.subscribe(new Subscriber<T>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(T item) {
                    // do nothing.
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
            rowConverter.expectConverted(row, requiredFields);
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
                return SortedIndex.INCLUDE_LEFT | SortedIndex.INCLUDE_RIGHT;
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
                    return lower ? SortedIndex.INCLUDE_LEFT : SortedIndex.INCLUDE_RIGHT;
                case EXCLUSIVE:
                case NONE:
                    return 0;
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    static BinaryTuplePrefix matchBound(@Nullable Object value) {
        if (value == null) {
            return isNull();
        } else {
            return any(BinaryTuplePrefix.class);
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

        void setBounds(Bound lower, @Nullable T lowerValue, Bound upper, @Nullable T upperValue) {
            setLower(lower, lowerValue);
            setUpper(upper, upperValue);
        }

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
                return new RangeCondition<T>() {
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
