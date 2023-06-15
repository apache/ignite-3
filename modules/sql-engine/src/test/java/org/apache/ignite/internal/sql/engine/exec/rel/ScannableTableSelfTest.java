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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.sql.type.SqlTypeName;
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
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link ScannableTableImpl}.
 * Test cases ensure that operations call appropriate method that perform scan/lookups
 * and created {@link Publisher publishers} work.
 */
@ExtendWith(MockitoExtension.class)
public class ScannableTableSelfTest {

    private static final NoOpTransaction RO_TX = new NoOpTransaction("RO", true);

    private static final NoOpTransaction RW_TX = new NoOpTransaction("RO", false);

    private static final int INDEX_ID = 42;

    private static final String INDEX_NAME = "test_idx";

    @Mock(lenient = true)
    private InternalTable internalTable;

    @Mock(lenient = true)
    private ExecutionContext<Object[]> ctx;

    @Mock
    private BinaryRow binaryRow;

    /**
     * Table scan (ro / rw).
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTableScan(boolean ro) throws InterruptedException {
        Tester tester = new Tester(ro ? RO_TX : RW_TX);
        tester.setRowType(SqlTypeName.INTEGER);

        if (ro) {
            tester.tableScanReadOnly();
        } else {
            tester.tableScanReadWrite();
        }

        SubmissionPublisher<BinaryRow> source = tester.beginTableScan();

        source.submit(binaryRow);
        source.submit(binaryRow);

        source.close();

        tester.expectDone(2);
    }

    /**
     * Table scan (ro / rw) - error propagation.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTableScanErrorPropagation(boolean ro) throws InterruptedException {
        Tester tester = new Tester(ro ? RO_TX : RW_TX);
        tester.setRowType(SqlTypeName.INTEGER);

        if (ro) {
            tester.tableScanReadOnly();
        } else {
            tester.tableScanReadWrite();
        }

        SubmissionPublisher<BinaryRow> source = tester.beginTableScan();

        source.submit(binaryRow);

        RuntimeException err = new RuntimeException("Broken");
        source.closeExceptionally(err);

        Throwable actualErr = tester.expectError();
        assertSame(err, actualErr);
    }

    /**
     * Index scan (ro/rw) - different bounds.
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
    public void testIndexScanBounds(boolean ro, Bound lower, Bound upper) throws InterruptedException {
        Tester tester = new Tester(ro ? RO_TX : RW_TX);
        tester.setRowType(SqlTypeName.INTEGER);

        Object[] lowerValue = lower == Bound.NONE ? null : new Object[]{1};
        Object[] upperValue = upper == Bound.NONE ? null : new Object[]{10};

        tester.setBounds(lower, lowerValue, upper, upperValue);
        tester.setFlags(Bound.toFlags(lower, upper));

        if (ro) {
            tester.indexScanReadOnly(INDEX_ID);
        } else {
            tester.indexScanReadWrite(INDEX_ID);
        }

        SubmissionPublisher<BinaryRow> source = tester.beginIndexScan(INDEX_ID, null);

        source.submit(binaryRow);
        source.close();

        tester.expectDone(1);
    }

    /**
     * Index scan (ro/rw) with required fields.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexScanWithFields(boolean ro) throws InterruptedException {
        Tester tester = new Tester(ro ? RO_TX : RW_TX);
        tester.setRowType(SqlTypeName.INTEGER);

        tester.setBounds(Bound.INCLUSIVE, new Object[]{1}, Bound.NONE, new Object[]{3});
        tester.setFlags(Bound.toFlags(Bound.INCLUSIVE, Bound.NONE));

        BitSet requiredFields = new BitSet();
        requiredFields.set(1);
        requiredFields.set(2);

        tester.setRequiredFields(1, 2);

        if (ro) {
            tester.indexScanReadOnly(INDEX_ID);
        } else {
            tester.indexScanReadWrite(INDEX_ID);
        }

        SubmissionPublisher<BinaryRow> source = tester.beginIndexScan(INDEX_ID, requiredFields);

        source.submit(binaryRow);
        source.close();

        tester.expectDone(1);
    }

    /**
     * Index scan (ro / rw) - error propagation.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexScanErrorPropagation(boolean ro) throws InterruptedException {
        Tester tester = new Tester(ro ? RO_TX : RW_TX);
        tester.setRowType(SqlTypeName.INTEGER);

        tester.setBounds(Bound.INCLUSIVE, new Object[]{1}, Bound.NONE, null);
        tester.setFlags(Bound.toFlags(Bound.INCLUSIVE, Bound.NONE));

        if (ro) {
            tester.indexScanReadOnly(INDEX_ID);
        } else {
            tester.indexScanReadWrite(INDEX_ID);
        }

        SubmissionPublisher<BinaryRow> source = tester.beginIndexScan(INDEX_ID, null);

        source.submit(binaryRow);

        RuntimeException err = new RuntimeException("Broken");
        source.closeExceptionally(err);

        Throwable actualErr = tester.expectError();
        assertSame(err, actualErr);
    }

    /**
     * Index scan (ro/rw) - invalid bounds key.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexScanInvalidKey(boolean ro) {
        Tester tester = new Tester(ro ? RO_TX : RW_TX);
        tester.setRowType(SqlTypeName.INTEGER, SqlTypeName.INTEGER);

        tester.setBounds(Bound.INCLUSIVE, new Object[]{1, 2, 3}, Bound.NONE, null);

        if (ro) {
            tester.indexScanReadOnly(INDEX_ID);
        } else {
            tester.indexScanReadWrite(INDEX_ID);
        }

        AssertionError err = assertThrows(AssertionError.class, () -> tester.beginIndexScan(INDEX_ID, null));
        assertEquals("Invalid range condition", err.getMessage());
    }

    /**
     * Index lookup (ro/rw) (required fields are not specified).
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexLookUp(boolean ro) throws InterruptedException {
        Tester tester = new Tester(ro ? RO_TX : RW_TX);
        tester.setRowType(SqlTypeName.INTEGER);

        tester.setKey(new Object[]{1});

        if (ro) {
            tester.indexLookUpReadOnly(INDEX_ID);
        } else {
            tester.indexLookUpReadWrite(INDEX_ID);
        }

        SubmissionPublisher<BinaryRow> source = tester.beginIndexLookUp(INDEX_ID, null);

        source.submit(binaryRow);
        source.close();

        tester.expectDone(1);
    }

    /**
     * Index lookup (ro/rw) with fields.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexLookUpWithFields(boolean ro) throws InterruptedException {
        Tester tester = new Tester(ro ? RO_TX : RW_TX);
        tester.setRowType(SqlTypeName.INTEGER);

        tester.setKey(new Object[]{1});

        BitSet requiredFields = new BitSet();
        requiredFields.set(1);
        requiredFields.set(2);

        tester.setRequiredFields(1, 2);

        if (ro) {
            tester.indexLookUpReadOnly(INDEX_ID);
        } else {
            tester.indexLookUpReadWrite(INDEX_ID);
        }

        SubmissionPublisher<BinaryRow> source = tester.beginIndexLookUp(INDEX_ID, requiredFields);

        source.submit(binaryRow);
        source.close();

        tester.expectDone(1);
    }

    /**
     * Index lookup (ro / rw) - error propagation.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexLookupErrorPropagation(boolean ro) throws InterruptedException {
        Tester tester = new Tester(ro ? RO_TX : RW_TX);
        tester.setRowType(SqlTypeName.INTEGER, SqlTypeName.INTEGER);

        tester.setKey(new Object[]{1});

        if (ro) {
            tester.indexLookUpReadOnly(INDEX_ID);
        } else {
            tester.indexLookUpReadWrite(INDEX_ID);
        }

        SubmissionPublisher<BinaryRow> source = tester.beginIndexLookUp(INDEX_ID, null);

        source.submit(binaryRow);

        RuntimeException err = new RuntimeException("Broken");
        source.closeExceptionally(err);

        Throwable actualErr = tester.expectError();
        assertSame(err, actualErr);
    }

    /**
     * Index lookup (ro/rw) - invalid key.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndexLookUpInvalidKey(boolean ro) {
        Tester tester = new Tester(ro ? RO_TX : RW_TX);
        tester.setRowType(SqlTypeName.INTEGER, SqlTypeName.INTEGER);

        tester.setKey(new Object[]{1, 2, 3});

        if (ro) {
            tester.indexLookUpReadOnly(INDEX_ID);
        } else {
            tester.indexLookUpReadWrite(INDEX_ID);
        }

        AssertionError err = assertThrows(AssertionError.class, () -> tester.beginIndexLookUp(INDEX_ID, null));
        assertEquals("Invalid lookup key.", err.getMessage());
    }

    class Tester {

        private final PartitionWithTerm partWithTerm = new PartitionWithTerm(1, 2L);

        private final BlockingQueue<Integer> actual = new LinkedBlockingQueue<>();

        private final AtomicInteger counter = new AtomicInteger();

        private final TableRowConverter rowConverter = new TableRowConverter() {

            @Override
            public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow row, RowFactory<RowT> factory,
                    @Nullable BitSet requiredColumns) {
                return (RowT) new Object[]{counter.getAndIncrement()};
            }
        };

        private final NoOpTransaction tx;

        private final SubmissionPublisher<BinaryRow> source = new SubmissionPublisher<>();

        private final TestSearchCondition<Object[]> searchCondition = new TestSearchCondition<>();

        private final CountDownLatch done = new CountDownLatch(1);

        private final AtomicReference<Throwable> error = new AtomicReference<>();

        private RelDataType rowType;

        private ScannableTable scannableTable;

        private RowFactory<Object[]> rowFactory;

        private RowHandler<Object[]> rowHandler;

        private BitSet requiredFields;

        private Operation operation;

        Tester(NoOpTransaction tx) {
            TxAttributes txAttributes = TxAttributes.fromTx(tx);

            when(ctx.txAttributes()).thenReturn(txAttributes);
            when(ctx.localNode()).thenReturn(tx.clusterNode());

            this.tx = tx;
        }

        void setRowType(SqlTypeName... type) {
            IgniteTypeFactory typeFactory = Commons.typeFactory();
            Builder builder = new Builder(typeFactory);

            int i = 1;
            for (SqlTypeName typeName : type) {
                builder = builder.add("C" + i, typeName);
                i++;
            }

            rowType = builder.build();

            TableDescriptor tableDescriptor = new TestTableDescriptor(IgniteDistributions::single, rowType);

            scannableTable = new ScannableTableImpl(internalTable, rowConverter, tableDescriptor);

            rowHandler = ArrayRowHandler.INSTANCE;
            rowFactory = rowHandler.factory(typeFactory, rowType);
        }

        void tableScanReadOnly() {
            when(internalTable.scan(partWithTerm.partId(), tx.readTimestamp(), tx.clusterNode())).thenReturn(source);

            operation = Operation.TABLE_SCAN;
        }

        void tableScanReadWrite() {
            PrimaryReplica recipient = new PrimaryReplica(tx.clusterNode(), partWithTerm.term());

            when(internalTable.scan(partWithTerm.partId(), tx.id(), recipient,
                    null, null, null, 0, null)).thenReturn(source);

            operation = Operation.TABLE_SCAN;
        }

        SubmissionPublisher<BinaryRow> beginTableScan() {
            checkOperation(Operation.TABLE_SCAN);

            Publisher<Object[]> publisher = scannableTable.scan(ctx, partWithTerm, rowFactory, null);

            setSubscriber(publisher);

            return source;
        }

        void setFlags(int flags) {
            this.searchCondition.flags = flags;
        }

        void setBounds(Bound lower, @Nullable Object [] lowerValue, Bound upper, @Nullable Object[] upperValue) {
            if (lower != Bound.NONE) {
                searchCondition.setLower(lowerValue, lower == Bound.INCLUSIVE, lowerValue.length);
            }
            if (upper != Bound.NONE) {
                searchCondition.setUpper(upperValue, upper == Bound.INCLUSIVE, upperValue.length);
            }
        }

        void setRequiredFields(int... fields) {
            if (fields.length != 0) {
                requiredFields = new BitSet();

                for (int b : fields) {
                    requiredFields.set(b);
                }
            }
        }

        void indexScanReadOnly(int indexId) {
            int flags = searchCondition.flags;

            when(internalTable.scan(eq(partWithTerm.partId()), eq(tx.readTimestamp()), eq(tx.clusterNode()),
                    eq(indexId), searchCondition.lowerBound(), searchCondition.upperBound(), eq(flags), eq(requiredFields)))
                    .thenReturn(source);

            operation = Operation.INDEX_SCAN;
        }

        void indexScanReadWrite(int indexId, int... fields) {
            int flags = searchCondition.flags;
            PrimaryReplica recipient = new PrimaryReplica(tx.clusterNode(), partWithTerm.term());

            when(internalTable.scan(eq(partWithTerm.partId()), eq(tx.id()), eq(recipient),
                    eq(indexId), searchCondition.lowerBound(), searchCondition.upperBound(), eq(flags), eq(requiredFields)))
                    .thenReturn(source);

            operation = Operation.INDEX_SCAN;
        }

        SubmissionPublisher<BinaryRow> beginIndexScan(int indexId, @Nullable BitSet requiredFields) {
            checkOperation(Operation.INDEX_SCAN);

            RangeCondition<Object[]> condition = searchCondition.toRangeCondition();

            String idxField = rowType.getFieldList().get(0).getName();

            Publisher<Object[]> publisher = scannableTable.indexRangeScan(ctx, partWithTerm, rowFactory,
                    indexId, INDEX_NAME, List.of(idxField), condition, requiredFields);

            setSubscriber(publisher);

            return source;
        }

        void setKey(Object[] key) {
            searchCondition.setKey(key, key.length);
        }

        void indexLookUpReadOnly(int indexId) {
            when(internalTable.lookup(eq(partWithTerm.partId()), eq(tx.readTimestamp()), eq(tx.clusterNode()),
                    eq(indexId), any(BinaryTuple.class), eq(requiredFields))).thenReturn(source);

            operation = Operation.INDEX_LOOKUP;
        }

        void indexLookUpReadWrite(int indexId) {
            PrimaryReplica recipient = new PrimaryReplica(tx.clusterNode(), partWithTerm.term());

            when(internalTable.lookup(eq(partWithTerm.partId()), eq(tx.id()), eq(recipient),
                    eq(indexId), any(BinaryTuple.class), eq(requiredFields))).thenReturn(source);

            operation = Operation.INDEX_LOOKUP;
        }

        SubmissionPublisher<BinaryRow> beginIndexLookUp(int indexId, @Nullable BitSet requiredFields) {
            checkOperation(Operation.INDEX_LOOKUP);

            RangeCondition<Object[]> condition = searchCondition.toRangeCondition();
            Object[] key = condition.lower();

            String idxField = rowType.getFieldList().get(0).getName();

            Publisher<Object[]> publisher = scannableTable.indexLookup(ctx, partWithTerm, rowFactory,
                    indexId, INDEX_NAME, List.of(idxField), key, requiredFields);

            setSubscriber(publisher);

            return source;
        }

        void expectDone(int count) throws InterruptedException {
            done.await();

            Throwable err = error.get();
            assertNull(err, "Unexpected publisher error");

            List<Integer> rows = IntStream.range(0, count)
                    .boxed()
                    .collect(Collectors.toList());

            assertEquals(rows, List.copyOf(actual), "items");
        }

        Throwable expectError() throws InterruptedException {
            done.await();

            Throwable err = error.get();
            assertNotNull(err, "Expected an error but operation has completed successfully");
            return err;
        }

        private void checkOperation(Operation expected) {
            if (expected != operation) {
                throw new IllegalStateException(format("Unexpected operation. Expected {} but got {}", expected, operation));
            }
        }

        private void setSubscriber(Publisher<Object[]> publisher) {
            if (done.getCount() == 0) {
                throw new IllegalStateException("Completed");
            }

            publisher.subscribe(new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Object[] item) {
                    actual.offer((Integer) item[0]);
                }

                @Override
                public void onError(Throwable t) {
                    error.set(t);
                    done.countDown();
                }

                @Override
                public void onComplete() {
                    done.countDown();
                }
            });
        }
    }

    enum Operation {
        TABLE_SCAN,
        INDEX_SCAN,
        INDEX_LOOKUP
    }

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

    private static class TestSearchCondition<T>  {

        T lower;
        BinaryTuplePrefix lowerPrefix;
        boolean lowerInclude;

        T upper;
        BinaryTuplePrefix upperPrefix;
        boolean upperInclude;

        BinaryTuple keyTuple;

        int flags;

        void setLower(T value, boolean inclusive, int fieldCount) {
            this.lower = value;
            this.lowerInclude = inclusive;
            this.lowerPrefix = BinaryTuplePrefix.fromBinaryTuple(newTuple(fieldCount));
        }

        void setUpper(T value, boolean inclusive, int fieldCount) {
            this.upper = value;
            this.upperInclude = inclusive;
            this.upperPrefix = BinaryTuplePrefix.fromBinaryTuple(newTuple(fieldCount));
        }

        void setKey(T value, int fields) {
            this.lower = value;
            keyTuple = newTuple(fields);
        }

        BinaryTuplePrefix lowerBound() {
            if (lowerPrefix == null) {
                return ArgumentMatchers.isNull();
            } else {
                return ArgumentMatchers.any(BinaryTuplePrefix.class);
            }
        }

        BinaryTuplePrefix upperBound() {
            if (upperPrefix == null) {
                return ArgumentMatchers.isNull();
            } else {
                return ArgumentMatchers.any(BinaryTuplePrefix.class);
            }
        }

        @Nullable
        RangeCondition<T> toRangeCondition() {
            if (lower == null && upper == null) {
                return null;
            } else {
                return new RangeCondition<T>() {
                    @Override
                    public T lower() {
                        return lower;
                    }

                    @Override
                    public T upper() {
                        return upper;
                    }

                    @Override
                    public boolean lowerInclude() {
                        return lowerInclude;
                    }

                    @Override
                    public boolean upperInclude() {
                        return upperInclude;
                    }
                };
            }
        }

        private static BinaryTuple newTuple(int fieldCount) {
            return new BinaryTuple(fieldCount, ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN));
        }

    }
}
