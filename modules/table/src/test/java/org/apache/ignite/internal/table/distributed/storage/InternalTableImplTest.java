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

package org.apache.ignite.internal.table.distributed.storage;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS_OR_EQUAL;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.collectMultiRowsResponsesWithRestoreOrder;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.collectRejectedRowsResponses;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapRootCause;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.SingleClusterNodeResolver;
import org.apache.ignite.internal.partition.replicator.network.replication.MultipleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.MultipleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.apache.ignite.internal.partition.replicator.network.replication.ScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.SingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.SingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.SwapRowReplicaRequest;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.NullBinaryRow;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.IndexScanCriteria;
import org.apache.ignite.internal.table.IndexScanCriteria.Range;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.QualifiedNameHelper;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * For {@link InternalTableImpl} testing.
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(SystemPropertiesExtension.class)
public class InternalTableImplTest extends BaseIgniteAbstractTest {
    /** Zone identifier. */
    private static final int ZONE_ID = 1;

    /** Table identifier. */
    private static final int TABLE_ID = 2;

    @Mock
    private PlacementDriver placementDriver;

    @Mock
    private TxManager txManager;

    @Mock
    private ReplicaService replicaService;

    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

    private final InternalClusterNode clusterNode = new ClusterNodeImpl(new UUID(1, 1), "node1", new NetworkAddress("host", 3000));

    @BeforeEach
    void setupMocks() {
        lenient().when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any()))
                .then(invocation -> {
                    ZonePartitionId groupId = invocation.getArgument(0);

                    return completedFuture(
                            new Lease(clusterNode.name(), clusterNode.id(), HybridTimestamp.MIN_VALUE, HybridTimestamp.MAX_VALUE, groupId)
                    );
                });

        lenient().when(txManager.finish(any(), any(), anyBoolean(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any()))
                .thenReturn(nullCompletedFuture());

        // Mock for creating implicit transactions when null is passed
        lenient().when(txManager.beginImplicitRw(any())).then(invocation -> {
            HybridTimestampTracker tracker = invocation.getArgument(0);
            return new ReadWriteTransactionImpl(
                    txManager,
                    tracker,
                    TestTransactionIds.newTransactionId(),
                    randomUUID(),
                    true, // implicit
                    10_000
            );
        });

        lenient().when(replicaService.invoke(anyString(), any())).then(invocation -> {
            ReplicaRequest request = invocation.getArgument(1);

            if (request instanceof MultipleRowReplicaRequest
                    && ((MultipleRowReplicaRequest) request).requestType() == RequestType.RW_UPSERT_ALL) {
                return nullCompletedFuture();
            }

            if (request instanceof MultipleRowPkReplicaRequest) {
                int rowCount = ((MultipleRowPkReplicaRequest) request).primaryKeys().size();
                return completedFuture(Collections.nCopies(rowCount, null));
            }
            if (request instanceof MultipleRowReplicaRequest) {
                int rowCount = ((MultipleRowReplicaRequest) request).binaryRows().size();
                return completedFuture(Collections.nCopies(rowCount, null));
            }

            if (isSingleRowReplicaRequest(request, RequestType.RW_INSERT)
                    || isSingleRowReplicaRequest(request, RequestType.RW_REPLACE_IF_EXIST)
                    || isSingleRowReplicaRequest(request, RequestType.RW_DELETE_EXACT)) {
                return trueCompletedFuture();
            }
            if (isSingleRowPkReplicaRequest(request, RequestType.RW_DELETE)) {
                return trueCompletedFuture();
            }
            if (request instanceof SwapRowReplicaRequest) {
                return trueCompletedFuture();
            }

            if (request instanceof ScanRetrieveBatchReplicaRequest) {
                return emptyListCompletedFuture();
            }

            return nullCompletedFuture();
        });
    }

    private static boolean isSingleRowReplicaRequest(ReplicaRequest request, RequestType requestType) {
        return request instanceof SingleRowReplicaRequest && ((SingleRowReplicaRequest) request).requestType() == requestType;
    }

    private static boolean isSingleRowPkReplicaRequest(ReplicaRequest request, RequestType requestType) {
        return request instanceof SingleRowPkReplicaRequest && ((SingleRowPkReplicaRequest) request).requestType() == requestType;
    }

    private InternalTableImpl newInternalTable(int tableId, int partitionCount) {
        // number of partitions.
        return new InternalTableImpl(
                QualifiedNameHelper.fromNormalized(SqlCommon.DEFAULT_SCHEMA_NAME, "test"),
                ZONE_ID,
                tableId,
                partitionCount, // number of partitions.
                new SingleClusterNodeResolver(clusterNode),
                txManager,
                mock(MvTableStorage.class),
                replicaService,
                mock(ClockService.class),
                HybridTimestampTracker.atomicTracker(null),
                placementDriver,
                new TransactionInflights(placementDriver, clockService),
                () -> mock(ScheduledExecutorService.class),
                mock(StreamerReceiverRunner.class),
                () -> 10_000L,
                () -> 10_000L,
                new TableMetricSource(QualifiedName.fromSimple("test"))
        );
    }

    @Test
    void testRowBatchByPartitionId() {
        InternalTableImpl internalTable = newInternalTable(TABLE_ID, 3);

        List<BinaryRowEx> originalRows = List.of(
                // Rows for 0 partition.
                createBinaryRow(0),
                createBinaryRow(0),
                // Rows for 1 partition.
                createBinaryRow(1),
                // Rows for 2 partition.
                createBinaryRow(2),
                createBinaryRow(2),
                createBinaryRow(2)
        );

        // We will get batches for processing and check them.
        Int2ObjectMap<RowBatch> rowBatchByPartitionId = internalTable.toRowBatchByPartitionId(originalRows);

        assertThat(rowBatchByPartitionId.get(0).requestedRows, hasSize(2));
        assertThat(rowBatchByPartitionId.get(0).originalRowOrder, contains(0, 1));

        assertThat(rowBatchByPartitionId.get(1).requestedRows, hasSize(1));
        assertThat(rowBatchByPartitionId.get(1).originalRowOrder, contains(2));

        assertThat(rowBatchByPartitionId.get(2).requestedRows, hasSize(3));
        assertThat(rowBatchByPartitionId.get(2).originalRowOrder, contains(3, 4, 5));

        // Collect the result and check it.
        rowBatchByPartitionId.get(0).resultFuture = completedFuture(List.of(originalRows.get(0), originalRows.get(1)));
        rowBatchByPartitionId.get(1).resultFuture = completedFuture(List.of(originalRows.get(2)));
        rowBatchByPartitionId.get(2).resultFuture = completedFuture(List.of(originalRows.get(3), originalRows.get(4), originalRows.get(5)));

        assertThat(
                collectMultiRowsResponsesWithRestoreOrder(rowBatchByPartitionId.values()),
                willBe(equalTo(originalRows))
        );

        var part1 = new ArrayList<>(2);

        part1.add(null);
        part1.add(new NullBinaryRow());

        rowBatchByPartitionId.get(0).resultFuture = completedFuture(part1);

        var part2 = new ArrayList<>(2);

        part2.add(null);

        rowBatchByPartitionId.get(1).resultFuture = completedFuture(part2);

        var part3 = new ArrayList<>(2);

        part3.add(new NullBinaryRow());
        part3.add(null);
        part3.add(new NullBinaryRow());

        rowBatchByPartitionId.get(2).resultFuture = completedFuture(part3);

        List<BinaryRowEx> rejectedRows = List.of(
                // Rows for 0 partition.
                originalRows.get(0),
                // Rows for 1 partition.
                originalRows.get(2),
                // Rows for 2 partition.
                originalRows.get(4)
        );

        List<RowBatch> partitionOrderBatch = new ArrayList<>();
        partitionOrderBatch.add(rowBatchByPartitionId.get(0));
        partitionOrderBatch.add(rowBatchByPartitionId.get(1));
        partitionOrderBatch.add(rowBatchByPartitionId.get(2));

        assertThat(
                collectRejectedRowsResponses(partitionOrderBatch),
                willBe(equalTo(rejectedRows))
        );
    }

    private static BinaryRowEx createBinaryRow(int colocationHash) {
        BinaryRowEx rowEx = mock(BinaryRowEx.class);

        lenient().when(rowEx.colocationHash()).thenReturn(colocationHash);

        byte[] data = new byte[0];
        lenient().when(rowEx.tupleSlice()).thenReturn(ByteBuffer.wrap(data));
        lenient().when(rowEx.tupleSliceLength()).thenReturn(data.length);

        return rowEx;
    }

    private static BinaryRowEx createBinaryRow() {
        return createBinaryRow(0);
    }

    @ParameterizedTest
    @EnumSource(EnlistingOperation.class)
    void tableIdGetsEnlisted(EnlistingOperation operation) {
        InternalTable table = newInternalTable(10, 1);
        InternalTransaction transaction = newReadWriteTransaction();

        assertThat(operation.perform(table, transaction), willCompleteSuccessfully());

        transaction.commit();

        PendingTxPartitionEnlistment enlistment = extractSingleEnlistmentForZone();
        assertThat(enlistment.tableIds(), contains(table.tableId()));
    }

    @ParameterizedTest
    @EnumSource(EnlistingOperation.class)
    void anotherTableIdGetsEnlistedInSameZonePartitionEnlistment(EnlistingOperation operation) {
        InternalTable table1 = newInternalTable(10, 1);
        InternalTable table2 = newInternalTable(11, 1);
        InternalTransaction transaction = newReadWriteTransaction();

        assertThat(table1.upsert(createBinaryRow(), transaction), willCompleteSuccessfully());
        assertThat(operation.perform(table2, transaction), willCompleteSuccessfully());

        transaction.commit();

        PendingTxPartitionEnlistment enlistment = extractSingleEnlistmentForZone();
        assertThat(enlistment.tableIds(), containsInAnyOrder(table1.tableId(), table2.tableId()));
    }

    private PendingTxPartitionEnlistment extractSingleEnlistmentForZone() {
        return extractSingleEnlistmentForZone(1, 0);
    }

    private PendingTxPartitionEnlistment extractSingleEnlistmentForZone(int expected, int partition) {
        Map<ZonePartitionId, PendingTxPartitionEnlistment> capturedEnlistments = extractEnlistmentsFromTxFinish();
        assertThat(capturedEnlistments, is(aMapWithSize(expected)));
        PendingTxPartitionEnlistment enlistment = capturedEnlistments.get(new ZonePartitionId(ZONE_ID, partition));
        assertThat(enlistment, is(notNullValue()));
        return enlistment;
    }

    private Map<ZonePartitionId, PendingTxPartitionEnlistment> extractEnlistmentsFromTxFinish() {
        ArgumentCaptor<Map<ZonePartitionId, PendingTxPartitionEnlistment>> enlistmentsCaptor = ArgumentCaptor.captor();

        verify(txManager).finish(any(), any(), anyBoolean(), anyBoolean(), anyBoolean(), false, enlistmentsCaptor.capture(), any());

        return enlistmentsCaptor.getValue();
    }

    private InternalTransaction newReadWriteTransaction() {
        return new ReadWriteTransactionImpl(
                txManager,
                mock(HybridTimestampTracker.class),
                TestTransactionIds.newTransactionId(),
                randomUUID(),
                false,
                10_000
        );
    }

    private InternalTransaction newReadOnlyTransaction() {
        InternalTransaction readOnlyTx = mock(InternalTransaction.class);
        lenient().when(readOnlyTx.id()).thenReturn(TestTransactionIds.newTransactionId());
        lenient().when(readOnlyTx.isReadOnly()).thenReturn(true);
        lenient().when(readOnlyTx.readTimestamp()).thenReturn(clock.now());
        lenient().when(readOnlyTx.implicit()).thenReturn(false);
        lenient().when(readOnlyTx.remote()).thenReturn(false);
        lenient().when(readOnlyTx.state()).thenReturn(null);
        lenient().when(readOnlyTx.isRolledBackWithTimeoutExceeded()).thenReturn(false);
        return readOnlyTx;
    }

    private enum EnlistingOperation {
        GET((table, tx) -> table.get(createBinaryRow(), tx)),
        GET_ALL((table, tx) -> table.getAll(List.of(createBinaryRow()), tx)),
        UPSERT((table, tx) -> table.upsert(createBinaryRow(), tx)),
        UPSERT_ALL((table, tx) -> table.upsertAll(List.of(createBinaryRow()), tx)),
        GET_AND_UPSERT((table, tx) -> table.getAndUpsert(createBinaryRow(), tx)),
        INSERT((table, tx) -> table.insert(createBinaryRow(), tx)),
        INSERT_ALL((table, tx) -> table.insertAll(List.of(createBinaryRow()), tx)),
        REPLACE((table, tx) -> table.replace(createBinaryRow(), tx)),
        REPLACE_EXACT((table, tx) -> table.replace(createBinaryRow(), createBinaryRow(), tx)),
        GET_AND_REPLACE((table, tx) -> table.getAndReplace(createBinaryRow(), tx)),
        DELETE((table, tx) -> table.delete(createBinaryRow(), tx)),
        DELETE_EXACT((table, tx) -> table.deleteExact(createBinaryRow(), tx)),
        GET_AND_DELETE((table, tx) -> table.getAndDelete(createBinaryRow(), tx)),
        DELETE_ALL((table, tx) -> table.deleteAll(List.of(createBinaryRow()), tx)),
        DELETE_ALL_EXACT((table, tx) -> table.deleteAllExact(List.of(createBinaryRow()), tx)),
        SCAN_MV_STORAGE(adaptScan((table, tx) -> table.scan(0, tx))),
        SCAN_INDEX(adaptScan((table, tx) -> table.scan(0, tx, 1, IndexScanCriteria.unbounded())));

        private final BiFunction<InternalTable, InternalTransaction, CompletableFuture<?>> action;

        EnlistingOperation(BiFunction<InternalTable, InternalTransaction, CompletableFuture<?>> action) {
            this.action = action;
        }

        private static BiFunction<InternalTable, InternalTransaction, CompletableFuture<?>> adaptScan(
                BiFunction<InternalTable, InternalTransaction, Publisher<BinaryRow>> func
        ) {
            return (table, tx) -> {
                CompletableFuture<Void> resultFuture = new CompletableFuture<>();

                Publisher<BinaryRow> publisher = func.apply(table, tx);

                publisher.subscribe(new BlackholeSubscriber(resultFuture));

                return resultFuture;
            };
        }

        CompletableFuture<?> perform(InternalTable table, InternalTransaction transaction) {
            return action.apply(table, transaction);
        }
    }

    @Test
    void testInvalidPartitionParameterScan() {
        InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);

        // Test negative partition ID
        assertThrowsWithCause(() -> internalTable.scan(-1, null), IllegalArgumentException.class,
                "Invalid partition [partition=-1, minValue=0, maxValue=0].");

        // Test partition ID >= number of partitions (table has 1 partition, so partition 1 is invalid)
        assertThrowsWithCause(() -> internalTable.scan(1, null), IllegalArgumentException.class,
                "Invalid partition [partition=1, minValue=0, maxValue=0].");
    }

    @Nested
    class ScanWithIndexAndRangeCriteriaTest {
        private static final int VALID_INDEX_ID = 1;
        private static final int VALID_PARTITION = 0;
        private static final int PARTITION_COUNT = 3;

        private Supplier<CompletableFuture<Void>> buildAction(InternalTable table, int partition,
                InternalTransaction tx, int index, Range range) {
            return () -> {
                Publisher<BinaryRow> publisher = table.scan(partition, tx, index, range);
                CompletableFuture<Void> resultFuture = new CompletableFuture<>();
                publisher.subscribe(new BlackholeSubscriber(resultFuture));
                return resultFuture;
            };
        }

        private Supplier<CompletableFuture<Void>> buildAction(InternalTable table, InternalTransaction tx, int index, Range range) {
            return () -> {
                Publisher<BinaryRow> publisher = table.scan(VALID_PARTITION, tx, index, range);
                CompletableFuture<Void> resultFuture = new CompletableFuture<>();
                publisher.subscribe(new BlackholeSubscriber(resultFuture));
                return resultFuture;
            };
        }

        private void commitTxAndAssertEnlistment(InternalTransaction tx, InternalTableImpl internalTable) {
            assertDoesNotThrow(tx::commit);
            PendingTxPartitionEnlistment enlistment = extractSingleEnlistmentForZone();
            assertThat(enlistment.tableIds(), contains(internalTable.tableId()));
        }

        /**
         * Creates a BinaryTuplePrefix with a single integer value.
         */
        private BinaryTuplePrefix createBinaryTuplePrefix(int value) {
            BinaryTuple tuple = new BinaryTuple(1, new BinaryTupleBuilder(1).appendInt(value).build());
            return BinaryTuplePrefix.fromBinaryTuple(tuple);
        }

        @Test
        void testInvalidPartitionIdThrowsException() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, PARTITION_COUNT);
            InternalTransaction tx = newReadWriteTransaction();
            // Test negative partition ID

            assertThrowsWithCause(() -> internalTable.scan(-1, tx, VALID_INDEX_ID, IndexScanCriteria.unbounded()),
                    IllegalArgumentException.class,
                    "Invalid partition [partition=-1, minValue=0, maxValue=2]");

            // Test partition ID >= number of partitions
            assertThrowsWithCause(() -> internalTable.scan(PARTITION_COUNT + 1, tx, VALID_INDEX_ID, IndexScanCriteria.unbounded()),
                    IllegalArgumentException.class,
                    "Invalid partition [partition=4, minValue=0, maxValue=2]");

            // Test partition ID == number of partitions (boundary)
            assertThrowsWithCause(() -> internalTable.scan(PARTITION_COUNT, tx, VALID_INDEX_ID, IndexScanCriteria.unbounded()),
                    IllegalArgumentException.class,
                    "Invalid partition [partition=3, minValue=0, maxValue=2]");
        }

        @Test
        void testValidPartitionIds() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, PARTITION_COUNT);
            InternalTransaction tx = newReadWriteTransaction();

            // Test valid partition IDs
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                Supplier<CompletableFuture<Void>> action = buildAction(internalTable, partition, tx,
                        VALID_INDEX_ID, IndexScanCriteria.unbounded());
                assertThat(action.get(), willCompleteSuccessfully());
            }
            assertDoesNotThrow(tx::commit);
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                PendingTxPartitionEnlistment enlistment = extractSingleEnlistmentForZone(3, partition);
                assertThat(enlistment.tableIds(), contains(internalTable.tableId()));
            }
        }

        @Test
        void testNullTransactionCreatesImplicit() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);

            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, null, VALID_INDEX_ID, IndexScanCriteria.unbounded());
            assertThat(action.get(), willCompleteSuccessfully());
        }

        @Test
        void testUnboundedRange() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();
            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, tx, VALID_INDEX_ID, IndexScanCriteria.unbounded());
            // Test unbounded range (both bounds null)
            assertThat(action.get(), willCompleteSuccessfully());

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @ParameterizedTest
        @CsvSource({
            "0, 0",           // GREATER | LESS (both exclusive)
            "1, 0",           // GREATER_OR_EQUAL | LESS (lower inclusive, upper exclusive)
            "0, 2",           // GREATER | LESS_OR_EQUAL (lower exclusive, upper inclusive)
            "1, 2"            // GREATER_OR_EQUAL | LESS_OR_EQUAL (both inclusive)
        })
        void testRangeWithDifferentFlags(int lowerFlag, int upperFlag) {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            BinaryTuplePrefix lowerBound = createBinaryTuplePrefix(10);
            BinaryTuplePrefix upperBound = createBinaryTuplePrefix(20);
            int flags = lowerFlag | upperFlag;

            IndexScanCriteria.Range criteria = IndexScanCriteria.range(lowerBound, upperBound, flags);
            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, tx, VALID_INDEX_ID, criteria);

            assertThat(action.get(), willCompleteSuccessfully());

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testLowerBoundOnly() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            BinaryTuplePrefix lowerBound = createBinaryTuplePrefix(10);
            IndexScanCriteria.Range criteria = IndexScanCriteria.range(lowerBound, null, GREATER_OR_EQUAL);
            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, tx, VALID_INDEX_ID, criteria);

            assertThat(action.get(), willCompleteSuccessfully());

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testUpperBoundOnly() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            BinaryTuplePrefix upperBound = createBinaryTuplePrefix(20);
            IndexScanCriteria.Range criteria = IndexScanCriteria.range(null, upperBound, LESS_OR_EQUAL);
            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, tx, VALID_INDEX_ID, criteria);

            assertThat(action.get(), willCompleteSuccessfully());

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testBothBoundsSet() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            BinaryTuplePrefix lowerBound = createBinaryTuplePrefix(10);
            BinaryTuplePrefix upperBound = createBinaryTuplePrefix(20);
            IndexScanCriteria.Range criteria = IndexScanCriteria.range(lowerBound, upperBound, GREATER_OR_EQUAL | LESS_OR_EQUAL);
            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, tx, VALID_INDEX_ID, criteria);

            assertThat(action.get(), willCompleteSuccessfully());

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testEqualBounds() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            BinaryTuplePrefix bound = createBinaryTuplePrefix(10);
            IndexScanCriteria.Range criteria = IndexScanCriteria.range(bound, bound, GREATER_OR_EQUAL | LESS_OR_EQUAL);
            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, tx, VALID_INDEX_ID, criteria);

            assertThat(action.get(), willCompleteSuccessfully());

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testFlagsWithNullBounds() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            // Flags should be ignored when bounds are null, but should not throw
            IndexScanCriteria.Range criteria = IndexScanCriteria.range(null, null, GREATER_OR_EQUAL | LESS_OR_EQUAL);
            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, tx, VALID_INDEX_ID, criteria);

            assertThat(action.get(), willCompleteSuccessfully());

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testZeroIndexId() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            // Index ID 0 might be valid (could be primary key index)
            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, tx, 0, IndexScanCriteria.unbounded());

            assertThat(action.get(), willCompleteSuccessfully());

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testNegativeIndexId() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            // Negative index ID - should be handled gracefully
            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, tx, -1, IndexScanCriteria.unbounded());

            assertThat(action.get(), willCompleteSuccessfully());

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testLargeIndexId() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            // Large index ID - should be handled gracefully
            Supplier<CompletableFuture<Void>> action = buildAction(internalTable, tx, Integer.MAX_VALUE, IndexScanCriteria.unbounded());

            assertThat(action.get(), willCompleteSuccessfully());

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testScanWithDataReturned() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            // Create mock binary rows to return
            BinaryRow row1 = mock(BinaryRow.class);
            BinaryRow row2 = mock(BinaryRow.class);
            List<BinaryRow> scanResults = List.of(row1, row2);

            // Mock replica service to return data for scan requests
            when(replicaService.invoke(anyString(), any(ReadWriteScanRetrieveBatchReplicaRequest.class)))
                    .thenReturn(completedFuture(scanResults));

            Publisher<BinaryRow> publisher = internalTable.scan(0, tx, VALID_INDEX_ID, IndexScanCriteria.unbounded());

            CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);

            // Wait for scan to complete
            assertThat(resultFuture, willCompleteSuccessfully());
            List<BinaryRow> collectedRows = resultFuture.join();

            // Verify that data was returned
            assertThat(collectedRows, hasSize(2));

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testScanWithEmptyResult() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            // Mock replica service to return empty list
            when(replicaService.invoke(anyString(), any(ReadWriteScanRetrieveBatchReplicaRequest.class)))
                    .thenReturn((CompletableFuture) emptyListCompletedFuture());

            Publisher<BinaryRow> publisher = internalTable.scan(0, tx, VALID_INDEX_ID, IndexScanCriteria.unbounded());

            CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);

            // Wait for scan to complete
            assertThat(resultFuture, willCompleteSuccessfully());
            List<BinaryRow> collectedRows = resultFuture.join();

            // Verify that empty result was returned
            assertThat(collectedRows, hasSize(0));

            commitTxAndAssertEnlistment(tx, internalTable);
        }

        @Test
        void testScanWithCommittedTransactionThrowsException() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            // Mock transaction manager to mark transaction as committed
            UUID txId = tx.id();
            when(txManager.stateMeta(txId)).thenReturn(new TxStateMeta(
                    TxState.COMMITTED,
                    txId,
                    null,
                    null,
                    null,
                    null
            ));

            // Commit the transaction
            assertDoesNotThrow(tx::commit);

            // Try to scan with committed transaction - should throw TransactionException
            Publisher<BinaryRow> publisher = internalTable.scan(VALID_PARTITION, tx, VALID_INDEX_ID, IndexScanCriteria.unbounded());

            CompletableFuture<Void> completed = new CompletableFuture<>();

            publisher.subscribe(new BlackholeSubscriber(completed));

            // Wait for error
            try {
                completed.get(10, TimeUnit.SECONDS);
                fail("Expected TransactionException but scan completed successfully");
            } catch (Exception e) {
                Throwable cause = unwrapRootCause(e);
                assertThat("Error should be TransactionException", cause, is(instanceOf(TransactionException.class)));
                TransactionException txEx = (TransactionException) cause;
                assertThat("Error code should be TX_ALREADY_FINISHED_ERR", txEx.code(), is(TX_ALREADY_FINISHED_ERR));
            }
        }

        @Test
        void testScanWithAbortedTransactionThrowsException() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction tx = newReadWriteTransaction();

            // Mock transaction manager to mark transaction as aborted
            UUID txId = tx.id();
            when(txManager.stateMeta(txId)).thenReturn(new TxStateMeta(
                    TxState.ABORTED,
                    txId,
                    null,
                    null,
                    null,
                    null
            ));

            // Rollback the transaction
            assertDoesNotThrow(tx::rollback);

            // Try to scan with aborted transaction - should throw TransactionException
            Publisher<BinaryRow> publisher = internalTable.scan(VALID_PARTITION, tx, VALID_INDEX_ID, IndexScanCriteria.unbounded());

            CompletableFuture<Void> completed = new CompletableFuture<>();
            publisher.subscribe(new BlackholeSubscriber(completed));

            // Wait for error
            try {
                completed.get(10, TimeUnit.SECONDS);
                fail("Expected TransactionException but scan completed successfully");
            } catch (Exception e) {
                Throwable cause = unwrapRootCause(e);
                assertThat("Error should be TransactionException", cause, is(instanceOf(TransactionException.class)));
                TransactionException txEx = (TransactionException) cause;
                assertThat("Error code should be TX_ALREADY_FINISHED_ERR", txEx.code(), is(TX_ALREADY_FINISHED_ERR));
            }
        }

        @Test
        void testScanWithReadOnlyTransactionThrowsException() {
            InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);
            InternalTransaction readOnlyTx = newReadOnlyTransaction();

            try {
                internalTable.scan(VALID_PARTITION, readOnlyTx, VALID_INDEX_ID, IndexScanCriteria.unbounded());
                fail("Expected exception when scanning with read-only transaction");
            } catch (TransactionException e) {
                assertThat("Error code should be TX_FAILED_READ_WRITE_OPERATION_ERR",
                        e.code(), is(TX_FAILED_READ_WRITE_OPERATION_ERR));
            }
        }
    }
}
