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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.index.SortedIndex.INCLUDE_LEFT;
import static org.apache.ignite.internal.index.SortedIndex.INCLUDE_RIGHT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.index.TableIndexConfiguration;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils.RunnableX;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests to check a scan internal command.
 */
public class ItTableScanTest extends ClusterPerClassIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test";

    /** Sorted index name. */
    private static final String SORTED_IDX = "sorted_idx";

    /** Ids to insert. */
    private static final List<Integer> ROW_IDS = List.of(1, 2, 5, 6, 7, 10, 53);

    /** The only partition in the table. */
    private static final int PART_ID = 0;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT32, false)},
            new Column[]{
                    new Column("valInt", NativeTypes.INT32, false),
                    new Column("valStr", NativeTypes.STRING, false)
            }
    );

    private TableImpl table;

    private InternalTable internalTable;

    @BeforeEach
    public void beforeTest() {
        table = getOrCreateTable();

        internalTable = table.internalTable();

        loadData(table);
    }

    @AfterEach
    public void afterTest() {
        clearData(table);
    }

    @Test
    public void testInsertWaitScanComplete() throws Exception {
        IgniteTransactions transactions = igniteTx();

        InternalTransaction tx0 = (InternalTransaction) transactions.begin();
        InternalTransaction tx1 = startTxWithEnlistedPartition(PART_ID, false);

        int sortedIndexId = getSortedIndexId();

        List<BinaryRow> scannedRows = new ArrayList<>();

        PrimaryReplica recipient = getLeaderRecipient(PART_ID, tx1);

        Publisher<BinaryRow> publisher = new RollbackTxOnErrorPublisher<>(
                tx1,
                internalTable.scan(PART_ID, tx1.id(), recipient, sortedIndexId, null, null, 0, null)
        );

        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(2);

        assertTrue(waitForCondition(() -> scannedRows.size() == 2, 10_000));

        assertFalse(scanned.isDone());

        CompletableFuture<Void> updateKey2Fut = table.keyValueView()
                .putAsync(tx0, Tuple.create().set("key", 2), Tuple.create().set("valInt", 2).set("valStr", "New_2"));

        assertFalse(updateKey2Fut.isDone());

        subscription.request(1_000); // Request so much entries here to close the publisher.

        assertThat(scanned, willCompleteSuccessfully());

        CompletableFuture<Void> insertKey99Fut = table.keyValueView()
                .putAsync(tx0, Tuple.create().set("key", 99), Tuple.create().set("valInt", 99).set("valStr", "New_99"));

        assertFalse(insertKey99Fut.isDone());

        log.info("Result: " + scannedRows.stream().map(ItTableScanTest::rowToString).collect(Collectors.joining(", ")));

        assertEquals(ROW_IDS.size(), scannedRows.size());

        tx1.commit();

        assertThat(updateKey2Fut, willCompleteSuccessfully());
        assertThat(insertKey99Fut, willCompleteSuccessfully());

        tx0.commit();
    }

    @Test
    public void testInsertDuringScan() throws Exception {
        int sortedIndexId = getSortedIndexId();

        List<BinaryRow> scannedRows = new ArrayList<>();

        Publisher<BinaryRow> publisher = internalTable.scan(0, null, sortedIndexId, null, null, 0, null);

        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(1);

        assertTrue(waitForCondition(() -> !scannedRows.isEmpty(), 10_000));

        assertEquals(1, scannedRows.size());

        assertFalse(scanned.isDone());

        table.keyValueView().put(null, Tuple.create().set("key", 3), Tuple.create().set("valInt", 3).set("valStr", "New_3"));

        subscription.request(1_000); // Request so much entries here to close the publisher.

        IgniteTestUtils.await(scanned);

        log.info("Result: " + scannedRows.stream().map(ItTableScanTest::rowToString).collect(Collectors.joining(", ")));

        assertEquals(ROW_IDS.size() + 1, scannedRows.size());
    }

    @Test
    public void testUpsertDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.upsert(createKeyValueRow(3), tx)
                .thenApply(unused -> 1)
        );
    }

    @Test
    public void testUpsertAllDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.upsertAll(List.of(createKeyValueRow(3), createKeyValueRow(60)), tx)
                .thenApply(unused -> 2)
        );
    }

    @Test
    public void testGetAndUpsertDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.getAndUpsert(createKeyValueRow(3), tx)
                .thenApply(previous -> {
                    assertNull(previous);

                    return 1;
                })
        );
    }

    @Test
    public void testInsertDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.insert(createKeyValueRow(3), tx)
                .thenApply(inserted -> {
                    assertTrue(inserted);

                    return 1;
                })
        );
    }

    @Test
    public void testInsertAllDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.insertAll(List.of(createKeyValueRow(3), createKeyValueRow(60)), tx)
                .thenApply(notInsertedRows -> {
                    assertTrue(notInsertedRows.isEmpty());

                    return 2;
                })
        );
    }

    @Test
    public void testReplaceDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.replace(createKeyValueRow(6), tx)
                .thenApply(inserted -> {
                    assertTrue(inserted);

                    return 0;
                })
        );
    }

    @Test
    @Disabled("IGNITE-18299 Value comparison in table operations")
    public void testReplaceOldDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.replace(createOldKeyValueRow(6), createKeyValueRow(6), tx)
                .thenApply(inserted -> {
                    assertTrue(inserted);

                    return 0;
                })
        );
    }

    @Test
    public void testGetAndReplaceDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.getAndReplace(createKeyValueRow(6), tx)
                .thenApply(previous -> {
                    assertNotNull(previous);

                    return 0;
                })
        );
    }

    @Test
    public void testDeleteDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.delete(createKeyRow(6), tx)
                .thenApply(deleted -> {
                    assertTrue(deleted);

                    return -1;
                })
        );
    }

    @Test
    public void testDeleteAllDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.deleteAll(List.of(createKeyRow(6), createKeyRow(10)), tx)
                .thenApply(deletedRows -> {
                    assertEquals(0, deletedRows.size());

                    return -2;
                })
        );
    }

    @Test
    @Disabled("IGNITE-18299 Value comparison in table operations")
    public void testDeleteExactDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.deleteExact(createOldKeyValueRow(6), tx)
                .thenApply(deleted -> {
                    assertTrue(deleted);

                    return -1;
                })
        );
    }

    @Test
    @Disabled("IGNITE-18299 Value comparison in table operations")
    public void testDeleteAllExactDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.deleteAllExact(List.of(createOldKeyValueRow(6), createOldKeyValueRow(10)), tx)
                .thenApply(deletedRows -> {
                    assertEquals(2, deletedRows.size());

                    return -2;
                })
        );
    }

    @Test
    public void testGetAndDeleteDuringPureTableScan() throws Exception {
        pureTableScan(tx -> internalTable.getAndDelete(createKeyRow(6), tx)
                .thenApply(deleted -> {
                    assertNotNull(deleted);

                    return -1;
                })
        );
    }

    /**
     * The method executes an operation, encapsulated in closure, during a pure table scan.
     *
     * @param txOperationAction An closure to apply during the scan operation.
     * @throws Exception If failed.
     */
    public void pureTableScan(Function<InternalTransaction, CompletableFuture<Integer>> txOperationAction) throws Exception {
        InternalTransaction tx = (InternalTransaction) CLUSTER_NODES.get(0).transactions().begin();

        log.info("Old transaction [id={}]", tx.id());

        List<BinaryRow> scannedRows = new ArrayList<>();

        Publisher<BinaryRow> publisher = internalTable.scan(0, null, null, null, null, 0, null);

        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(1);

        assertTrue(waitForCondition(() -> !scannedRows.isEmpty(), 10_000));

        assertEquals(1, scannedRows.size());
        assertFalse(scanned.isDone());

        var txOpFut = txOperationAction.apply(tx);

        assertFalse(txOpFut.isDone());

        subscription.request(2);

        assertTrue(waitForCondition(() -> scannedRows.size() == 3, 10_000));

        assertFalse(scanned.isDone());
        assertFalse(txOpFut.isDone());

        subscription.request(1_000); // Request so much entries here to close the publisher.

        IgniteTestUtils.await(scanned);

        log.info("Result: " + scannedRows.stream().map(ItTableScanTest::rowToString).collect(Collectors.joining(", ")));

        assertThat(txOpFut, willCompleteSuccessfully());

        tx.commit();

        assertEquals(ROW_IDS.size(), scannedRows.size());

        var pub = internalTable.scan(0, null, null, null, null, 0, null);

        assertEquals(ROW_IDS.size() + txOpFut.get(), scanAllRows(pub).size());
    }

    @Test
    public void testTwiceScanInTransaction() throws Exception {
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        int sortedIndexId = getSortedIndexId();

        List<BinaryRow> scannedRows = new ArrayList<>();

        InternalTransaction tx = startTxWithEnlistedPartition(PART_ID, false);

        PrimaryReplica recipient = getLeaderRecipient(PART_ID, tx);

        Publisher<BinaryRow> publisher = new RollbackTxOnErrorPublisher<>(
                tx,
                internalTable.scan(PART_ID, tx.id(), recipient, sortedIndexId, null, null, 0, null)
        );

        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(3);

        assertTrue(waitForCondition(() -> scannedRows.size() == 3, 10_000));

        assertFalse(scanned.isDone());

        assertThrows(TransactionException.class,
                () -> kvView.put(null, Tuple.create().set("key", 3), Tuple.create().set("valInt", 3).set("valStr", "New_3")));

        kvView.put(null, Tuple.create().set("key", 8), Tuple.create().set("valInt", 8).set("valStr", "New_8"));

        subscription.request(1_000); // Request so much entries here to close the publisher.

        IgniteTestUtils.await(scanned);

        log.info("Result: " + scannedRows.stream().map(ItTableScanTest::rowToString).collect(Collectors.joining(", ")));

        assertEquals(ROW_IDS.size() + 1, scannedRows.size());

        Publisher<BinaryRow> publisher1 = new RollbackTxOnErrorPublisher<>(
                tx,
                internalTable.scan(PART_ID, tx.id(), recipient, sortedIndexId, null, null, 0, null)
        );

        assertEquals(scanAllRows(publisher1).size(), scannedRows.size());

        assertTrue(scanned.isDone());

        tx.commit();

        kvView.put(null, Tuple.create().set("key", 3), Tuple.create().set("valInt", 3).set("valStr", "New_3"));
    }

    @Test
    public void testScanWithUpperBound() throws Exception {
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        BinaryTuplePrefix lowBound = BinaryTuplePrefix.fromBinaryTuple(new BinaryTuple(1,
                new BinaryTupleBuilder(1).appendInt(5).build()));
        BinaryTuplePrefix upperBound = BinaryTuplePrefix.fromBinaryTuple(new BinaryTuple(1,
                new BinaryTupleBuilder(1).appendInt(9).build()));

        int soredIndexId = getSortedIndexId();

        InternalTransaction tx = startTxWithEnlistedPartition(PART_ID, false);
        PrimaryReplica recipient = getLeaderRecipient(PART_ID, tx);

        Publisher<BinaryRow> publisher = new RollbackTxOnErrorPublisher<>(
                tx,
                internalTable.scan(
                        PART_ID,
                        tx.id(),
                        recipient,
                        soredIndexId,
                        lowBound,
                        upperBound,
                        INCLUDE_LEFT | INCLUDE_RIGHT,
                        null
                )
        );

        List<BinaryRow> scannedRows = scanAllRows(publisher);

        log.info("Result of scanning in old transaction: " + scannedRows.stream().map(ItTableScanTest::rowToString)
                .collect(Collectors.joining(", ")));

        assertEquals(3, scannedRows.size());

        assertThrows(TransactionException.class, () ->
                kvView.put(null, Tuple.create().set("key", 8), Tuple.create().set("valInt", 8).set("valStr", "New_8")));
        assertThrows(TransactionException.class, () ->
                kvView.put(null, Tuple.create().set("key", 9), Tuple.create().set("valInt", 9).set("valStr", "New_9")));

        Publisher<BinaryRow> publisher1 = new RollbackTxOnErrorPublisher<>(
                tx,
                internalTable.scan(
                        PART_ID,
                        tx.id(),
                        recipient,
                        soredIndexId,
                        lowBound,
                        upperBound,
                        INCLUDE_LEFT | INCLUDE_RIGHT,
                        null
                )
        );

        List<BinaryRow> scannedRows1 = scanAllRows(publisher1);

        tx.commit();

        assertEquals(scannedRows.size(), scannedRows1.size());

        kvView.put(null, Tuple.create().set("key", 8), Tuple.create().set("valInt", 8).set("valStr", "New_8"));

        kvView.put(null, Tuple.create().set("key", 9), Tuple.create().set("valInt", 9).set("valStr", "New_9"));

        Publisher<BinaryRow> publisher2 = internalTable.scan(
                PART_ID,
                null,
                soredIndexId,
                lowBound,
                upperBound,
                INCLUDE_LEFT | INCLUDE_RIGHT,
                null
        );

        List<BinaryRow> scannedRows2 = scanAllRows(publisher2);

        assertEquals(5, scannedRows2.size());

        log.info("Result of scanning after insert rows: " + scannedRows2.stream().map(ItTableScanTest::rowToString)
                .collect(Collectors.joining(", ")));
    }

    @Test
    public void testPhantomReads() throws Exception {
        int iterations = 10;

        // "for" is better at detecting data races than RepeatedTest.
        for (int i = 0; i < iterations; i++) {
            KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

            int sortedIndexId = getSortedIndexId();

            InternalTransaction tx = startTxWithEnlistedPartition(PART_ID, false);

            try {
                PrimaryReplica recipient = getLeaderRecipient(PART_ID, tx);

                Publisher<BinaryRow> publisher = new RollbackTxOnErrorPublisher<>(
                        tx,
                        internalTable.scan(PART_ID, tx.id(), recipient, sortedIndexId, null, null, 0, null)
                );

                // Non-thread-safe collection is fine, HB is guaranteed by "Thread#join" inside of "runRace".
                List<BinaryRow> scannedRows = new ArrayList<>();

                IntFunction<RunnableX> put = intValue -> () -> {
                    try {
                        kvView.put(null, Tuple.create().set("key", intValue),
                                Tuple.create().set("valInt", intValue).set("valStr", "Str_" + intValue));
                    } catch (TransactionException e) {
                        // May happen, this is a race after all.
                        assertThat(e.getMessage(), containsString("Failed to acquire a lock"));
                    }
                };

                runRace(
                        put.apply(3),
                        put.apply(4),
                        put.apply(8),
                        put.apply(9),
                        () -> scannedRows.addAll(scanAllRows(publisher))
                );

                Publisher<BinaryRow> publisher1 = new RollbackTxOnErrorPublisher<>(
                        tx,
                        internalTable.scan(PART_ID, tx.id(), recipient, sortedIndexId, null, null, 0, null)
                );

                assertEquals(scanAllRows(publisher1).size(), scannedRows.size());
            } finally {
                tx.commit();
            }

            clearData(table);

            if (i != iterations - 1) {
                loadData(table);
            }
        }
    }

    /**
     * Ensures that multiple consecutive scan requests with different requested rows amount
     * return the expected total number of requested rows.
     *
     * @param requestAmount1 Number of rows in the first request.
     * @param requestAmount2 Number of rows in the second request.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @CsvSource({"3, 1", "1, 3"})
    public void testCompositeScanRequest(int requestAmount1, int requestAmount2) throws Exception {
        List<BinaryRow> scannedRows = new ArrayList<>();
        Publisher<BinaryRow> publisher = internalTable.scan(0, null, null, null, null, 0, null);
        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(requestAmount1);
        subscription.request(requestAmount2);

        int total = requestAmount1 + requestAmount2;
        assertTrue(waitForCondition(() -> scannedRows.size() == total, 10_000),
                "expected=" + total + ", actual=" + scannedRows.size());

        subscription.cancel();

        assertThat(scanned, willCompleteSuccessfully());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMvScan(boolean readOnly) throws Exception {
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        kvView.remove(null, Tuple.create().set("key", ROW_IDS.get(0)));
        kvView.remove(null, Tuple.create().set("key", ROW_IDS.get(1)));
        kvView.put(null, Tuple.create().set("key", ROW_IDS.get(2)), Tuple.create().set("valInt", 999).set("valStr", "Str_999"));

        int sortedIndexId = getSortedIndexId();

        InternalTransaction tx = startTxWithEnlistedPartition(PART_ID, readOnly);

        try {
            Publisher<BinaryRow> publisher;

            if (readOnly) {
                List<String> assignments = internalTable.assignments();

                // Any node from assignments will do it.
                ClusterNode node0 = CLUSTER_NODES.get(0).clusterNodes().stream().filter(clusterNode -> {
                    return assignments.contains(clusterNode.name());
                }).findFirst().orElseThrow();

                //noinspection DataFlowIssue
                publisher = internalTable.scan(PART_ID, tx.readTimestamp(), node0, sortedIndexId, null, null, 0, null);
            } else {
                PrimaryReplica recipient = getLeaderRecipient(PART_ID, tx);

                publisher = new RollbackTxOnErrorPublisher<>(
                        tx,
                        internalTable.scan(PART_ID, tx.id(), recipient, sortedIndexId, null, null, 0, null)
                );
            }

            List<BinaryRow> scannedRows = scanAllRows(publisher);

            // Two rows are removed, one changed.
            assertThat(scannedRows, hasSize(ROW_IDS.size() - 2));
        } finally {
            tx.commit();
        }
    }

    private PrimaryReplica getLeaderRecipient(int partId, InternalTransaction tx) {
        IgniteBiTuple<ClusterNode, Long> leaderWithTerm = tx.enlistedNodeAndTerm(new TablePartitionId(table.tableId(), partId));

        return new PrimaryReplica(leaderWithTerm.get1(), leaderWithTerm.get2());
    }

    /**
     * Represents a binary row as a string.
     *
     * @param binaryRow Binary row.
     * @return String representation.
     */
    private static String rowToString(BinaryRow binaryRow) {
        Row row = Row.wrapBinaryRow(SCHEMA, binaryRow);

        return IgniteStringFormatter.format("[{}, {}, {}]", row.intValue(0), row.intValue(1), row.stringValue(2));
    }

    /**
     * Scans all rows form given publisher.
     *
     * @param publisher Publisher.
     * @return List of scanned rows.
     * @throws Exception If failed.
     */
    private List<BinaryRow> scanAllRows(Publisher<BinaryRow> publisher) throws Exception {
        List<BinaryRow> scannedRows = new ArrayList<>();
        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(1_000); // Request so much entries here to close the publisher.

        assertTrue(waitForCondition(() -> scanned.isDone(), 10_000));

        return scannedRows;
    }

    /**
     * Loads data.
     *
     * @param table Ignite table.
     */
    private static void loadData(TableImpl table) {
        ROW_IDS.forEach(id -> insertRow(id));

        for (Integer rowId : ROW_IDS) {
            Tuple row = table.keyValueView().get(null, Tuple.create().set("key", rowId));

            assertNotNull(row);
            assertEquals("Str_" + rowId, row.value("valStr"));
            assertEquals(rowId, row.value("valInt"));
        }
    }

    /**
     * Clears data with primary keys for 0 to 100.
     *
     * @param table Ignite table.
     */
    private static void clearData(TableImpl table) {
        ArrayList<Tuple> keysToRemove = new ArrayList<>(100);

        IntStream.range(0, 100).forEach(rowId -> keysToRemove.add(Tuple.create().set("key", rowId)));

        table.keyValueView().removeAll(null, keysToRemove);
    }

    /**
     * Gets an index id.
     */
    private static int getSortedIndexId() {
        return getSortedIndexConfig(CLUSTER_NODES.get(0)).id().value();
    }

    private static @Nullable TableIndexConfiguration getSortedIndexConfig(Ignite node) {
        return ((IgniteImpl) node).clusterConfiguration()
                .getConfiguration(TablesConfiguration.KEY)
                .indexes()
                .get(SORTED_IDX.toUpperCase());
    }

    /**
     * Subscribes to a cursor publisher.
     *
     * @param scannedRows List of rows, that were scanned.
     * @param publisher Publisher.
     * @param scanned A future that will be completed when the scan is finished.
     * @return Subscription, that can request rows from cluster.
     */
    private static Subscription subscribeToPublisher(
            List<BinaryRow> scannedRows,
            Publisher<BinaryRow> publisher,
            CompletableFuture<Void> scanned
    ) {
        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();

        publisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscriptionRef.set(subscription);
            }

            @Override
            public void onNext(BinaryRow item) {
                scannedRows.add(item);
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
                scanned.complete(null);
            }
        });

        return subscriptionRef.get();
    }

    /**
     * Creates or gets, if the table already exists, a table with the sorted index.
     *
     * @return Ignite table.
     */
    private static TableImpl getOrCreateTable() {
        sql("CREATE ZONE IF NOT EXISTS ZONE1 WITH REPLICAS=1, PARTITIONS=1;");

        sql("CREATE TABLE IF NOT EXISTS " + TABLE_NAME
                + " (key INTEGER PRIMARY KEY, valInt INTEGER NOT NULL, valStr VARCHAR NOT NULL) WITH PRIMARY_ZONE='ZONE1';");

        sql("CREATE INDEX IF NOT EXISTS " + SORTED_IDX + " ON " + TABLE_NAME + " USING TREE (valInt)");

        return (TableImpl) CLUSTER_NODES.get(0).tables().table(TABLE_NAME);
    }

    /**
     * Adds a new row to the table.
     *
     * @param rowId Primary key of the new row.
     */
    private static void insertRow(int rowId) {
        sql(IgniteStringFormatter.format("INSERT INTO {} (key, valInt, valStr) VALUES ({}, {}, '{}');",
                TABLE_NAME, rowId, rowId, "Str_" + rowId));
    }

    /**
     * Creates an entire row with new value.
     *
     * @param id Primary key.
     * @return Entire row.
     */
    private static Row createKeyValueRow(int id) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA);

        rowBuilder.appendInt(id);
        rowBuilder.appendInt(id);
        rowBuilder.appendString("StrNew_" + id);

        return Row.wrapBinaryRow(SCHEMA, rowBuilder.build());
    }

    /**
     * Creates an entire row with old value.
     *
     * @param id Primary key.
     * @return Entire row.
     */
    private static Row createOldKeyValueRow(int id) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA);

        rowBuilder.appendInt(id);
        rowBuilder.appendInt(id);
        rowBuilder.appendString("Str_" + id);

        return Row.wrapBinaryRow(SCHEMA, rowBuilder.build());
    }

    /**
     * Creates a key row from primary key.
     *
     * @param id Primary key.
     * @return Key row.
     */
    private static Row createKeyRow(int id) {
        RowAssembler rowBuilder = RowAssembler.keyAssembler(SCHEMA);

        rowBuilder.appendInt(id);

        return Row.wrapKeyOnlyBinaryRow(SCHEMA, rowBuilder.build());
    }

    /**
     * Starts an RW transaction and enlists the specified partition in it.
     *
     * @param partId Partition ID.
     * @param readOnly Read-only flag for transaction.
     * @return Transaction.
     */
    private InternalTransaction startTxWithEnlistedPartition(int partId, boolean readOnly) {
        Ignite ignite = CLUSTER_NODES.get(0);

        InternalTransaction tx = (InternalTransaction) ignite.transactions().begin(new TransactionOptions().readOnly(readOnly));

        InternalTable table = ((TableImpl) ignite.tables().table(TABLE_NAME)).internalTable();
        TablePartitionId tblPartId = new TablePartitionId(table.tableId(), partId);
        RaftGroupService raftSvc = table.partitionRaftGroupService(partId);
        long term = IgniteTestUtils.await(raftSvc.refreshAndGetLeaderWithTerm()).term();

        tx.assignCommitPartition(tblPartId);
        tx.enlist(tblPartId, new IgniteBiTuple<>(table.leaderAssignment(partId), term));

        return tx;
    }
}
