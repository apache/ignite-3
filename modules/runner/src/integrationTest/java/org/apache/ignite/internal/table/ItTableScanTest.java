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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests to check a scan internal command.
 */
public class ItTableScanTest extends AbstractBasicIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test";

    /** Sorted index name. */
    public static final String SORTED_IDX = "sorted_idx";

    /** Ids to insert. */
    private static final List<Integer> ROW_IDS = List.of(1, 2, 5, 6, 7, 10, 53);

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT32, false)},
            new Column[]{
                    new Column("valInt", NativeTypes.INT32, false),
                    new Column("valInt", NativeTypes.STRING, false)
            }
    );

    @BeforeEach
    public void beforeTest() {
        TableImpl table = getOrCreateTable();

        loadData(table);
    }

    @AfterEach
    public void afterTest() {
        clearData(getOrCreateTable());
    }

    @Test
    public void testInsertWaitScanComplete() throws Exception {
        TableImpl table = getOrCreateTable();
        IgniteTransactions transactions = CLUSTER_NODES.get(0).transactions();

        InternalTransaction tx0 = (InternalTransaction) transactions.begin();
        InternalTransaction tx1 = (InternalTransaction) transactions.begin();

        InternalTable internalTable = table.internalTable();

        UUID soredIndexId = getSortedIndexId();

        ArrayList<ByteBuffer> scannedRows = new ArrayList<>();

        Publisher<BinaryRow> publisher = internalTable.scan(0, tx1, soredIndexId, null, null, 0, null);

        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(2);

        IgniteTestUtils.waitForCondition(() -> scannedRows.size() == 2, 10_000);

        assertEquals(2, scannedRows.size());
        assertFalse(scanned.isDone());

        CompletableFuture<Void> insertFut = table.keyValueView()
                .putAsync(tx0, Tuple.create().set("key", 3), Tuple.create().set("valInt", 3).set("valStr", "New_3"));

        assertFalse(insertFut.isDone());

        subscription.request(1_000); // Request so much entries here to close the publisher.

        IgniteTestUtils.await(scanned);

        assertEquals(ROW_IDS.size(), scannedRows.size());

        tx1.commit();
        IgniteTestUtils.await(insertFut);

        tx0.commit();
    }

    @Test
    public void testInsertDuringScan() throws Exception {
        TableImpl table = getOrCreateTable();

        InternalTable internalTable = table.internalTable();

        UUID soredIndexId = getSortedIndexId();

        ArrayList<ByteBuffer> scannedRows = new ArrayList<>();

        Publisher<BinaryRow> publisher = internalTable.scan(0, null, soredIndexId, null, null, 0, null);

        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(1);

        IgniteTestUtils.waitForCondition(() -> !scannedRows.isEmpty(), 10_000);

        assertEquals(1, scannedRows.size());
        assertFalse(scanned.isDone());

        table.keyValueView().put(null, Tuple.create().set("key", 3), Tuple.create().set("valInt", 3).set("valStr", "New_3"));

        subscription.request(1_000); // Request so much entries here to close the publisher.

        IgniteTestUtils.await(scanned);

        //TODO: IGNITE-18243 Uncomment this change when the scan on a sorted index will be redesigned.
        //assertEquals(ROW_IDS.size() + 1, scannedRows.size());
    }

    @Test
    public void testUpsertDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.upsert(createKeyValueRow(3), tx)
                    .thenApply(unused -> 1);
        });
    }

    @Test
    public void testUpsertAllDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.upsertAll(List.of(createKeyValueRow(3), createKeyValueRow(60)), tx)
                    .thenApply(unused -> 2);
        });
    }

    @Test
    public void testGetAndUpsertDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.getAndUpsert(createKeyValueRow(3), tx)
                    .thenApply(previous -> {
                        assertNull(previous);

                        return 1;
                    });
        });
    }

    @Test
    public void testInsertDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.insert(createKeyValueRow(3), tx)
                    .thenApply(inserted -> {
                        assertTrue(inserted);

                        return 1;
                    });
        });
    }

    @Test
    public void testInsertAllDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.insertAll(List.of(createKeyValueRow(3), createKeyValueRow(60)), tx)
                    .thenApply(notInsertedRows -> {
                        assertTrue(notInsertedRows.isEmpty());

                        return 2;
                    });
        });
    }

    @Test
    public void testReplaceDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.replace(createKeyValueRow(6), tx)
                    .thenApply(inserted -> {
                        assertTrue(inserted);

                        return 0;
                    });
        });
    }

    @Test
    @Disabled("IGNITE-18299 Value comparison in table operations")
    public void testReplaceOldDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.replace(createOldKeyValueRow(6), createKeyValueRow(6), tx)
                    .thenApply(inserted -> {
                        assertTrue(inserted);

                        return 0;
                    });
        });
    }

    @Test
    public void testGetAndReplaceDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.getAndReplace(createKeyValueRow(6), tx)
                    .thenApply(previous -> {
                        assertNotNull(previous);

                        return 0;
                    });
        });
    }

    @Test
    public void testDeleteDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.delete(createKeyRow(6), tx)
                    .thenApply(deleted -> {
                        assertTrue(deleted);

                        return -1;
                    });
        });
    }

    @Test
    public void testDeleteAllDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.deleteAll(List.of(createKeyRow(6), createKeyRow(10)), tx)
                    .thenApply(deletedRows -> {
                        assertEquals(2, deletedRows.size());

                        return -2;
                    });
        });
    }

    @Test
    @Disabled("IGNITE-18299 Value comparison in table operations")
    public void testDeleteExactDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.deleteExact(createOldKeyValueRow(6), tx)
                    .thenApply(deleted -> {
                        assertTrue(deleted);

                        return -1;
                    });
        });
    }

    @Test
    @Disabled("IGNITE-18299 Value comparison in table operations")
    public void testDeleteAllExactDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.deleteAllExact(List.of(createOldKeyValueRow(6), createOldKeyValueRow(10)), tx)
                    .thenApply(deletedRows -> {
                        assertEquals(2, deletedRows.size());

                        return -2;
                    });
        });
    }

    @Test
    public void testGetAndDeleteDuringPureTableScan() throws Exception {
        pureTableScan(tx -> {
            TableImpl table = getOrCreateTable();

            InternalTable internalTable = table.internalTable();

            return internalTable.getAndDelete(createKeyRow(6), tx)
                    .thenApply(deleted -> {
                        assertNotNull(deleted);

                        return -1;
                    });
        });
    }

    /**
     * The method executes an operation, encapsulated in closure, during a pure table scan.
     *
     * @param txOperationAction An closure to apply during the scan operation.
     * @throws Exception If failed.
     */
    public void pureTableScan(Function<InternalTransaction, CompletableFuture<Integer>> txOperationAction) throws Exception {
        TableImpl table = getOrCreateTable();

        InternalTable internalTable = table.internalTable();

        InternalTransaction tx = (InternalTransaction) CLUSTER_NODES.get(0).transactions().begin();

        log.info("Old transaction [id=" + tx.id());

        ArrayList<ByteBuffer> scannedRows = new ArrayList<>();

        Publisher<BinaryRow> publisher = internalTable.scan(0, null, null, null, null, 0, null);

        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(1);

        IgniteTestUtils.waitForCondition(() -> !scannedRows.isEmpty(), 10_000);

        assertEquals(1, scannedRows.size());
        assertFalse(scanned.isDone());

        var txOpFut = txOperationAction.apply(tx);

        assertFalse(txOpFut.isDone());

        subscription.request(2);

        IgniteTestUtils.waitForCondition(() -> scannedRows.size() == 3, 10_000);

        assertEquals(3, scannedRows.size());
        assertFalse(scanned.isDone());
        assertFalse(txOpFut.isDone());

        subscription.request(1_000); // Request so much entries here to close the publisher.

        IgniteTestUtils.await(scanned);

        assertThat(txOpFut, willCompleteSuccessfully());

        tx.commit();

        assertEquals(ROW_IDS.size(), scannedRows.size());

        var pub = internalTable.scan(0, null, null, null, null, 0, null);

        assertEquals(ROW_IDS.size() + txOpFut.get(), scanAllRows(pub).size());
    }

    /**
     * Scans all rows form given publisher.
     *
     * @param publisher Publisher.
     * @return List of scanned rows.
     * @throws Exception If failed.
     */
    private ArrayList<ByteBuffer> scanAllRows(Publisher<BinaryRow> publisher) throws Exception {
        ArrayList<ByteBuffer> scannedRows = new ArrayList<>();
        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(1_000); // Request so much entries here to close the publisher.

        IgniteTestUtils.waitForCondition(() -> scanned.isDone(), 10_000);

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
     *
     * @return Index id.
     */
    private static UUID getSortedIndexId() {
        return ((IgniteImpl) CLUSTER_NODES.get(0)).clusterConfiguration().getConfiguration(TablesConfiguration.KEY).indexes()
                .get(SORTED_IDX.toUpperCase()).id().value();
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
            List<ByteBuffer> scannedRows,
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
                scannedRows.add(item.valueSlice());
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
        sql("CREATE TABLE IF NOT EXISTS " + TABLE_NAME
                + " (key INTEGER PRIMARY KEY, valInt INTEGER, valStr VARCHAR) WITH REPLICAS=1, PARTITIONS=1;");

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
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendInt(id);
        rowBuilder.appendInt(id);
        rowBuilder.appendString("StrNew_" + id);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }

    /**
     * Creates an entire row with old value.
     *
     * @param id Primary key.
     * @return Entire row.
     */
    private static Row createOldKeyValueRow(int id) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendInt(id);
        rowBuilder.appendInt(id);
        rowBuilder.appendString("Str_" + id);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }

    /**
     * Creates a key row from primary key.
     *
     * @param id Primary key.
     * @return Key row.
     */
    private static Row createKeyRow(int id) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendInt(id);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }
}
