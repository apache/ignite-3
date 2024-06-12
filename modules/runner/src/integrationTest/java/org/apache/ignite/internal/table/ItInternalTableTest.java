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

import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.schema.BinaryRowMatcher.equalToRow;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.EmbeddedNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.thread.PublicApiThreading.ApiEntryRole;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the internal table API.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItInternalTableTest extends BaseIgniteAbstractTest {
    private static final String TABLE_NAME = "SOME_TABLE";

    private static final SchemaDescriptor SCHEMA_1 = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT64, false)},
            new Column[]{
                    new Column("valInt", NativeTypes.INT32, false),
                    new Column("valStr", NativeTypes.STRING, false)
            }
    );

    private static final int BASE_PORT = 3344;

    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    private static EmbeddedNode NODE;

    @WorkDirectory
    private static Path WORK_DIR;

    private Table table;

    @BeforeAll
    static void startNode(TestInfo testInfo) {
        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        String nodeName = testNodeName(testInfo, 0);

        String config = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, BASE_PORT, connectNodeAddr);

        NODE = TestIgnitionManager.start(nodeName, config, WORK_DIR.resolve(nodeName));

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(NODE)
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(NODE, initParameters);

        assertThat(NODE.joinClusterAsync(), willCompleteSuccessfully());
    }

    @AfterAll
    static void stopNode(TestInfo testInfo) throws Exception {
        closeAll(() -> NODE.stop());
        NODE = null;
    }

    @BeforeEach
    void createTable() {
        table = startTable(node(), TABLE_NAME);
    }

    @AfterEach
    void dropTable() {
        stopTable(node(), TABLE_NAME);

        table = null;
    }

    @BeforeEach
    void allowAllOperationsToTestThread() {
        // Doing this as this class tests internal API which relies on public API to mark the threads.
        // Without this marking, thread assertions would go off.
        PublicApiThreading.setThreadRole(ApiEntryRole.SYNC_PUBLIC_API);
    }

    @AfterEach
    void cleanupThreadApiRole() {
        PublicApiThreading.setThreadRole(null);
    }

    @Test
    public void testRoGet() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();

        Row keyValueRow = createKeyValueRow(1, 1, "some string row" + 1);
        Row keyRow = createKeyRow(1);

        BinaryRow res = internalTable.get(keyRow, node.clock().now(), node.node()).get();

        assertNull(res);

        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        populateData(node, keyValueView, false);

        res = internalTable.get(keyRow, node.clock().now(), node.node()).get();

        assertThat(res, is(equalToRow(keyValueRow)));
    }

    @Test
    public void testRoGetWithSeveralInserts() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();

        Row keyValueRow = createKeyValueRow(1, 1, "some string row" + 1);

        Row keyValueRow2 = createKeyValueRow(1, 2, "some string row" + 2);

        Row keyRow = createKeyRow(1);

        assertNull(internalTable.get(keyRow, node.clock().now(), node.node()).get());
        assertNull(internalTable.get(keyRow, node.clock().now(), node.node()).get());

        Transaction tx1 = node.transactions().begin();

        internalTable.upsert(keyValueRow, (InternalTransaction) tx1).get();

        tx1.commit();

        Transaction tx2 = node.transactions().begin();

        internalTable.upsert(keyValueRow2, (InternalTransaction) tx2).get();

        tx2.commit();

        BinaryRow res = internalTable.get(keyRow, node.clock().now(), node.node()).get();

        assertThat(res, is(equalToRow(keyValueRow2)));
    }

    @Test
    public void testRoScanWithSeveralInserts() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();

        Row keyValueRow = createKeyValueRow(1, 1, "some string row" + 1);

        Row keyValueRow2 = createKeyValueRow(1, 2, "some string row" + 2);

        Row keyRow = createKeyRow(1);

        assertNull(internalTable.get(keyRow, node.clock().now(), node.node()).get());
        assertNull(internalTable.get(keyRow, node.clock().now(), node.node()).get());

        Transaction tx1 = node.transactions().begin();

        internalTable.insert(keyValueRow, (InternalTransaction) tx1).get();

        tx1.commit();

        Transaction tx2 = node.transactions().begin();

        internalTable.upsert(keyValueRow2, (InternalTransaction) tx2).get();

        tx2.commit();

        List<BinaryRow> list = scanAllPartitions(node);

        assertThat(list, contains(equalToRow(keyValueRow2)));
    }

    @Test
    public void testRoGetOngoingCommitIsNotVisible() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();

        Row keyRow = createKeyRow(1);

        Row keyValueRow = createKeyValueRow(1, 1, "some string row" + 1);

        Row keyValueRow2 = createKeyValueRow(1, 2, "some string row" + 2);

        assertNull(internalTable.get(keyRow, node.clock().now(), node.node()).get());

        Transaction tx1 = node.transactions().begin();

        internalTable.insert(keyValueRow, (InternalTransaction) tx1).get();

        tx1.commit();

        Transaction tx2 = node.transactions().begin();

        internalTable.upsert(keyValueRow2, (InternalTransaction) tx2);

        BinaryRow res = internalTable.get(keyRow, node.clock().now(), node.node()).get();

        assertThat(res, is(equalToRow(keyValueRow)));

        tx2.commit();

        res = internalTable.get(keyRow, node.clock().now(), node.node()).get();

        assertThat(res, is(equalToRow(keyValueRow2)));
    }

    @Test
    public void testRoGetAll() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();

        var keyRows = new ArrayList<BinaryRowEx>();
        var keyValueRows = new ArrayList<BinaryRowEx>();

        for (int i = 1; i <= 3; i++) {
            keyRows.add(createKeyRow(i));
            keyValueRows.add(createKeyValueRow(i, i, "some string row" + i));
        }

        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        List<BinaryRow> res = internalTable.getAll(keyRows, node.clock().now(), node.node()).get();

        assertEquals(3, res.size());

        node.transactions().runInTransaction(txs -> {
            for (int i = 0; i < 15; i++) {
                putValue(keyValueView, i, txs);
            }
        });

        res = internalTable.getAll(keyRows, node.clock().now(), node.node()).get();

        assertThat(res, contains(equalToRow(keyValueRows.get(0)), equalToRow(keyValueRows.get(1)), equalToRow(keyValueRows.get(2))));
    }

    @Test
    public void testRoGetAllWithSeveralInserts() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();

        var keyRows = new ArrayList<BinaryRowEx>();
        var keyValueRows = new ArrayList<BinaryRowEx>();

        for (int i = 1; i <= 3; i++) {
            keyRows.add(createKeyRow(i));
            keyValueRows.add(createKeyValueRow(i, i, "some string row" + i));
        }

        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        List<BinaryRow> res = internalTable.getAll(keyRows, node.clock().now(), node.node()).get();

        assertEquals(3, res.size());

        populateData(node(), keyValueView, false);

        res = internalTable.getAll(keyRows, node.clock().now(), node.node()).get();

        assertThat(res, contains(equalToRow(keyValueRows.get(0)), equalToRow(keyValueRows.get(1)), equalToRow(keyValueRows.get(2))));

        node.transactions().runInTransaction(txs -> {
            for (int i = 0; i < 15; i++) {
                putValue(keyValueView, i + 100, txs);
            }
        });

        Row newKeyValueRow1 = createKeyValueRow(1, 101, "some string row" + 101);
        Row newKeyValueRow2 = createKeyValueRow(2, 102, "some string row" + 102);
        Row newKeyValueRow3 = createKeyValueRow(3, 103, "some string row" + 103);

        res = internalTable.getAll(keyRows, node.clock().now(), node.node()).get();

        assertThat(res, contains(equalToRow(newKeyValueRow1), equalToRow(newKeyValueRow2), equalToRow(newKeyValueRow3)));
    }

    @Test
    public void testRoScanAllImplicitPopulatingData() throws InterruptedException {
        roScanAll(true);
    }

    @Test
    public void testRoScanAllExplicitPopulatingData() throws InterruptedException {
        roScanAll(false);
    }

    @Test
    public void getAllOrderTest() {
        List<BinaryRowEx> keyRows = populateEvenKeysAndPrepareEntriesToLookup(true);

        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();
        SchemaDescriptor schemaDescriptor = unwrapTableViewInternal(table).schemaView().lastKnownSchema();

        CompletableFuture<List<BinaryRow>> getAllFut = internalTable.getAll(keyRows, null);

        assertThat(getAllFut, willCompleteSuccessfully());

        List<BinaryRow> res = getAllFut.join();

        assertEquals(keyRows.size(), res.size());

        Iterator<BinaryRow> resIter = res.iterator();

        for (BinaryRowEx key : keyRows) {
            int i = TableRow.keyTuple(Row.wrapKeyOnlyBinaryRow(schemaDescriptor, key)).<Long>value("key").intValue();

            BinaryRow resRow = resIter.next();

            if (i % 2 == 1) {
                assertNull(resRow);
            } else {
                assertNotNull(resRow);

                Tuple rowTuple = TableRow.tuple(Row.wrapBinaryRow(schemaDescriptor, resRow));

                assertEquals(i % 100L, rowTuple.<Long>value("key"));
                assertEquals(i, rowTuple.<Integer>value("valInt"));
                assertEquals("some string row" + i, rowTuple.<Integer>value("valStr"));
            }
        }
    }

    @Test
    public void deleteAllOrderTest() {
        List<BinaryRowEx> keyRows = populateEvenKeysAndPrepareEntriesToLookup(true);

        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();
        SchemaDescriptor schemaDescriptor = unwrapTableViewInternal(table).schemaView().lastKnownSchema();

        CompletableFuture<List<BinaryRow>> deleteAllFut = internalTable.deleteAll(keyRows, null);

        assertThat(deleteAllFut, willCompleteSuccessfully());

        List<BinaryRow> res = deleteAllFut.join();

        Iterator<BinaryRow> resIter = res.iterator();

        for (BinaryRowEx key : keyRows) {
            int i = TableRow.keyTuple(Row.wrapKeyOnlyBinaryRow(schemaDescriptor, key)).<Long>value("key").intValue();

            if (i % 2 == 1) {
                Tuple rowTuple = TableRow.keyTuple(Row.wrapKeyOnlyBinaryRow(schemaDescriptor, resIter.next()));

                assertEquals(i % 100L, rowTuple.<Long>value("key"));
            }
        }
    }

    @Test
    public void deleteAllExactOrderTest() {
        List<BinaryRowEx> rowsToLookup = populateEvenKeysAndPrepareEntriesToLookup(false);

        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();
        SchemaDescriptor schemaDescriptor = unwrapTableViewInternal(table).schemaView().lastKnownSchema();

        CompletableFuture<List<BinaryRow>> deleteAllExactFut = internalTable.deleteAllExact(rowsToLookup, null);

        assertThat(deleteAllExactFut, willCompleteSuccessfully());

        List<BinaryRow> res = deleteAllExactFut.join();

        Iterator<BinaryRow> resIter = res.iterator();

        for (BinaryRowEx key : rowsToLookup) {
            int i = TableRow.tuple(Row.wrapBinaryRow(schemaDescriptor, key)).<Long>value("key").intValue();

            if (i % 2 == 1) {
                Tuple rowTuple = TableRow.tuple(Row.wrapBinaryRow(schemaDescriptor, resIter.next()));

                assertEquals(i % 100L, rowTuple.<Long>value("key"));
                assertEquals(i, rowTuple.<Integer>value("valInt"));
                assertEquals("some string row" + i, rowTuple.<Integer>value("valStr"));
            }
        }
    }

    @Test
    public void insertAllOrderTest() {
        List<BinaryRowEx> rowsToLookup = populateEvenKeysAndPrepareEntriesToLookup(false);

        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();
        SchemaDescriptor schemaDescriptor = unwrapTableViewInternal(table).schemaView().lastKnownSchema();

        CompletableFuture<List<BinaryRow>> insertAllFut = internalTable.insertAll(rowsToLookup, null);

        assertThat(insertAllFut, willCompleteSuccessfully());

        List<BinaryRow> res = insertAllFut.join();

        Iterator<BinaryRow> resIter = res.iterator();

        for (BinaryRowEx key : rowsToLookup) {
            int i = TableRow.tuple(Row.wrapBinaryRow(schemaDescriptor, key)).<Long>value("key").intValue();

            if (i % 2 == 0) {
                Tuple rowTuple = TableRow.tuple(Row.wrapBinaryRow(schemaDescriptor, resIter.next()));

                assertEquals(i % 100L, rowTuple.<Long>value("key"));
                assertEquals(i, rowTuple.<Integer>value("valInt"));
                assertEquals("some string row" + i, rowTuple.<Integer>value("valStr"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void updateAllOrderTest(boolean existingKey) {
        RecordView<Tuple> view = table.recordView();
        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();
        List<BinaryRowEx> rows = new ArrayList<>();

        int count = 100;
        int lastId = count - 1;
        long id = existingKey ? 1 : 12345;
        BitSet deleted = new BitSet(count);

        if (existingKey) {
            view.upsert(null, Tuple.create().set("key", id).set("valInt", 1).set("valStr", "val1"));
        }

        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                rows.add(createKeyRow(id));
                deleted.set(i);
            } else {
                rows.add(createKeyValueRow(id, i, "row-" + i));
            }
        }

        int partitionId = internalTable.partitionId(rows.get(0));

        internalTable.updateAll(rows, deleted, partitionId).join();

        Tuple res = view.get(null, Tuple.create().set("key", id));
        assertEquals(lastId, res.intValue("valInt"));
        assertEquals("row-" + lastId, res.stringValue("valStr"));
    }

    @Test
    public void updateAllWithDeleteTest() {
        InternalTable internalTable = unwrapTableViewInternal(table).internalTable();

        RecordView<Tuple> view = table.recordView();
        view.upsert(null, Tuple.create().set("key", 1L).set("valInt", 1).set("valStr", "val1"));
        view.upsert(null, Tuple.create().set("key", 3L).set("valInt", 3).set("valStr", "val3"));

        // Update, insert, delete.
        List<BinaryRowEx> rows = List.of(
                createKeyValueRow(1, 11, "val11"),
                createKeyValueRow(3, 2, "val2"),
                createKeyRow(5)
        );

        int partitionId = internalTable.partitionId(rows.get(0));

        for (int i = 0; i < rows.size(); i++) {
            assertEquals(partitionId, internalTable.partitionId(rows.get(i)), "Unexpected partition for row " + i);
        }

        BitSet deleted = new BitSet(3);
        deleted.set(2);
        internalTable.updateAll(rows, deleted, partitionId).join();

        var row1 = view.get(null, Tuple.create().set("key", 1L));
        assertEquals(11, row1.intValue("valInt"));

        var row2 = view.get(null, Tuple.create().set("key", 3L));
        assertEquals(2, row2.intValue("valInt"));

        var row3 = view.get(null, Tuple.create().set("key", 5L));
        assertNull(row3);
    }

    private ArrayList<BinaryRowEx> populateEvenKeysAndPrepareEntriesToLookup(boolean keyOnly) {
        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        var keyRows = new ArrayList<BinaryRowEx>();

        for (int i = 0; i < 15; i++) {
            keyRows.add(keyOnly ? createKeyRow(i) : createKeyValueRow(i, i, "some string row" + i));

            if (i % 2 == 0) {
                putValue(keyValueView, i);
            }
        }

        Collections.shuffle(keyRows);

        return keyRows;
    }

    private void roScanAll(boolean implicit) throws InterruptedException {
        IgniteImpl node = node();

        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        List<BinaryRow> retrievedItems = scanAllPartitions(node);

        assertEquals(0, retrievedItems.size());

        populateData(node, keyValueView, implicit);

        retrievedItems = scanAllPartitions(node);

        assertEquals(15, retrievedItems.size());
    }


    /**
     * Scans all table entries.
     *
     * @param node Ignite instance.
     * @return Collection with all rows.
     * @throws InterruptedException If fail.
     */
    private static List<BinaryRow> scanAllPartitions(IgniteImpl node) throws InterruptedException {
        InternalTable internalTable = unwrapTableViewInternal(node.tables().table(TABLE_NAME)).internalTable();

        List<BinaryRow> retrievedItems = new CopyOnWriteArrayList<>();

        int parts = internalTable.partitions();

        var subscriberAllDataAwaitLatch = new CountDownLatch(parts);

        InternalTransaction roTx =
                (InternalTransaction) node.transactions().begin(new TransactionOptions().readOnly(true));

        for (int i = 0; i < parts; i++) {
            Publisher<BinaryRow> res = internalTable.scan(i, roTx.id(), node.clock().now(), node.node(), roTx.coordinatorId());

            res.subscribe(new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscription.request(10000);
                }

                @Override
                public void onNext(BinaryRow item) {
                    retrievedItems.add(item);
                }

                @Override
                public void onError(Throwable throwable) {
                    fail("onError call is not expected.");
                }

                @Override
                public void onComplete() {
                    subscriberAllDataAwaitLatch.countDown();
                }
            });
        }

        assertTrue(subscriberAllDataAwaitLatch.await(10, TimeUnit.SECONDS));

        roTx.commit();

        return retrievedItems;
    }

    private static Row createKeyValueRow(long id, int value, String str) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA_1, -1);

        rowBuilder.appendLong(id);
        rowBuilder.appendInt(value);
        rowBuilder.appendString(str);

        return Row.wrapBinaryRow(SCHEMA_1, rowBuilder.build());
    }

    private static Row createKeyRow(long id) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA_1.version(), SCHEMA_1.keyColumns(), -1);

        rowBuilder.appendLong(id);

        return Row.wrapKeyOnlyBinaryRow(SCHEMA_1, rowBuilder.build());
    }

    private static void putValue(KeyValueView<Tuple, Tuple> kv, int val) {
        putValue(kv, val, null);
    }

    private static void putValue(KeyValueView<Tuple, Tuple> kv, int val, Transaction tx) {
        Tuple tableKey = Tuple.create().set("key", (long) (val % 100));

        Tuple value = Tuple.create().set("valInt", val).set("valStr", "some string row" + val);

        kv.put(tx, tableKey, value);
    }

    private static void populateData(Ignite node, KeyValueView<Tuple, Tuple> keyValueView, boolean implicit) {
        if (implicit) {
            for (int i = 0; i < 15; i++) {
                putValue(keyValueView, i);
            }
        } else {
            Transaction tx1 = node.transactions().begin();

            for (int i = 0; i < 15; i++) {
                putValue(keyValueView, i, tx1);
            }

            tx1.commit();
        }
    }

    private static Table startTable(Ignite node, String tableName) {
        String zoneName = zoneNameForTable(tableName);
        IgniteSql sql = node.sql();

        sql.execute(null, String.format("create zone \"%s\" with partitions=3, replicas=%d, storage_profiles='%s'",
                zoneName, DEFAULT_REPLICA_COUNT, DEFAULT_STORAGE_PROFILE));

        sql.execute(null,
                String.format(
                        "create table \"%s\" (key bigint primary key, valInt int, valStr varchar default 'default') "
                                + "with primary_zone='%s'",
                        tableName, zoneName
                )
        );

        Table table = node.tables().table(tableName);

        assertNotNull(table);

        return table;
    }

    private static String zoneNameForTable(String tableName) {
        return "ZONE_" + tableName;
    }

    private static void stopTable(Ignite node, String tableName) {
        IgniteSql sql = node.sql();

        sql.execute(null, "drop table " + tableName);
        sql.execute(null, "drop zone " + zoneNameForTable(tableName));
    }

    protected static int nodes() {
        return 1;
    }

    protected static IgniteImpl node() {
        return (IgniteImpl) NODE.joinClusterAsync().join();
    }
}
