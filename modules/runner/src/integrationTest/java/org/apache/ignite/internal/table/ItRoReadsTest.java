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

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.runner.app.ItTablesApiTest.SCHEMA;
import static org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnDefinition;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the read-only API.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItRoReadsTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItRoReadsTest.class);

    private static final String TABLE_NAME = "some-table";

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

    private static Ignite NODE;

    @WorkDirectory
    private static Path WORK_DIR;

    private Table table;

    @BeforeAll
    static void startNode(TestInfo testInfo) {
        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        String nodeName = testNodeName(testInfo, 0);

        String config = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, BASE_PORT, connectNodeAddr);

        CompletableFuture<Ignite> future = TestIgnitionManager.start(nodeName, config, WORK_DIR.resolve(nodeName));

        String metaStorageNodeName = testNodeName(testInfo, nodes() - 1);

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNodeName)
                .metaStorageNodeNames(List.of(metaStorageNodeName))
                .clusterName("cluster")
                .build();

        IgnitionManager.init(initParameters);

        assertThat(future, willCompleteSuccessfully());

        NODE = future.join();
    }

    @AfterAll
    static void stopNode(TestInfo testInfo) throws Exception {
        LOG.info("Start tearDown()");

        NODE = null;

        IgniteUtils.closeAll(() -> IgnitionManager.stop(testNodeName(testInfo, 0)));

        LOG.info("End tearDown()");
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

    @Test
    public void testRoGet() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = ((TableImpl) table).internalTable();

        Row keyValueRow = createKeyValueRow(1, 1, "some string row" + 1);

        BinaryRow res = internalTable.get(keyValueRow, node.clock().now(), node.node()).get();

        assertNull(res);

        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        populateData(node, keyValueView, false);

        res = internalTable.get(keyValueRow, node.clock().now(), node.node()).get();

        assertEquals(res.byteBuffer(), keyValueRow.byteBuffer());
    }

    @Test
    public void testRoGetWithSeveralInserts() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = ((TableImpl) table).internalTable();

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

        assertEquals(res.byteBuffer(), keyValueRow2.byteBuffer());
    }

    @Test
    public void testRoScanWithSeveralInserts() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = ((TableImpl) table).internalTable();

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

        Publisher<BinaryRow> res = internalTable.scan(0, node.clock().now(), node.node());

        CountDownLatch latch = new CountDownLatch(1);

        List<ByteBuffer> list = new ArrayList<>();

        res.subscribe(new Subscriber<BinaryRow>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(100);
            }

            @Override
            public void onNext(BinaryRow item) {
                list.add(item.byteBuffer());
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        latch.await();

        assertEquals(1, list.size());

        assertEquals(list.get(0), keyValueRow2.byteBuffer());
    }

    @Test
    public void testRoGetOngoingCommitIsNotVisible() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = ((TableImpl) table).internalTable();

        Row keyValueRow = createKeyValueRow(1, 1, "some string row" + 1);

        Row keyValueRow2 = createKeyValueRow(1, 2, "some string row" + 2);

        assertNull(internalTable.get(keyValueRow, node.clock().now(), node.node()).get());
        assertNull(internalTable.get(keyValueRow2, node.clock().now(), node.node()).get());

        Transaction tx1 = node.transactions().begin();

        internalTable.insert(keyValueRow, (InternalTransaction) tx1).get();

        tx1.commit();

        Transaction tx2 = node.transactions().begin();

        internalTable.upsert(keyValueRow2, (InternalTransaction) tx2);

        BinaryRow res = internalTable.get(keyValueRow, node.clock().now(), node.node()).get();

        assertEquals(res.byteBuffer(), keyValueRow.byteBuffer());

        tx2.commit();

        res = internalTable.get(keyValueRow, node.clock().now(), node.node()).get();

        assertEquals(res.byteBuffer(), keyValueRow2.byteBuffer());
    }

    @Test
    public void testRoGetAll() throws Exception {
        IgniteImpl node = node();

        InternalTable internalTable = ((TableImpl) table).internalTable();

        Row keyValueRow1 = createKeyValueRow(1, 1, "some string row" + 1);
        Row keyValueRow2 = createKeyValueRow(2, 2, "some string row" + 2);
        Row keyValueRow3 = createKeyValueRow(3, 3, "some string row" + 3);

        Set<BinaryRowEx> rowsToSearch = Set.of(keyValueRow1, keyValueRow2, keyValueRow3);

        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        Collection<BinaryRow> res = internalTable.getAll(rowsToSearch, node.clock().now(), node.node()).get();

        assertEquals(res.size(), 0);

        node.transactions().runInTransaction(txs -> {
            for (int i = 0; i < 15; i++) {
                putValue(keyValueView, i, txs);
            }
        });

        res = internalTable.getAll(rowsToSearch, node.clock().now(), node.node()).get();

        assertEquals(res.size(), 3);

        Set<ByteBuffer> resultKeys = res.stream().map(BinaryRow::byteBuffer).collect(Collectors.toSet());

        assertTrue(resultKeys.contains(keyValueRow1.byteBuffer()));
        assertTrue(resultKeys.contains(keyValueRow2.byteBuffer()));
        assertTrue(resultKeys.contains(keyValueRow3.byteBuffer()));
    }

    @Test
    public void testRoGetAllWithSeveralInserts() throws ExecutionException, InterruptedException {
        IgniteImpl node = node();

        InternalTable internalTable = ((TableImpl) table).internalTable();

        Row keyValueRow1 = createKeyValueRow(1, 1, "some string row" + 1);
        Row keyValueRow2 = createKeyValueRow(2, 2, "some string row" + 2);
        Row keyValueRow3 = createKeyValueRow(3, 3, "some string row" + 3);

        Set<BinaryRowEx> rowsToSearch = Set.of(keyValueRow1, keyValueRow2, keyValueRow3);

        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        Collection<BinaryRow> res = internalTable.getAll(rowsToSearch, node.clock().now(), node.node()).get();

        assertEquals(res.size(), 0);

        populateData(node(), keyValueView, false);

        res = internalTable.getAll(rowsToSearch, node.clock().now(), node.node()).get();

        assertEquals(res.size(), 3);

        Set<ByteBuffer> resultKeys = res.stream().map(BinaryRow::byteBuffer).collect(Collectors.toSet());

        assertTrue(resultKeys.contains(keyValueRow1.byteBuffer()));
        assertTrue(resultKeys.contains(keyValueRow2.byteBuffer()));
        assertTrue(resultKeys.contains(keyValueRow3.byteBuffer()));

        node.transactions().runInTransaction(txs -> {
            for (int i = 0; i < 15; i++) {
                putValue(keyValueView, i + 100, txs);
            }
        });

        Row newKeyValueRow1 = createKeyValueRow(1, 101, "some string row" + 101);
        Row newKeyValueRow2 = createKeyValueRow(2, 102, "some string row" + 102);
        Row newKeyValueRow3 = createKeyValueRow(3, 103, "some string row" + 103);

        res = internalTable.getAll(rowsToSearch, node.clock().now(), node.node()).get();

        assertEquals(res.size(), 3);

        resultKeys = res.stream().map(BinaryRow::byteBuffer).collect(Collectors.toSet());

        assertTrue(resultKeys.contains(newKeyValueRow1.byteBuffer()));
        assertTrue(resultKeys.contains(newKeyValueRow2.byteBuffer()));
        assertTrue(resultKeys.contains(newKeyValueRow3.byteBuffer()));
    }

    @Test
    public void testRoScanAllImplicitPopulatingData() throws InterruptedException {
        roScanAll(true);
    }

    @Test
    public void testRoScanAllExplicitPopulatingData() throws InterruptedException {
        roScanAll(false);
    }

    private void roScanAll(boolean implicit) throws InterruptedException {
        IgniteImpl node = node();

        InternalTable internalTable = ((TableImpl) table).internalTable();

        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        Publisher<BinaryRow> res = internalTable.scan(0, node.clock().now(), node.node());

        var subscriberAllDataAwaitLatch = new CountDownLatch(1);

        var retrievedItems = new ArrayList<BinaryRow>();

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

        subscriberAllDataAwaitLatch.await();

        assertEquals(0, retrievedItems.size());

        populateData(node, keyValueView, implicit);

        res = internalTable.scan(0, node.clock().now(), node.node());

        var subscriberAllDataAwaitLatch2 = new CountDownLatch(1);

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
                subscriberAllDataAwaitLatch2.countDown();
            }
        });

        subscriberAllDataAwaitLatch2.await();

        assertEquals(15, retrievedItems.size());
    }

    private static Row createKeyValueRow(long id, int value, String str) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA_1, false, -1);

        rowBuilder.appendLong(id);
        rowBuilder.appendInt(value);
        rowBuilder.appendString(str);

        return new Row(SCHEMA_1, rowBuilder.build());
    }

    private static Row createKeyRow(long id) {
        RowAssembler rowBuilder = RowAssembler.keyAssembler(SCHEMA_1);

        rowBuilder.appendLong(id);

        return new Row(SCHEMA_1, rowBuilder.build());
    }

    private static void putValue(KeyValueView<Tuple, Tuple> kv, int val) {
        putValue(kv, val, null);
    }

    private static void putValue(KeyValueView<Tuple, Tuple> kv, int val, Transaction tx) {
        Tuple tableKey = Tuple.create().set("key", Long.valueOf(val % 100));

        Tuple value = Tuple.create().set("valInt", Integer.valueOf(val)).set("valStr", "some string row" + val);

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
        List<ColumnDefinition> cols = new ArrayList<>();
        cols.add(SchemaBuilders.column("key", ColumnType.INT64).build());
        cols.add(SchemaBuilders.column("valInt", ColumnType.INT32).asNullable(true).build());
        cols.add(SchemaBuilders.column("valStr", ColumnType.string()).withDefaultValue("default").build());

        String zoneName = "zone_" + tableName;
        int zoneId = await(createZone(((IgniteImpl) node).distributionZoneManager(), zoneName, 1, DEFAULT_REPLICA_COUNT));

        return await(((TableManager) node.tables()).createTableAsync(
                tableName,
                zoneName,
                tblCh -> convert(SchemaBuilders.tableBuilder(SCHEMA, tableName).columns(
                        cols).withPrimaryKey("key").build(), tblCh)
                        .changeZoneId(zoneId)
        ));
    }

    private static void stopTable(Ignite node, String tableName) {
        await(((TableManager) node.tables()).dropTableAsync(tableName));
        await(((IgniteImpl) node).distributionZoneManager().dropZone("zone_" + tableName));
    }

    protected static int nodes() {
        return 1;
    }

    protected static IgniteImpl node() {
        return (IgniteImpl) NODE;
    }
}
