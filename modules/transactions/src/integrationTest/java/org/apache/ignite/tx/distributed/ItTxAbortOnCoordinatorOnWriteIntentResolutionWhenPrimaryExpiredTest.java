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

package org.apache.ignite.tx.distributed;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runInExecutor;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.findTupleToBeHostedOnNode;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.partitionIdForTuple;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.table;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.txId;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.waitAndGetPrimaryReplica;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.zoneId;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for transaction abort on coordinator when write-intent resolution happens after primary replica expiration.
 */
public class ItTxAbortOnCoordinatorOnWriteIntentResolutionWhenPrimaryExpiredTest extends ClusterPerTestIntegrationTest {
    /** Zone name. */
    private static final String ZONE_NAME = "test_zone";

    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    private static final Tuple INITIAL_TUPLE = Tuple.create().set("key", 1L).set("val", "1");

    private static final Function<Tuple, Tuple> NEXT_TUPLE = t -> Tuple.create()
            .set("key", t.longValue("key") + 1)
            .set("val", "" + (t.longValue("key") + 1));

    private ExecutorService storageExecutor;

    @BeforeEach
    public void setup() {
        storageExecutor = Executors.newSingleThreadExecutor(
                IgniteThreadFactory.create("test", "storage-test-pool", log, ThreadOperation.STORAGE_READ)
        );

        String zoneSql = "create zone " + ZONE_NAME + " (partitions 20, replicas 3) storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String sql = "create table " + TABLE_NAME + " (key bigint primary key, val varchar(20)) zone " + ZONE_NAME;

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    @AfterEach
    public void tearDown() {
        shutdownAndAwaitTermination(storageExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{0, 1, 2};
    }

    @Test
    public void test() throws Exception {
        IgniteImpl firstPrimaryNode = anyNode();
        IgniteImpl coordinatorNode = findNode(n -> !n.name().equals(firstPrimaryNode.name()));

        log.info("Test: firstPrimaryNode: {}, firstPrimaryNode id: {}", firstPrimaryNode.name(), firstPrimaryNode.id());
        log.info("Test: coordinatorNode: {}, coordinatorNode id: {}", coordinatorNode.name(), coordinatorNode.id());

        RecordView<Tuple> view = coordinatorNode.tables().table(TABLE_NAME).recordView();

        Transaction txx = coordinatorNode.transactions().begin();
        Tuple tuple = findTupleToBeHostedOnNode(firstPrimaryNode, TABLE_NAME, txx, INITIAL_TUPLE, NEXT_TUPLE, true);
        int partId = partitionIdForTuple(firstPrimaryNode, TABLE_NAME, tuple, txx);
        var groupId = new ZonePartitionId(zoneId(firstPrimaryNode, TABLE_NAME), partId);
        log.info("Test: groupId: " + groupId);
        view.upsert(txx, tuple);

        txx.commit();

        Transaction tx0 = coordinatorNode.transactions().begin();
        log.info("Test: unfinished tx id: " + txId(tx0));
        view.upsert(tx0, tuple);
        // Don't commit or rollback tx0.

        // Wait for replication of write intent.
        Tuple keyTuple = Tuple.create().set("key", tuple.longValue("key"));
        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> checkWriteIntentInStorageOnAllNodes(partId, keyTuple));

        UUID firstPrimaryNodeId = firstPrimaryNode.id();

        log.info("Test: node stop: " + firstPrimaryNode.name());
        firstPrimaryNode.stop();

        // Wait for lease to expire.
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    ReplicaMeta meta = coordinatorNode.placementDriver()
                            .getCurrentPrimaryReplica(groupId, coordinatorNode.clock().current());

                    return meta != null && !meta.getLeaseholderId().equals(firstPrimaryNodeId);
                });

        waitAndGetPrimaryReplica(coordinatorNode, groupId);

        Transaction tx = coordinatorNode.transactions().begin();
        log.info("Test: new tx: " + txId(tx));
        log.info("Test: upsert");

        Tuple newTuple = Tuple.create().set("key", tuple.longValue("key")).set("val", "v");

        // If coordinator of tx0 doesn't abort it, tx will stumble into write intent and fail with TxIdMismatchException.
        view.upsert(tx, newTuple);

        tx.commit();

        // Check that new value is written successfully.
        Tuple actual = view.get(null, keyTuple);
        assertEquals(newTuple, actual);
    }

    private boolean checkWriteIntentInStorageOnAllNodes(int partId, Tuple key) {
        return cluster.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .allMatch(n -> checkWriteIntentInStorage(n, partId, key));
    }

    private boolean checkWriteIntentInStorage(IgniteImpl node, int partId, Tuple key) {
        return runInExecutor(storageExecutor, () -> {
            HybridClock clock = node.clock();

            TableImpl table = table(node, TABLE_NAME);
            InternalTable internalTable = table.internalTable();
            TableSchemaAwareIndexStorage pkIndex = table.indexStorageAdapters(partId).get().get(table.pkId());

            MvPartitionStorage partitionStorage = internalTable.storage().getMvPartition(partId);

            var marshaller = new TupleMarshallerImpl(() -> QualifiedName.of("default", TABLE_NAME), table.schemaView().lastKnownSchema());
            Row keyRow = marshaller.marshalKey(key);
            BinaryTuple keyBinaryTuple = pkIndex.resolve(keyRow.byteBuffer());

            for (RowId rowId : pkIndex.storage().get(keyBinaryTuple)) {
                ReadResult readResult = partitionStorage.read(rowId, clock.current());

                if (readResult.isWriteIntent()) {
                    return true;
                }
            }

            return false;
        });
    }
}
