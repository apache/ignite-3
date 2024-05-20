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

package org.apache.ignite.internal.placementdriver;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.wrapper.Wrappers.unwrapNullable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.impl.schema.TestProfileConfigurationSchema;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.NodeUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.testframework.flow.TestFlowUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * TODO: IGNITE-20485 Configure the lease interval as less as possible to decrease the duration of tests.
 * The test class checks invariant of a primary replica choice.
 */
@SuppressWarnings("resource")
@WithSystemProperty(key = "IGNITE_ALWAYS_FORCE", value = "false")
public class ItPrimaryReplicaChoiceTest extends ClusterPerTestIntegrationTest {
    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 10;

    private static final int PART_ID = 0;

    private static final String ZONE_NAME = "ZONE_TABLE";

    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String HASH_IDX = "HASH_IDX";

    private static final String SORTED_IDX = "SORTED_IDX";

    @BeforeEach
    @Override
    public void setup(TestInfo testInfo) throws Exception {
        super.setup(testInfo);

        String zoneSql = IgniteStringFormatter.format(
                "CREATE ZONE IF NOT EXISTS {} WITH REPLICAS={}, PARTITIONS={}, STORAGE_PROFILES='{}'",
                ZONE_NAME, 3, 1, TestProfileConfigurationSchema.TEST_PROFILE_NAME
        );

        String sql = IgniteStringFormatter.format(
                "CREATE TABLE {} (key INT PRIMARY KEY, val VARCHAR(20)) WITH PRIMARY_ZONE='{}'",
                TABLE_NAME, ZONE_NAME
        );

        String hashIdxSql = IgniteStringFormatter.format("CREATE INDEX {} ON {} USING HASH (val)",
                HASH_IDX, TABLE_NAME);

        String sortedIdxSql = IgniteStringFormatter.format("CREATE INDEX {} ON {} USING SORTED (val)",
                SORTED_IDX, TABLE_NAME);

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
            executeUpdate(hashIdxSql, session);
            executeUpdate(sortedIdxSql, session);
        });
    }

    @Test
    public void testPrimaryChangeSubscription() throws Exception {
        TableViewInternal tbl = unwrapTableViewInternal(node(0).tables().table(TABLE_NAME));

        var tblReplicationGrp = new ZonePartitionId(tbl.internalTable().zoneId(), tbl.tableId(), PART_ID);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplicaForTable(
                tblReplicationGrp,
                node(0).clock().now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String primary = primaryReplicaFut.get().getLeaseholder();

        IgniteImpl ignite = node(primary);

        AtomicBoolean primaryChanged = new AtomicBoolean();

        ignite.placementDriver().listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, evt -> {
            primaryChanged.set(true);

            return falseCompletedFuture();
        });

        NodeUtils.transferPrimary(cluster.runningNodes().collect(toSet()), tblReplicationGrp);

        assertTrue(waitForCondition(primaryChanged::get, 10_000));
    }

    @Test
    public void testPrimaryChangeLongHandling() throws Exception {
        TableViewInternal tbl = unwrapTableImpl(node(0).tables().table(TABLE_NAME));

        var tblReplicationGrp = new ZonePartitionId(tbl.internalTable().zoneId(), tbl.tableId(), PART_ID);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplicaForTable(
                tblReplicationGrp,
                node(0).clock().now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String primary = primaryReplicaFut.get().getLeaseholder();

        IgniteImpl ignite = node(primary);

        CompletableFuture<Boolean> primaryChangedHandling = new CompletableFuture<>();

        ignite.placementDriver().listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, evt -> primaryChangedHandling);

        log.info("Primary replica is: " + primary);

        Collection<IgniteImpl> nodes = cluster.runningNodes().collect(toSet());

        NodeUtils.transferPrimary(nodes, tblReplicationGrp);

        CompletableFuture<String> primaryChangeTask =
                IgniteTestUtils.runAsync(() -> NodeUtils.transferPrimary(nodes, tblReplicationGrp, primary));

        waitingForLeaderCache(tbl, primary);

        assertFalse(primaryChangeTask.isDone());

        primaryChangedHandling.complete(false);

        assertThat(primaryChangeTask, willCompleteSuccessfully());

        assertEquals(primary, primaryChangeTask.get());
    }

    /**
     * The test checks cleanup two types of resources: locks (which are used to support transaction logic) and cursors (which are used to
     * hold a context of scan operations on the server side).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClearingTransactionResourcesWhenPrimaryChange() throws Exception {
        Table publicTableOnNode0 = node(0).tables().table(TABLE_NAME);
        TableViewInternal unwrappedTableOnNode0 = unwrapTableViewInternal(publicTableOnNode0);

        for (int i = 0; i < 10; i++) {
            assertTrue(publicTableOnNode0.recordView().insert(null, Tuple.create().set("key", i).set("val", "preload val")));
        }

        var tblReplicationGrp = new ZonePartitionId(unwrappedTableOnNode0.internalTable().zoneId(),
                unwrappedTableOnNode0.tableId(), PART_ID);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplicaForTable(
                tblReplicationGrp,
                node(0).clock().now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String primary = primaryReplicaFut.get().getLeaseholder();

        IgniteImpl primaryIgnite = node(primary);

        int hashIdxId = getIndexId(HASH_IDX);
        int sortedIdxId = getIndexId(SORTED_IDX);

        InternalTransaction rwTx = (InternalTransaction) node(0).transactions().begin();
        InternalTransaction roTx = (InternalTransaction) node(0).transactions().begin(new TransactionOptions().readOnly(true));

        assertTrue(publicTableOnNode0.recordView().insert(rwTx, Tuple.create().set("key", 42).set("val", "val 42")));

        BinaryTuple idxKey = new BinaryTuple(1, new BinaryTupleBuilder(1).appendString("preload val").build());

        // Start cursors in the RW transaction.
        scanSingleEntryAndLeaveCursorOpen(unwrappedTableOnNode0, rwTx, null, null);
        scanSingleEntryAndLeaveCursorOpen(unwrappedTableOnNode0, rwTx, hashIdxId, idxKey);
        scanSingleEntryAndLeaveCursorOpen(unwrappedTableOnNode0, rwTx, sortedIdxId, null);

        // Start cursors in the RO transaction.
        scanSingleEntryAndLeaveCursorOpen(unwrappedTableOnNode0, roTx, null, null);
        scanSingleEntryAndLeaveCursorOpen(unwrappedTableOnNode0, roTx, hashIdxId, idxKey);
        scanSingleEntryAndLeaveCursorOpen(unwrappedTableOnNode0, roTx, sortedIdxId, null);

        TableViewInternal unwrappedTableFromPrimary = unwrapTableViewInternal(primaryIgnite.tables().table(TABLE_NAME));
        TestMvPartitionStorage partitionStorage = unwrapNullable(
                unwrappedTableFromPrimary.internalTable().storage().getMvPartition(PART_ID),
                TestMvPartitionStorage.class
        );

        TestHashIndexStorage hashIdxStorage = unwrapNullable(
                unwrappedTableFromPrimary.internalTable().storage().getIndex(PART_ID, hashIdxId),
                TestHashIndexStorage.class
        );

        TestSortedIndexStorage sortedIdxStorage = unwrapNullable(
                unwrappedTableFromPrimary.internalTable().storage().getIndex(PART_ID, sortedIdxId),
                TestSortedIndexStorage.class
        );

        assertTrue(primaryIgnite.txManager().lockManager().locks(rwTx.id()).hasNext());
        assertEquals(6, partitionStorage.pendingCursors() + hashIdxStorage.pendingCursors() + sortedIdxStorage.pendingCursors());

        NodeUtils.transferPrimary(cluster.runningNodes().collect(toSet()), tblReplicationGrp);

        assertTrue(primaryIgnite.txManager().lockManager().locks(rwTx.id()).hasNext());
        assertEquals(6, partitionStorage.pendingCursors() + hashIdxStorage.pendingCursors() + sortedIdxStorage.pendingCursors());

        rwTx.rollback();

        assertFalse(primaryIgnite.txManager().lockManager().locks(rwTx.id()).hasNext());
        assertEquals(3, partitionStorage.pendingCursors() + hashIdxStorage.pendingCursors() + sortedIdxStorage.pendingCursors());
    }

    /**
     * Starts a scan procedure for a specific transaction and reads only the first line from the cursor.
     *
     * @param unwrappedTable Scanned table.
     * @param tx Transaction.
     * @param idxId Index id.
     */
    private void scanSingleEntryAndLeaveCursorOpen(
            TableViewInternal unwrappedTable,
            InternalTransaction tx,
            Integer idxId,
            BinaryTuple exactKey
    ) {
        bypassingThreadAssertions(() -> {
            try {
                scanSingleEntryAndLeaveCursorOpen0(unwrappedTable, tx, idxId, exactKey);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void scanSingleEntryAndLeaveCursorOpen0(
            TableViewInternal unwrappedTable,
            InternalTransaction tx,
            Integer idxId,
            BinaryTuple exactKey
    ) throws Exception {
        InternalTable internalTable = unwrappedTable.internalTable();

        Publisher<BinaryRow> publisher;

        if (tx.isReadOnly()) {
            CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().getPrimaryReplicaForTable(
                    new ZonePartitionId(unwrappedTable.internalTable().zoneId(), unwrappedTable.tableId(), PART_ID),
                    node(0).clock().now()
            );

            assertThat(primaryReplicaFut, willCompleteSuccessfully());

            assertNotNull(primaryReplicaFut.join());

            String primaryId = primaryReplicaFut.get().getLeaseholderId();

            ClusterNode primaryNode = node(0).clusterNodes().stream().filter(node -> node.id().equals(primaryId)).findAny().get();

            if (idxId == null) {
                publisher = internalTable.scan(PART_ID, tx.id(), tx.readTimestamp(), primaryNode, tx.coordinatorId());
            } else if (exactKey == null) {
                publisher = internalTable.scan(PART_ID, tx.id(), tx.readTimestamp(), primaryNode, idxId, null, null, 0, null,
                        tx.coordinatorId());
            } else {
                publisher = internalTable.lookup(PART_ID, tx.id(), tx.readTimestamp(), primaryNode, idxId, exactKey, null,
                        tx.coordinatorId());
            }
        } else if (idxId == null) {
            publisher = unwrappedTable.internalTable().scan(PART_ID, tx);
        } else if (exactKey == null) {
            publisher = unwrappedTable.internalTable().scan(PART_ID, tx, idxId, null, null, 0, null);
        } else {
            ReadWriteTransactionImpl rwTx = Wrappers.unwrap(tx, ReadWriteTransactionImpl.class);

            CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().getPrimaryReplica(
                    new TablePartitionId(unwrappedTable.tableId(), PART_ID),
                    node(0).clock().now()
            );

            assertThat(primaryReplicaFut, willCompleteSuccessfully());

            assertNotNull(primaryReplicaFut.join());

            String primaryId = primaryReplicaFut.get().getLeaseholderId();

            ClusterNode primaryNode = node(0).clusterNodes().stream().filter(node -> node.id().equals(primaryId)).findAny().get();

            publisher = unwrappedTable.internalTable().lookup(
                    PART_ID,
                    rwTx.id(),
                    rwTx.zoneCommitPartition(),
                    rwTx.coordinatorId(),
                    new PrimaryReplica(primaryNode, primaryReplicaFut.get().getStartTime().longValue()),
                    idxId,
                    exactKey,
                    null
            );
        }

        ArrayList<BinaryRow> scannedRows = new ArrayList<>();
        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = TestFlowUtils.subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(1);

        assertTrue(waitForCondition(() -> scannedRows.size() == 1, 10_000));

        assertFalse(scanned.isDone());
    }

    /**
     * Gets index id by name.
     *
     * @param idxName Index name.
     * @return Index id.
     */
    private int getIndexId(String idxName) {
        CatalogManager catalogManager = node(0).catalogManager();

        int catalogVersion = catalogManager.latestCatalogVersion();

        return catalogManager.indexes(catalogVersion).stream()
                .filter(index -> {
                    log.info("Scanned idx " + index.name());

                    return idxName.equalsIgnoreCase(index.name());
                })
                .mapToInt(CatalogObjectDescriptor::id)
                .findFirst()
                .getAsInt();
    }

    /**
     * Waits when the leader would be a different with the current primary replica.
     *
     * @param tbl Table.
     * @param primary Current primary replica name.
     * @throws InterruptedException If fail.
     */
    private static void waitingForLeaderCache(TableViewInternal tbl, String primary) throws InterruptedException {
        RaftGroupService raftSrvc = tbl.internalTable().tableRaftService().partitionRaftGroupService(0);

        assertTrue(waitForCondition(() -> {
            raftSrvc.refreshLeader();

            Peer leader = raftSrvc.leader();

            return leader != null;
        }, 10_000));
    }

    /**
     * Gets Ignite instance by the name.
     *
     * @param name Node name.
     * @return Ignite instance.
     */
    private @Nullable IgniteImpl node(String name) {
        for (int i = 0; i < initialNodes(); i++) {
            if (node(i).name().equals(name)) {
                return node(i);
            }
        }

        return null;
    }
}
