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

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runInExecutor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.impl.ResourceVacuumManager.RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.partitionIdForTuple;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.table;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.tupleToBinaryRow;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.txId;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.waitAndGetPrimaryReplica;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.zoneId;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlySingleRowPkReplicaRequest;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.FastTimestamps;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test for write intent resolution in distributed transactions.
 */
@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "500")
public class ItTxWriteIntentResolutionTest extends ClusterPerClassIntegrationTest {
    /** Table messages factory. */
    private static final PartitionReplicationMessagesFactory TABLE_MESSAGES_FACTORY = new PartitionReplicationMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Table name. */
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String ZONE_NAME = "TEST_ZONE";

    private static final int REPLICAS = 3;

    private static final Tuple TUPLE = Tuple.create().set("key", 1L).set("val", "1");
    private static final Tuple KEY_TUPLE = Tuple.create().set("key", TUPLE.longValue("key"));

    private final ExecutorService storageExecutor = Executors.newSingleThreadExecutor(
            IgniteThreadFactory.create(
                    "test",
                    "storage-test-pool-itwirt",
                    log,
                    ThreadOperation.STORAGE_READ,
                    ThreadOperation.STORAGE_WRITE
            )
    );

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{0, 1, 2};
    }

    @Override
    protected int initialNodes() {
        return 3;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        super.configureInitParameters(builder);

        builder.clusterConfiguration("ignite {"
                + "  system.properties.txnResourceTtl: \"0\""
                + "}");
    }

    @BeforeEach
    public void setup() {
        String zoneSql = "create zone test_zone (partitions 20, replicas " + REPLICAS
                + ") storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String sql = "create table " + TABLE_NAME + " (key bigint primary key, val varchar(20)) zone TEST_ZONE";

        sql(anyNode(), zoneSql);
        sql(anyNode(), sql);
    }

    @AfterEach
    public void tearDown() {
        shutdownAndAwaitTermination(storageExecutor, 10, TimeUnit.SECONDS);
    }

    @Test
    public void test() throws InterruptedException {
        setTxResourceTtl(1);

        IgniteImpl node = anyNode();

        int partId = partitionIdForTuple(anyNode(), TABLE_NAME, TUPLE, null);

        IgniteImpl leaseholder = leaseholderForPartition(partId);

        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        IgniteImpl minorityNode = findNode(n -> !n.name().equals(node.name()) && !n.name().equals(leaseholder.name()));

        Transaction tx = node.transactions().begin();
        log.info("Test: tx id: " + txId(tx));
        view.upsert(tx, TUPLE);

        ReadResult wi = firstWriteIntent(leaseholder, partId);

        Transaction roTx = node.transactions().begin(new TransactionOptions().readOnly(true));
        //Thread.sleep(1000);

        //dropReplicationFromTo(node, minorityNode);

        tx.commit();

        Set<String> majorityNodes = Stream.of(node.name(), leaseholder.name())
                        .collect(toSet());

        waitForTxStateVacuum(majorityNodes, txId(tx), partId, true, 10_000);

        //node.stopDroppingMessages();

        //view.get(roTx, KEY_TUPLE);
        //node.sql().execute(roTx, "SELECT * FROM " + TABLE_NAME + " WHERE key = 1");

        rowToWriteIntent(minorityNode, partId, wi);

        roRequestToNode(node, minorityNode, partId, roTx);
    }

    private void roRequestToNode(IgniteImpl from, IgniteImpl to, int partId, Transaction roTx) {
        ReplicationGroupIdMessage groupIdMsg = REPLICA_MESSAGES_FACTORY.zonePartitionIdMessage()
                .zoneId(zoneId(anyNode(), TABLE_NAME))
                .partitionId(partId)
                .build();

        HybridTimestamp readTimestamp = from.clockService().current();

        BinaryRowEx keyRow = tupleToBinaryRow(from, TABLE_NAME, KEY_TUPLE, null, true);

        ReadOnlySingleRowPkReplicaRequest req = TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(groupIdMsg)
                .tableId(table(from, TABLE_NAME).tableId())
                .schemaVersion(keyRow.schemaVersion())
                .primaryKey(keyRow.tupleSlice())
                .requestType(RO_GET)
                .readTimestamp(readTimestamp)
                .transactionId(txId(roTx))
                .coordinatorId(from.id())
                .build();

        CompletableFuture<NetworkMessage> fut = from.clusterService().messagingService().invoke(to.name(), req, 10_000);
        assertThat(fut, willCompleteSuccessfully());
        NetworkMessage resp = fut.join();


    }

    private ReadResult firstWriteIntent(IgniteImpl node, int partId) {
        return callInStorageExecutor(() -> {
            MvPartitionStorage partitionStorage = partitionStorage(node, partId);

            try (Cursor<ReadResult> cursor =  partitionStorage.scan(HybridTimestamp.MAX_VALUE)) {
                while (cursor.hasNext()) {
                    ReadResult readResult = cursor.next();

                    if (readResult.isWriteIntent()) {
                        return readResult;
                    }
                }
            }

            throw new AssertionError();
        });
    }

    private void rowToWriteIntent(IgniteImpl node, int partId, ReadResult writeIntent) {
        callInStorageExecutor(() -> {
            MvPartitionStorage partitionStorage = partitionStorage(node, partId);

            partitionStorage.runConsistently(locker -> {
                locker.lock(writeIntent.rowId());

                partitionStorage.addWriteCommitted(writeIntent.rowId(), null, node.clockService().now());

                partitionStorage.addWrite(
                        writeIntent.rowId(),
                        writeIntent.binaryRow(),
                        writeIntent.transactionId(),
                        writeIntent.commitZoneId(),
                        writeIntent.commitPartitionId()
                );

                return null;
            });

            return null;
        });
    }

    private <T> T callInStorageExecutor(Supplier<T> sup) {
        CompletableFuture<T> fut = supplyAsync(sup, storageExecutor);
        assertThat(fut, willCompleteSuccessfully());
        return fut.join();
    }

    private MvPartitionStorage partitionStorage(IgniteImpl node, int partId) {
        node.partitionReplicaLifecycleManager().
        InternalTable internalTable = table(node, TABLE_NAME).internalTable();


        return internalTable.storage().getMvPartition(partId);
    }

    private IgniteImpl leaseholderForPartition(int partId) {
        var groupId = new ZonePartitionId(zoneId(anyNode(), TABLE_NAME), partId);
        ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(anyNode(), groupId);
        assertNotNull(replicaMeta);
        assertNotNull(replicaMeta.getLeaseholder());

        return findNode(n -> n.name().equals(replicaMeta.getLeaseholder()));
    }

    private static void dropReplicationFromTo(IgniteImpl from, IgniteImpl to) {
        from.dropMessages((s, msg) -> msg instanceof AppendEntriesRequest && s.equals(to.name()));
    }

    private static void sql(IgniteImpl node, String sql) {
        node.sql().execute(null, sql);
    }

    private void setTxResourceTtl(long ttl) {
        SystemDistributedConfiguration system = anyNode().clusterConfiguration()
                .getConfiguration(SystemDistributedExtensionConfiguration.KEY).system();

        CompletableFuture<Void> changeFut = system.change(
                c -> c.changeProperties(c0 -> c0.update("txnResourceTtl", c1 -> c1.changePropertyValue(ttl + ""))));

        assertThat(changeFut, willCompleteSuccessfully());
    }



    /**
     * Waits for vacuum of volatile (and if needed, persistent) state of the given tx on the given nodes.
     *
     * @param nodeConsistentIds Node names.
     * @param txId Transaction id.
     * @param partId Commit partition id to check the persistent tx state storage of this partition.
     * @param checkPersistent Whether to wait for vacuum of persistent tx state as well.
     * @param timeMs Time to wait.
     */
    private void waitForTxStateVacuum(Set<String> nodeConsistentIds, UUID txId, int partId, boolean checkPersistent, long timeMs)
            throws InterruptedException {
        boolean r = waitForCondition(() -> txStateIsAbsent(nodeConsistentIds, txId, TABLE_NAME, partId, checkPersistent, false), timeMs);

        if (!r) {
            logCurrentTxState(nodeConsistentIds, txId, TABLE_NAME, partId);
        }

        assertTrue(r);
    }

    /**
     * Checks whether the tx state is absent on all of the given nodes.
     *
     * @param nodeConsistentIds Set of node names to check.
     * @param txId Transaction id.
     * @param tableName Table name of the table that commit partition belongs to.
     * @param partId Commit partition id.
     * @param checkPersistent Whether the persistent state should be checked.
     * @param checkCpPrimaryOnly If {@code} true, the persistent state should be checked only on the commit partition primary,
     *     otherwise it would be checked on every given node.
     * @return {@code true} if tx state is absent, {@code false} otherwise. Call {@link #logCurrentTxState(Set, UUID, String, int)}
     *     for details.
     */
    private boolean txStateIsAbsent(
            Set<String> nodeConsistentIds,
            UUID txId,
            String tableName,
            int partId,
            boolean checkPersistent,
            boolean checkCpPrimaryOnly
    ) {
        boolean result = true;

        UUID cpPrimaryId = null;

        if (checkCpPrimaryOnly) {
            IgniteImpl node = anyNode();

            ZonePartitionId partitionGroupId = new ZonePartitionId(zoneId(node, tableName), partId);

            CompletableFuture<ReplicaMeta> replicaFut = node.placementDriver().getPrimaryReplica(partitionGroupId, node.clock().now());
            assertThat(replicaFut, willCompleteSuccessfully());

            ReplicaMeta replicaMeta = replicaFut.join();
            // The test doesn't make sense if there is no primary right now.
            assertNotNull(replicaMeta);

            cpPrimaryId = replicaMeta.getLeaseholderId();
        }

        for (Iterator<IgniteImpl> iterator = CLUSTER.runningNodes().map(TestWrappers::unwrapIgniteImpl).iterator(); iterator.hasNext();) {
            IgniteImpl node = iterator.next();

            if (!nodeConsistentIds.contains(node.name())) {
                continue;
            }

            result = result
                    && volatileTxState(node, txId) == null
                    && (!checkPersistent || !node.id().equals(cpPrimaryId) || persistentTxState(node, txId, tableName, partId) == null);
        }

        return result;
    }

    private void logCurrentTxState(Set<String> nodeConsistentIds, UUID txId, String table, int partId) {
        CLUSTER.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .filter(n -> nodeConsistentIds.contains(n.name()))
                .forEach(node -> {
                    log.info("Test: volatile   state [tx={}, node={}, state={}].", txId, node.name(), volatileTxState(node, txId));
                    log.info(
                            "Test: persistent state [tx={}, node={}, state={}].",
                            txId, node.name(), persistentTxState(node, txId, table, partId)
                    );
                });
    }

    @Nullable
    private static TransactionMeta volatileTxState(IgniteImpl node, UUID txId) {
        TxManagerImpl txManager = (TxManagerImpl) node.txManager();

        TxStateMeta txInMemoryState = txManager.stateMeta(txId);

        if (txInMemoryState == null) {
            return null;
        }

        if (TxState.isFinalState(txInMemoryState.txState())) {
            long current = FastTimestamps.coarseCurrentTimeMillis();

            Long initialTs = txInMemoryState.initialVacuumObservationTimestamp();

            assertNotNull(initialTs);

            assertTrue(current >= initialTs);
        }

        return txInMemoryState;
    }

    @Nullable
    private TransactionMeta persistentTxState(IgniteImpl node, UUID txId, String tableName, int partId) {
        return runInExecutor(storageExecutor, () -> {
            InternalTable internalTable = table(node, tableName).internalTable();

            TxStatePartitionStorage txStatePartitionStorage = node
                    .partitionReplicaLifecycleManager()
                    .txStatePartitionStorage(internalTable.zoneId(), partId);

            assertNotNull(txStatePartitionStorage);

            return txStatePartitionStorage.get(txId);
        });
    }
}
