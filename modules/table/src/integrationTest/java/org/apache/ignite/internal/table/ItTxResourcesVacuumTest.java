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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.table.NodeUtils.transferPrimary;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runInExecutor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.impl.ResourceVacuumManager.RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.findTupleToBeHostedOnNode;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.partitionAssignment;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.partitionIdForTuple;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.table;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.tableId;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.txId;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.waitAndGetPrimaryReplica;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxCleanupMessage;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for tx recources vacuum.
 */
@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "1000")
public class ItTxResourcesVacuumTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    private static final Tuple INITIAL_TUPLE = Tuple.create().set("key", 1L).set("val", "1");

    private static final Function<Tuple, Tuple> NEXT_TUPLE = t -> Tuple.create()
            .set("key", t.longValue("key") + 1)
            .set("val", "" + (t.longValue("key") + 1));

    private static final int REPLICAS = 2;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network {\n"
            + "    port: {}\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  }\n"
            + "  clientConnector.port: {}\n"
            + "  rest.port: {}\n"
            + "  raft.responseTimeout: 30000\n"
            + "  compute.threadPoolSize: 1\n"
            + "}";

    private final ExecutorService txStateStorageExecutor = Executors.newSingleThreadExecutor(
            IgniteThreadFactory.create("test", "tx-state-storage-test-pool-itrvt", log, ThreadOperation.STORAGE_READ)
    );

    @BeforeEach
    @Override
    public void setup(TestInfo testInfo) throws Exception {
        super.setup(testInfo);

        String zoneSql = "create zone test_zone with partitions=20, replicas=" + REPLICAS
                + ", storage_profiles='" + DEFAULT_STORAGE_PROFILE + "'";
        String sql = "create table " + TABLE_NAME + " (key bigint primary key, val varchar(20)) with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    @Override
    @AfterEach
    public void tearDown() {
        shutdownAndAwaitTermination(txStateStorageExecutor, 10, TimeUnit.SECONDS);

        super.tearDown();
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        super.customizeInitParameters(builder);

        builder.clusterConfiguration("{"
                + "  transaction: {"
                + "      txnResourceTtl: 0"
                + "  },"
                + "  replication: {"
                + "      rpcTimeout: 30000"
                + "  },"
                + "}");
    }

    /**
     * Returns node bootstrap config template.
     *
     * @return Node bootstrap config template.
     */
    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    /**
     * Simple TTL-triggered vacuum test, checking also that PENDING and FINISHING states are not removed.
     *
     * <ul>
     *     <li>Run a transaction;</li>
     *     <li>Run a parallel transaction;</li>
     *     <li>Insert values within both transactions;</li>
     *     <li>Commit the parallel transaction and wait for vacuum of its state;</li>
     *     <li>Run another parallel transaction;</li>
     *     <li>Check that the volatile PENDING state of the transaction is preserved;</li>
     *     <li>Block {@link TxFinishReplicaRequest} for the pending transaction;</li>
     *     <li>Start the tx commit;</li>
     *     <li>While the state is FINISHING, commit the parallel transaction and wait for vacuum of its state;</li>
     *     <li>Check that the volatile state of the transaction is preserved;</li>
     *     <li>Unblock {@link TxFinishReplicaRequest};</li>
     *     <li>Check that both volatile and persistent state is vacuumized;</li>
     *     <li>Check that the committed value is correct.</li>
     * </ul>
     */
    @Test
    public void testVacuum() throws InterruptedException {
        // We should test the TTL-triggered vacuum.
        setTxResourceTtl(1);

        IgniteImpl node = anyNode();

        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx = node.transactions().begin();
        Transaction parallelTx1 = node.transactions().begin();
        UUID txId = txId(tx);
        UUID parallelTx1Id = txId(parallelTx1);

        log.info("Test: Loading the data [tx={}].", txId);

        Tuple tuple = findTupleToBeHostedOnNode(node, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);
        Tuple tupleForParallelTx = findTupleToBeHostedOnNode(node, TABLE_NAME, tx, NEXT_TUPLE.apply(tuple), NEXT_TUPLE, true);
        int partIdForParallelTx = partitionIdForTuple(anyNode(), TABLE_NAME, tupleForParallelTx, parallelTx1);

        int partId = partitionIdForTuple(node, TABLE_NAME, tuple, tx);

        Set<String> nodes = partitionAssignment(node, new TablePartitionId(tableId(node, TABLE_NAME), partId));

        view.upsert(tx, tuple);
        view.upsert(parallelTx1, tupleForParallelTx);

        // Check that the volatile PENDING state of the transaction is preserved.
        parallelTx1.commit();
        waitForTxStateVacuum(nodes, parallelTx1Id, partIdForParallelTx, true, 10_000);
        assertTrue(checkVolatileTxStateOnNodes(nodes, txId));

        Transaction parallelTx2 = node.transactions().begin();
        UUID parallelTx2Id = txId(parallelTx2);
        view.upsert(parallelTx2, tupleForParallelTx);

        CompletableFuture<Void> finishStartedFuture = new CompletableFuture<>();

        node.dropMessages((n, msg) -> {
            if (msg instanceof TxFinishReplicaRequest) {
                TxFinishReplicaRequest finishRequest = (TxFinishReplicaRequest) msg;

                if (finishRequest.txId().equals(txId)) {
                    finishStartedFuture.complete(null);

                    log.info("Test: dropping finish on [node= {}].", n);

                    return true;
                }
            }

            return false;
        });

        Transaction roTxBefore = beginReadOnlyTx(anyNode());

        CompletableFuture<Void> commitFut = runAsync(tx::commit);

        assertThat(finishStartedFuture, willCompleteSuccessfully());

        // While the state is FINISHING, wait 3 seconds.
        assertEquals(FINISHING, volatileTxState(node, txId).txState());
        parallelTx2.commit();
        waitForTxStateVacuum(nodes, parallelTx2Id, partId, true, 10_000);

        // Check that the volatile state of the transaction is preserved.
        assertTrue(checkVolatileTxStateOnNodes(nodes, txId));

        node.stopDroppingMessages();

        assertThat(commitFut, willCompleteSuccessfully());

        log.info("Test: Tx committed [tx={}].", txId);

        Transaction roTxAfter = beginReadOnlyTx(anyNode());

        waitForTxStateReplication(nodes, txId, partId, 10_000);

        // Check that both volatile and persistent state is vacuumized..
        waitForTxStateVacuum(txId, partId, true, 10_000);

        // Trying to read the value.
        Tuple keyRec = Tuple.create().set("key", tuple.longValue("key"));
        checkValueReadOnly(view, roTxBefore, keyRec, null);
        checkValueReadOnly(view, roTxAfter, keyRec, tuple);
    }

    /**
     * Check that the ABANDONED transaction state is preserved until recovery.
     *
     * <ul>
     *     <li>Start a transaction from a coordinator that would be not included into commit partition group;</li>
     *     <li>Start a parallel transaction;</li>
     *     <li>Find a tuple for parallel tx that would be hosted on the same partition as a tuple for the abandoned tx;</li>
     *     <li>Insert values within both transactions;</li>
     *     <li>Commit the parallel transaction;</li>
     *     <li>Stop the tx coordinator;</li>
     *     <li>Wait for tx state of parallel tx to be vacuumized;</li>
     *     <li>Check that the volatile state of the transaction is preserved;</li>
     *     <li>Try to read the value using another transaction, which starts the tx recovery;</li>
     *     <li>Check that abandoned tx is rolled back and thus the value is null;</li>
     *     <li>Check that the abandoned transaction is recovered; its volatile and persistent states are vacuumized.</li>
     * </ul>
     */
    @Test
    public void testAbandonedTxnsAreNotVacuumizedUntilRecovered() throws InterruptedException {
        setTxResourceTtl(1);

        IgniteImpl leaseholder = unwrapIgniteImpl(cluster.node(0));

        Tuple tuple = findTupleToBeHostedOnNode(leaseholder, TABLE_NAME, null, INITIAL_TUPLE, NEXT_TUPLE, true);

        int partId = partitionIdForTuple(anyNode(), TABLE_NAME, tuple, null);

        TablePartitionId groupId = new TablePartitionId(tableId(anyNode(), TABLE_NAME), partId);

        Set<String> txNodes = partitionAssignment(anyNode(), groupId);

        IgniteImpl abandonedTxCoord = findNode(n -> !txNodes.contains(n.name()));

        RecordView<Tuple> view = abandonedTxCoord.tables().table(TABLE_NAME).recordView();

        Transaction abandonedTx = abandonedTxCoord.transactions().begin();
        UUID abandonedTxId = txId(abandonedTx);
        Transaction parallelTx = abandonedTxCoord.transactions().begin();
        UUID parallelTxId = txId(parallelTx);

        // Find a tuple hosted on the same partition.
        Tuple tupleForParallelTx = tuple;
        int partIdForParallelTx = -1;
        while (partIdForParallelTx != partId) {
            tupleForParallelTx = findTupleToBeHostedOnNode(leaseholder, TABLE_NAME, null, NEXT_TUPLE.apply(tupleForParallelTx), NEXT_TUPLE,
                    true);

            partIdForParallelTx = partitionIdForTuple(anyNode(), TABLE_NAME, tupleForParallelTx, parallelTx);
        }

        view.upsert(abandonedTx, tuple);
        view.upsert(parallelTx, tupleForParallelTx);

        parallelTx.commit();

        stopNode(abandonedTxCoord.name());

        waitForTxStateVacuum(txNodes, parallelTxId, partIdForParallelTx, true, 10_000);

        // Check that the volatile state of the transaction is preserved.
        assertTrue(checkVolatileTxStateOnNodes(txNodes, abandonedTxId));

        // Try to read the value using another transaction, which starts the tx recovery.
        RecordView<Tuple> viewLh = leaseholder.tables().table(TABLE_NAME).recordView();
        Tuple value = viewLh.get(null, Tuple.create().set("key", tuple.longValue("key")));
        // Check that abandoned tx is rolled back and thus the value is null.
        assertNull(value);

        // Check that the abandoned transaction is recovered; its volatile and persistent states are vacuumized.
        // Wait for it, because we don't have the recovery completion future.
        waitForTxStateVacuum(txNodes, abandonedTxId, partId, true, 10_000);
    }

    /**
     * Check that the tx state on commit partition is vacuumized only when cleanup is completed.
     *
     * <ul>
     *     <li>Start a transaction;</li>
     *     <li>Generate some tuple and define on which nodes it would be hosted;</li>
     *     <li>Choose one more node that doesn't host the first tuple and choose a tuple that will be sent on this node as primary;</li>
     *     <li>Upsert both tuples within a transaction;</li>
     *     <li>Block {@link TxCleanupMessage}-s from commit partition primary;</li>
     *     <li>Start a tx commit;</li>
     *     <li>Wait for vacuum completion on a node that doesn't host the commit partition;</li>
     *     <li>Unblock {@link TxCleanupMessage}-s;</li>
     *     <li>Wait for the tx state vacuum on the commit partition group.</li>
     * </ul>
     */
    @Test
    @WithSystemProperty(key = RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "0")
    public void testVacuumWithCleanupDelay() throws InterruptedException {
        IgniteImpl node = anyNode();

        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx = node.transactions().begin();
        UUID txId = txId(tx);

        log.info("Test: Loading the data [tx={}].", txId);

        // Generate some tuple and define on which nodes it would be hosted.
        Tuple tuple0 = findTupleToBeHostedOnNode(node, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        int commitPartId = partitionIdForTuple(node, TABLE_NAME, tuple0, tx);

        TablePartitionId commitPartGrpId = new TablePartitionId(tableId(node, TABLE_NAME), commitPartId);

        ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(node, commitPartGrpId);
        IgniteImpl commitPartitionLeaseholder = findNode(n -> n.id().equals(replicaMeta.getLeaseholderId()));

        Set<String> commitPartNodes = partitionAssignment(node, new TablePartitionId(tableId(node, TABLE_NAME), commitPartId));

        log.info("Test: Commit partition [part={}, leaseholder={}, hostingNodes={}].", commitPartGrpId, commitPartitionLeaseholder.name(),
                commitPartNodes);

        // Some node that does not host the commit partition, will be the primary node for upserting another tuple.
        IgniteImpl leaseholderForAnotherTuple = findNode(n -> !commitPartNodes.contains(n.name()));

        log.info("Test: leaseholderForAnotherTuple={}", leaseholderForAnotherTuple.name());

        Tuple tuple1 = findTupleToBeHostedOnNode(leaseholderForAnotherTuple, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        // Upsert both tuples within a transaction.
        view.upsert(tx, tuple0);
        view.upsert(tx, tuple1);

        CompletableFuture<Void> cleanupStarted = new CompletableFuture<>();

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage) {
                log.info("Test: cleanup started on [node= {}].", n);

                if (commitPartNodes.contains(n)) {
                    cleanupStarted.complete(null);

                    log.info("Test: dropping cleanup on [node= {}].", n);

                    return true;
                }
            }

            return false;
        });

        Transaction roTxBefore = beginReadOnlyTx(anyNode());

        CompletableFuture<Void> commitFut = tx.commitAsync();

        waitForTxStateReplication(commitPartNodes, txId, commitPartId, 10_000);

        assertThat(cleanupStarted, willCompleteSuccessfully());

        // Check the vacuum result on a node that doesn't host the commit partition.
        triggerVacuum();
        assertTxStateVacuumized(Set.of(leaseholderForAnotherTuple.name()), txId, commitPartId, false);

        // Unblocking cleanup.
        commitPartitionLeaseholder.stopDroppingMessages();

        assertThat(commitFut, willCompleteSuccessfully());

        Transaction roTxAfter = beginReadOnlyTx(anyNode());

        waitForCleanupCompletion(commitPartNodes, txId);

        triggerVacuum();
        assertTxStateVacuumized(txId, commitPartId, true);

        // Trying to read the values.
        Tuple key0 = Tuple.create().set("key", tuple0.longValue("key"));
        Tuple key1 = Tuple.create().set("key", tuple1.longValue("key"));
        checkValueReadOnly(view, roTxBefore, key0, null);
        checkValueReadOnly(view, roTxAfter, key0, tuple0);
        checkValueReadOnly(view, roTxBefore, key1, null);
        checkValueReadOnly(view, roTxAfter, key1, tuple1);
    }

    /**
     * Check that the tx state on commit partition is vacuumized only when cleanup is completed.
     *
     * <ul>
     *     <li>Start a transaction;</li>
     *     <li>Upsert a value;</li>
     *     <li>Block {@link TxCleanupMessage}-s;</li>
     *     <li>Start a tx commit;</li>
     *     <li>Transfer the primary replica;</li>
     *     <li>Unblock the {@link TxCleanupMessage}-s;</li>
     *     <li>Ensure that tx states are finally vacuumized.</li>
     * </ul>
     */
    @Test
    public void testCommitPartitionPrimaryChangesBeforeVacuum() throws InterruptedException {
        // We can't leave TTL as 0 here, because the primary replica is changed during cleanup, and this means
        // WriteIntentSwitchReplicaRequest will be processed not on the primary. Removing tx state instantly will cause incorrect
        // tx recovery and write intent switch with tx state as ABORTED.
        setTxResourceTtl(1);

        IgniteImpl node = anyNode();

        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx = node.transactions().begin();
        UUID txId = txId(tx);

        log.info("Test: Loading the data [tx={}].", txId);

        Tuple tuple = findTupleToBeHostedOnNode(node, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        int commitPartId = partitionIdForTuple(node, TABLE_NAME, tuple, tx);

        TablePartitionId commitPartGrpId = new TablePartitionId(tableId(node, TABLE_NAME), commitPartId);

        ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(node, commitPartGrpId);
        IgniteImpl commitPartitionLeaseholder = findNode(n -> n.id().equals(replicaMeta.getLeaseholderId()));

        Set<String> commitPartNodes = partitionAssignment(node, new TablePartitionId(tableId(node, TABLE_NAME), commitPartId));

        log.info("Test: Commit partition [leaseholder={}, hostingNodes={}].", commitPartitionLeaseholder.name(), commitPartNodes);

        view.upsert(tx, tuple);

        CompletableFuture<Void> cleanupStarted = new CompletableFuture<>();
        boolean[] cleanupAllowed = new boolean[1];

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage) {
                log.info("Test: perform cleanup on [node= {}, msg={}].", n, msg);

                cleanupStarted.complete(null);

                if (!cleanupAllowed[0]) {
                    log.info("Test: dropping cleanup on [node= {}].", n);

                    return true;
                }
            }

            return false;
        });

        Transaction roTxBefore = beginReadOnlyTx(anyNode());

        CompletableFuture<Void> commitFut = tx.commitAsync();

        assertThat(cleanupStarted, willCompleteSuccessfully());

        transferPrimary(
                cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(toSet()),
                commitPartGrpId,
                commitPartNodes::contains
        );

        cleanupAllowed[0] = true;

        assertThat(commitFut, willCompleteSuccessfully());

        log.info("Test: tx committed.");

        waitForTxStateVacuum(txId, commitPartId, true, 10_000);

        log.info("Test: checking values.");

        Transaction roTxAfter = beginReadOnlyTx(anyNode());

        // Trying to read the value.
        Tuple key = Tuple.create().set("key", tuple.longValue("key"));
        checkValueReadOnly(view, roTxBefore, key, null);
        checkValueReadOnly(view, roTxAfter, key, tuple);
    }

    /**
     * Check that the tx state on commit partition is vacuumized only when cleanup is completed.
     *
     * <ul>
     *     <li>Start a transaction;</li>
     *     <li>Upsert a tuple;</li>
     *     <li>Block {@link TxCleanupMessage}-s from commit partition primary;</li>
     *     <li>Start a tx commit;</li>
     *     <li>Wait for tx cleanup to start;</li>
     *     <li>Wait for volatile tx state vacuum;</li>
     *     <li>Unblock {@link TxCleanupMessage}-s;</li>
     *     <li>Wait for the tx state vacuum on the commit partition group.</li>
     * </ul>
     */
    @Test
    @WithSystemProperty(key = RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "0")
    public void testVacuumPersistentStateAfterCleanupDelayAndVolatileStateVacuum() throws InterruptedException {
        IgniteImpl node = anyNode();

        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx = node.transactions().begin();
        UUID txId = txId(tx);

        log.info("Test: Loading the data [tx={}].", txId);

        Tuple tuple = findTupleToBeHostedOnNode(node, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        int commitPartId = partitionIdForTuple(node, TABLE_NAME, tuple, tx);

        TablePartitionId commitPartGrpId = new TablePartitionId(tableId(node, TABLE_NAME), commitPartId);

        ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(node, commitPartGrpId);
        IgniteImpl commitPartitionLeaseholder = findNode(n -> n.id().equals(replicaMeta.getLeaseholderId()));

        Set<String> commitPartNodes = partitionAssignment(node, new TablePartitionId(tableId(node, TABLE_NAME), commitPartId));

        log.info("Test: Commit partition [part={}, leaseholder={}, hostingNodes={}].", commitPartGrpId, commitPartitionLeaseholder.name(),
                commitPartNodes);

        view.upsert(tx, tuple);

        CompletableFuture<Void> cleanupStarted = new CompletableFuture<>();
        boolean[] cleanupAllowed = new boolean[1];

        // Cleanup may be triggered by the primary replica reelection as well.
        runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .filter(n -> commitPartNodes.contains(n.name()))
                .forEach(nd -> nd.dropMessages((n, msg) -> {
                    if (msg instanceof TxCleanupMessage) {
                        log.info("Test: perform cleanup on [node={}, msg={}].", n, msg);

                        cleanupStarted.complete(null);

                        if (!cleanupAllowed[0]) {
                            log.info("Test: dropping cleanup on [node={}].", n);

                            return true;
                        }
                    }

                    return false;
                }));

        Transaction roTxBefore = beginReadOnlyTx(anyNode());

        CompletableFuture<Void> commitFut = tx.commitAsync();

        waitForTxStateReplication(commitPartNodes, txId, commitPartId, 10_000);

        log.info("Test: state replicated.");

        assertThat(cleanupStarted, willCompleteSuccessfully());

        // Wait for volatile tx state vacuum. This is possible because tx finish is complete.
        triggerVacuum();
        assertTxStateVacuumized(txId, commitPartId, false);

        log.info("Test: volatile state vacuumized");

        cleanupAllowed[0] = true;

        assertThat(commitFut, willCompleteSuccessfully());

        log.info("Test: commit completed.");

        Transaction roTxAfter = beginReadOnlyTx(anyNode());

        waitForCleanupCompletion(commitPartNodes, txId);

        log.info("Test: cleanup completed.");

        triggerVacuum();
        assertTxStateVacuumized(txId, commitPartId, true);

        // Trying to read the data.
        Tuple key = Tuple.create().set("key", tuple.longValue("key"));
        checkValueReadOnly(view, roTxBefore, key, null);
        checkValueReadOnly(view, roTxAfter, key, tuple);
    }

    /**
     * Checks that the tx recovery doesn't change tx finish result from COMMITTED to ABORTED if it once saved in the persistent storage.
     *
     * <ul>
     *     <li>Start a transaction tx0;</li>
     *     <li>Upsert some value;</li>
     *     <li>Block {@link TxCleanupMessage}-s;</li>
     *     <li>Start the commit of tx0 and with for tx state COMMITTED to be replicated in persistent storage;</li>
     *     <li>Stop the tx0's coordinator;</li>
     *     <li>Wait for tx0's state vacuum;</li>
     *     <li>Try to get the data that has been committed by tx0, ensure the data is correct.</li>
     * </ul>
     */
    @Test
    public void testRecoveryAfterPersistentStateVacuumized() throws InterruptedException {
        // This node isn't going to be stopped, so let it be node 0.
        IgniteImpl commitPartitionLeaseholder = unwrapIgniteImpl(cluster.node(0));

        Tuple tuple0 = findTupleToBeHostedOnNode(commitPartitionLeaseholder, TABLE_NAME, null, INITIAL_TUPLE, NEXT_TUPLE, true);

        int commitPartId = partitionIdForTuple(commitPartitionLeaseholder, TABLE_NAME, tuple0, null);

        Set<String> commitPartitionNodes = partitionAssignment(commitPartitionLeaseholder,
                new TablePartitionId(tableId(commitPartitionLeaseholder, TABLE_NAME), commitPartId));

        // Choose some node that doesn't host the partition as a tx coordinator.
        IgniteImpl coord0 = findNode(n -> !commitPartitionNodes.contains(n.name()));

        RecordView<Tuple> view0 = coord0.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx0 = coord0.transactions().begin();
        UUID txId0 = txId(tx0);

        log.info("Test: Transaction 0 [tx={}].", txId0);

        log.info("Test: Commit partition of transaction 0 [leaseholder={}, hostingNodes={}].", commitPartitionLeaseholder.name(),
                commitPartitionNodes);

        view0.upsert(tx0, tuple0);

        CompletableFuture<Void> cleanupStarted = new CompletableFuture<>();

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage) {
                cleanupStarted.complete(null);

                return false;
            }

            return false;
        });

        log.info("Test: Committing the transaction 0 [tx={}].", txId0);

        tx0.commitAsync();

        // Cleanup starts not earlier than the finish command is applied to commit partition group.
        assertThat(cleanupStarted, willCompleteSuccessfully());

        // Stop the first transaction coordinator.
        stopNode(coord0.name());

        // No cleanup happened, waiting for vacuum on the remaining nodes that participated on tx0.
        waitForTxStateVacuum(txId0, commitPartId, true, 10_000);

        // Preparing to run another tx.
        IgniteImpl coord1 = anyNode();

        RecordView<Tuple> view1 = coord1.tables().table(TABLE_NAME).recordView();

        // Another tx should get the data committed by tx 0.
        Tuple keyTuple = Tuple.create().set("key", tuple0.longValue("key"));
        Tuple tx0Data = view1.get(null, keyTuple);
        assertEquals(tuple0.longValue("key"), tx0Data.longValue("key"));
        assertEquals(tuple0.stringValue("val"), tx0Data.stringValue("val"));

        // Waiting for vacuum, because there is no recovery future here.
        waitForTxStateVacuum(txId0, commitPartId, true, 10_000);
    }

    /**
     * Check that RO txns read the correct data consistent with commit timestamps.
     *
     * <ul>
     *     <li>For this test, create another zone and table with number of replicas that is equal to number of nodes;</li>
     *     <li>Start RO tx 1;</li>
     *     <li>Upsert (k1, v1) within RW tx 1 and commit it;</li>
     *     <li>Start RO tx 2;</li>
     *     <li>Upsert (k1, v2) within RW tx 2 and commit it;</li>
     *     <li>Start RO tx 3;</li>
     *     <li>Wait for vacuum of the states of RW tx 1 and RW tx 2;</li>
     *     <li>Read the data by k1 within RO tx 1, should be null;</li>
     *     <li>Read the data by k1 within RO tx 2, should be v1;</li>
     *     <li>Read the data by k1 within RO tx 3, should be v2.</li>
     * </ul>
     */
    @Test
    public void testRoReadTheCorrectDataInBetween() {
        IgniteImpl node = anyNode();

        String tableName = TABLE_NAME + "_1";

        // For this test, create another zone and table with number of replicas that is equal to number of nodes.
        String zoneSql = "create zone test_zone_1 with partitions=20, replicas=" + initialNodes()
                + ", storage_profiles='" + DEFAULT_STORAGE_PROFILE + "'";
        String sql = "create table " + tableName + " (key bigint primary key, val varchar(20)) with primary_zone='TEST_ZONE_1'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });

        Transaction roTx1 = beginReadOnlyTx(node);

        Tuple t1 = Tuple.create().set("key", 1L).set("val", "val1");
        Tuple t2 = Tuple.create().set("key", 1L).set("val", "val2");

        RecordView<Tuple> view = table(node, tableName).recordView();

        Transaction rwTx1 = node.transactions().begin();
        view.upsert(rwTx1, t1);
        rwTx1.commit();
        UUID rwTxId1 = txId(rwTx1);

        Transaction roTx2 = beginReadOnlyTx(node);

        Transaction rwTx2 = node.transactions().begin();
        view.upsert(rwTx2, t2);
        rwTx2.commit();
        UUID rwTxId2 = txId(rwTx1);

        Transaction roTx3 = beginReadOnlyTx(node);

        triggerVacuum();

        assertTxStateVacuumized(rwTxId1, tableName, partitionIdForTuple(node, tableName, t1, rwTx1), true);
        assertTxStateVacuumized(rwTxId2, tableName, partitionIdForTuple(node, tableName, t2, rwTx2), true);

        Tuple keyRec = Tuple.create().set("key", 1L);

        checkValueReadOnly(view, roTx1, keyRec, null);
        checkValueReadOnly(view, roTx2, keyRec, t1);
        checkValueReadOnly(view, roTx3, keyRec, t2);
    }

    private static Transaction beginReadOnlyTx(IgniteImpl node) {
        return node.transactions().begin(new TransactionOptions().readOnly(true));
    }

    /**
     * Check value using given read only tx.
     *
     * @param view Record view.
     * @param readOnlyTx RO tx.
     * @param keyTuple Key tuple.
     * @param expected Expected tuple.
     */
    private static void checkValueReadOnly(RecordView<Tuple> view, Transaction readOnlyTx, Tuple keyTuple, @Nullable Tuple expected) {
        Tuple actual = view.get(readOnlyTx, keyTuple);

        if (expected == null) {
            assertNull(actual);
        } else {
            assertNotNull(actual);
            assertEquals(expected.stringValue("val"), actual.stringValue("val"));
        }
    }

    private void setTxResourceTtl(long ttl) {
        CompletableFuture<Void> changeFuture = anyNode().clusterConfiguration().change(c ->
                c.changeRoot(TransactionConfiguration.KEY).changeTxnResourceTtl(ttl));

        assertThat(changeFuture, willCompleteSuccessfully());
    }

    /**
     * To use it, set tx resource TTL should be set to {@code 0}, see {@link #setTxResourceTtl(long)}.
     */
    private void triggerVacuum() {
        runningNodes().forEach(node -> {
            log.info("Test: triggering vacuum manually on node: " + node.name());

            CompletableFuture<Void> vacuumFut = unwrapIgniteImpl(node).txManager().vacuum();
            assertThat(vacuumFut, willCompleteSuccessfully());
        });
    }

    private boolean checkVolatileTxStateOnNodes(Set<String> nodeConsistentIds, UUID txId) {
        return cluster.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .filter(n -> nodeConsistentIds.contains(n.name()))
                .allMatch(n -> volatileTxState(n, txId) != null);
    }

    private boolean checkPersistentTxStateOnNodes(Set<String> nodeConsistentIds, UUID txId, int partId) {
        return cluster.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .filter(n -> nodeConsistentIds.contains(n.name()))
                .allMatch(n -> persistentTxState(n, txId, partId) != null);
    }

    /**
     * Waits for persistent tx state to be replicated on the given nodes.
     *
     * @param nodeConsistentIds Node names.
     * @param txId Transaction id.
     * @param partId Commit partition id.
     * @param timeMs Time to wait.
     */
    private void waitForTxStateReplication(Set<String> nodeConsistentIds, UUID txId, int partId, long timeMs)
            throws InterruptedException {
        assertTrue(waitForCondition(() -> checkPersistentTxStateOnNodes(nodeConsistentIds, txId, partId), timeMs));
    }

    /**
     * Waits for vacuum of volatile (and if needed, persistent) state of the given tx on all nodes of the cluster.
     *
     * @param txId Transaction id.
     * @param partId Commit partition id to check the persistent tx state storage of this partition.
     * @param checkPersistent Whether to wait for vacuum of persistent tx state as well.
     * @param timeMs Time to wait.
     */
    private void waitForTxStateVacuum(UUID txId, int partId, boolean checkPersistent, long timeMs) throws InterruptedException {
        waitForTxStateVacuum(cluster.runningNodes().map(Ignite::name).collect(toSet()), txId, partId, checkPersistent, timeMs);
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
     * Wait for cleanup completion timestamp on any node of commit partition group.
     *
     * @param commitPartitionNodeNames Node names of nodes in commit partition group.
     * @param txId Transaction id.
     */
    private void waitForCleanupCompletion(Set<String> commitPartitionNodeNames, UUID txId) throws InterruptedException {
        Set<IgniteImpl> commitPartitionNodes = runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .filter(n -> commitPartitionNodeNames.contains(n.name()))
                .collect(toSet());

        assertTrue(waitForCondition(() -> {
            boolean res = false;

            for (IgniteImpl node : commitPartitionNodes) {
                TxStateMeta txStateMeta = (TxStateMeta) volatileTxState(node, txId);

                res = res || txStateMeta != null && txStateMeta.cleanupCompletionTimestamp() != null;
            }

            return res;
        }, 10_000));
    }

    /**
     * Assert that volatile (and if needed, persistent) state of the given tx is vacuumized on all nodes of the cluster.
     *
     * @param txId Transaction id.
     * @param partId Commit partition id to check the persistent tx state storage of this partition.
     * @param checkPersistent Whether to wait for vacuum of persistent tx state as well.
     */
    private void assertTxStateVacuumized(UUID txId, int partId, boolean checkPersistent) {
        assertTxStateVacuumized(txId, TABLE_NAME, partId, checkPersistent);
    }

    /**
     * Assert that volatile (and if needed, persistent) state of the given tx is vacuumized on all nodes of the cluster.
     *
     * @param txId Transaction id.
     * @param tableName Table name of the table that commit partition belongs to.
     * @param partId Commit partition id to check the persistent tx state storage of this partition.
     * @param checkPersistent Whether to wait for vacuum of persistent tx state as well.
     */
    private void assertTxStateVacuumized(UUID txId, String tableName, int partId, boolean checkPersistent) {
        Set<String> allNodes = cluster.runningNodes().map(Ignite::name).collect(toSet());

        assertTxStateVacuumized(allNodes, txId, tableName, partId, checkPersistent);
    }

    /**
     * Assert that volatile (and if needed, persistent) state of the given tx is vacuumized on the given nodes. Uses default
     * {@link #TABLE_NAME}.
     *
     * @param nodeConsistentIds Node names.
     * @param txId Transaction id.
     * @param partId Commit partition id to check the persistent tx state storage of this partition.
     * @param checkPersistent Whether to wait for vacuum of persistent tx state as well.
     */
    private void assertTxStateVacuumized(Set<String> nodeConsistentIds, UUID txId, int partId, boolean checkPersistent) {
        assertTxStateVacuumized(nodeConsistentIds, txId, TABLE_NAME, partId, checkPersistent);
    }

    /**
     * Assert that volatile (and if needed, persistent) state of the given tx is vacuumized on the given nodes.
     *
     * @param nodeConsistentIds Node names.
     * @param txId Transaction id.
     * @param tableName Table name of the table that commit partition belongs to.
     * @param partId Commit partition id to check the persistent tx state storage of this partition.
     * @param checkPersistent Whether to wait for vacuum of persistent tx state as well.
     */
    private void assertTxStateVacuumized(Set<String> nodeConsistentIds, UUID txId, String tableName, int partId, boolean checkPersistent) {
        boolean result = txStateIsAbsent(nodeConsistentIds, txId, tableName, partId, checkPersistent, true);

        if (!result) {
            triggerVacuum();

            result = txStateIsAbsent(nodeConsistentIds, txId, tableName, partId, checkPersistent, true);

            if (!result) {
                logCurrentTxState(nodeConsistentIds, txId, tableName, partId);
            }
        }

        assertTrue(result);
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

        String cpPrimaryId = null;

        if (checkCpPrimaryOnly) {
            IgniteImpl node = anyNode();

            TablePartitionId tablePartitionId = new TablePartitionId(tableId(node, tableName), partId);

            CompletableFuture<ReplicaMeta> replicaFut = node.placementDriver().getPrimaryReplica(tablePartitionId, node.clock().now());
            assertThat(replicaFut, willCompleteSuccessfully());

            ReplicaMeta replicaMeta = replicaFut.join();
            // The test doesn't make sense if there is no primary right now.
            assertNotNull(replicaMeta);

            cpPrimaryId = replicaMeta.getLeaseholderId();
        }

        for (Iterator<IgniteImpl> iterator = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).iterator(); iterator.hasNext();) {
            IgniteImpl node = iterator.next();

            if (!nodeConsistentIds.contains(node.name())) {
                continue;
            }

            result = result
                    && volatileTxState(node, txId) == null
                    && (!checkPersistent || !node.id().equals(cpPrimaryId) || persistentTxState(node, txId, partId) == null);
        }

        return result;
    }

    private void logCurrentTxState(Set<String> nodeConsistentIds, UUID txId, String table, int partId) {
        cluster.runningNodes()
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

    private IgniteImpl anyNode() {
        return runningNodes().map(TestWrappers::unwrapIgniteImpl).findFirst().orElseThrow();
    }

    @Nullable
    private static TransactionMeta volatileTxState(IgniteImpl node, UUID txId) {
        TxManagerImpl txManager = (TxManagerImpl) node.txManager();
        return txManager.stateMeta(txId);
    }

    @Nullable
    private TransactionMeta persistentTxState(IgniteImpl node, UUID txId, int partId) {
        return persistentTxState(node, txId, TABLE_NAME, partId);
    }

    @Nullable
    private TransactionMeta persistentTxState(IgniteImpl node, UUID txId, String tableName, int partId) {
        return runInExecutor(txStateStorageExecutor, () -> {
            TxStateStorage txStateStorage = table(node, tableName).internalTable().txStateStorage().getTxStateStorage(partId);

            assertNotNull(txStateStorage);

            return txStateStorage.get(txId);
        });
    }

    private IgniteImpl findNode(Predicate<IgniteImpl> filter) {
        return cluster.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .filter(n -> n != null && filter.test(n))
                .findFirst()
                .get();
    }

}
