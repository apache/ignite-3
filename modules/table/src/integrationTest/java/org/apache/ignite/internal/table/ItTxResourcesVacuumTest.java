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
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.impl.ResourceVacuumManager.RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.findTupleToBeHostedOnNode;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.partitionAssignment;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.partitionIdForTuple;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.table;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.tableId;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.txId;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.waitAndGetPrimaryReplica;
import static org.apache.ignite.internal.table.NodeUtils.transferPrimary;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.internal.tx.TransactionMeta;
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
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} },\n"
            + "  rest.port: {},\n"
            + "  raft: { responseTimeout: 30000 },"
            + "  compute.threadPoolSize: 1\n"
            + "}";

    private ExecutorService txStateStorageExecutor = Executors.newSingleThreadExecutor();

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

        txStateStorageExecutor = Executors.newSingleThreadExecutor(IgniteThreadFactory.create("test", "tx-state-storage-test-pool", log,
                ThreadOperation.STORAGE_READ));
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
                + "      txnResourceTtl: 1"
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
     * Simple vacuum test, checking also that PENDING and FINISHING states are not removed.
     *
     * <ul>
     *     <li>Run a transaction;</li>
     *     <li>Insert a value;</li>
     *     <li>Wait 3 seconds;</li>
     *     <li>Check that the volatile PENDING state of the transaction is preserved;</li>
     *     <li>Block {@link TxFinishReplicaRequest};</li>
     *     <li>Start the tx commit;</li>
     *     <li>While the state is FINISHING, wait 3 seconds;</li>
     *     <li>Check that the volatile state of the transaction is preserved;</li>
     *     <li>Unblock {@link TxFinishReplicaRequest};</li>
     *     <li>Check that both volatile and persistent state is vacuumized;</li>
     *     <li>Check that the committed value is correct.</li>
     * </ul>
     */
    @Test
    public void testVacuum() throws InterruptedException {
        IgniteImpl node = anyNode();

        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx = node.transactions().begin();
        UUID txId = txId(tx);

        log.info("Test: Loading the data [tx={}].", txId);

        Tuple tuple = findTupleToBeHostedOnNode(node, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        int partId = partitionIdForTuple(node, TABLE_NAME, tuple, tx);

        Set<String> nodes = partitionAssignment(node, new TablePartitionId(tableId(node, TABLE_NAME), partId));

        view.upsert(tx, tuple);

        // Check that the volatile PENDING state of the transaction is preserved.
        Thread.sleep(3000);
        assertTrue(checkVolatileTxStateOnNodes(nodes, txId));

        CompletableFuture<Void> finishStartedFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishAllowedFuture = new CompletableFuture<>();

        node.dropMessages((n, msg) -> {
            if (msg instanceof TxFinishReplicaRequest) {
                finishStartedFuture.complete(null);

                finishAllowedFuture.join();
            }

            return false;
        });

        CompletableFuture<Void> commitFut = runAsync(tx::commit);

        assertThat(finishStartedFuture, willCompleteSuccessfully());

        // While the state is FINISHING, wait 3 seconds.
        assertEquals(FINISHING, volatileTxState(node, txId).txState());
        Thread.sleep(3000);

        // Check that the volatile state of the transaction is preserved.
        assertTrue(checkVolatileTxStateOnNodes(nodes, txId));

        finishAllowedFuture.complete(null);

        assertThat(commitFut, willCompleteSuccessfully());

        log.info("Test: Tx committed [tx={}].", txId);

        waitForTxStateReplication(nodes, txId, partId, 10_000);

        // Check that both volatile and persistent state is vacuumized..
        waitForTxStateVacuum(txId, partId, true, 10_000);

        // Trying to read the value.
        Tuple data = view.get(null, Tuple.create().set("key", tuple.longValue("key")));
        assertEquals(tuple, data);
    }

    /**
     * Check that the ABANDONED transaction state is preserved until recovery.
     *
     * <ul>
     *     <li>Start a transaction from a coordinator that would be not included into commit partition group;</li>
     *     <li>Insert a value;</li>
     *     <li>Stop the tx coordinator;</li>
     *     <li>Wait 3 seconds;</li>
     *     <li>Check that the volatile state of the transaction is preserved;</li>
     *     <li>Try to read the value using another transaction, which starts the tx recovery;</li>
     *     <li>Check that abandoned tx is rolled back and thus the value is null;</li>
     *     <li>Check that the abandoned transaction is recovered; its volatile and persistent states are vacuumized.</li>
     * </ul>
     */
    @Test
    public void testAbandonedTxnsAreNotVacuumizedUntilRecovered() throws InterruptedException {
        IgniteImpl leaseholder = cluster.node(0);

        Tuple tuple = findTupleToBeHostedOnNode(leaseholder, TABLE_NAME, null, INITIAL_TUPLE, NEXT_TUPLE, true);

        int partId = partitionIdForTuple(anyNode(), TABLE_NAME, tuple, null);

        TablePartitionId groupId = new TablePartitionId(tableId(anyNode(), TABLE_NAME), partId);

        Set<String> txNodes = partitionAssignment(anyNode(), groupId);

        IgniteImpl abandonedTxCoord = findNode(n -> !txNodes.contains(n.name()));

        RecordView<Tuple> view = abandonedTxCoord.tables().table(TABLE_NAME).recordView();

        Transaction abandonedTx = abandonedTxCoord.transactions().begin();
        UUID abandonedTxId = txId(abandonedTx);
        view.upsert(abandonedTx, tuple);

        stopNode(abandonedTxCoord.name());

        Thread.sleep(3000);

        // Check that the volatile state of the transaction is preserved.
        assertTrue(checkVolatileTxStateOnNodes(txNodes, abandonedTxId));

        // Try to read the value using another transaction, which starts the tx recovery.
        RecordView<Tuple> viewLh = leaseholder.tables().table(TABLE_NAME).recordView();
        Tuple value = viewLh.get(null, Tuple.create().set("key", tuple.longValue("key")));
        // Check that abandoned tx is rolled back and thus the value is null.
        assertNull(value);

        // Check that the abandoned transaction is recovered; its volatile and persistent states are vacuumized.
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

        log.info("Test: Commit partition [leaseholder={}, hostingNodes={}].", commitPartitionLeaseholder.name(), commitPartNodes);

        // Some node that does not host the commit partition, will be the primary node for upserting another tuple.
        IgniteImpl leaseholderForAnotherTuple = findNode(n -> !commitPartNodes.contains(n.name()));

        log.info("Test: leaseholderForAnotherTuple={}", leaseholderForAnotherTuple.name());

        Tuple tuple1 = findTupleToBeHostedOnNode(leaseholderForAnotherTuple, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        // Upsert both tuples within a transaction.
        view.upsert(tx, tuple0);
        view.upsert(tx, tuple1);

        CompletableFuture<Void> cleanupStarted = new CompletableFuture<>();
        CompletableFuture<Void> cleanupAllowed = new CompletableFuture<>();

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage) {
                cleanupStarted.complete(null);

                log.info("Test: cleanup started.");

                if (commitPartNodes.contains(n)) {
                    cleanupAllowed.join();
                }
            }

            return false;
        });

        CompletableFuture<Void> commitFut = tx.commitAsync();

        waitForTxStateReplication(commitPartNodes, txId, commitPartId, 10_000);

        assertThat(cleanupStarted, willCompleteSuccessfully());

        // Wait for vacuum completion on a node that doesn't host the commit partition.
        waitForTxStateVacuum(Set.of(leaseholderForAnotherTuple.name()), txId, 0, false, 10_000);

        // Unblocking cleanup.
        cleanupAllowed.complete(null);

        assertThat(commitFut, willCompleteSuccessfully());

        // Wait for the cleanup on the commit partition group.
        waitForTxStateVacuum(txId, commitPartId, true, 10_000);

        // Trying to read the values.
        Tuple data0 = view.get(null, Tuple.create().set("key", tuple0.longValue("key")));
        assertEquals(tuple0, data0);

        Tuple data1 = view.get(null, Tuple.create().set("key", tuple1.longValue("key")));
        assertEquals(tuple1, data1);
    }

    /**
     * Check that the tx state on commit partition is vacuumized only when cleanup is completed.
     *
     * <ul>
     *     <li>Start a transaction;</li>
     *     <li>Upsert a value;</li>
     *     <li>Block {@link TxCleanupMessage}-s;</></li>
     *     <li>Start a tx commit;</li>
     *     <li>Transfer the primary replica;</li>
     *     <li>Unblock the {@link TxCleanupMessage}-s;</li>
     *     <li>Ensure that tx states are finally vacuumized.</li>
     * </ul>
     */
    @Test
    public void testCommitPartitionPrimaryChangesBeforeVacuum() throws InterruptedException {
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
        CompletableFuture<Void> cleanupAllowedFut = new CompletableFuture<>();
        boolean[] cleanupAllowed = new boolean[1];

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage && !cleanupAllowed[0]) {
                cleanupStarted.complete(null);

                cleanupAllowedFut.join();
            }

            return false;
        });

        CompletableFuture<Void> commitFut = tx.commitAsync();

        assertThat(cleanupStarted, willCompleteSuccessfully());

        transferPrimary(cluster.runningNodes().collect(toSet()), commitPartGrpId, commitPartNodes::contains);

        cleanupAllowedFut.complete(null);

        cleanupAllowed[0] = true;

        assertThat(commitFut, willCompleteSuccessfully());

        log.info("Test: tx committed.");

        waitForTxStateVacuum(txId, commitPartId, true, 10_000);

        // Trying to read the value.
        Tuple data = view.get(null, Tuple.create().set("key", tuple.longValue("key")));
        assertEquals(tuple, data);
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

        log.info("Test: Commit partition [leaseholder={}, hostingNodes={}].", commitPartitionLeaseholder.name(), commitPartNodes);

        view.upsert(tx, tuple);

        CompletableFuture<Void> cleanupStarted = new CompletableFuture<>();
        CompletableFuture<Void> cleanupAllowedFut = new CompletableFuture<>();
        boolean[] cleanupAllowed = new boolean[1];

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage && !cleanupAllowed[0]) {
                cleanupStarted.complete(null);

                cleanupAllowedFut.join();

                return true;
            }

            return false;
        });

        CompletableFuture<Void> commitFut = tx.commitAsync();

        assertThat(cleanupStarted, willCompleteSuccessfully());

        // Wait for volatile tx state vacuum. This is possible because tx finish is complete.
        waitForTxStateVacuum(txId, commitPartId, false, 10_000);

        log.info("Test: volatile state vacuumized");

        cleanupAllowedFut.complete(null);

        cleanupAllowed[0] = true;

        assertThat(commitFut, willCompleteSuccessfully());

        waitForTxStateVacuum(txId, commitPartId, true, 10_000);

        // Trying to read the data.
        Tuple data = view.get(null, Tuple.create().set("key", tuple.longValue("key")));
        assertEquals(tuple, data);
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
        IgniteImpl commitPartitionLeaseholder = cluster.node(0);

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
        boolean[] cleanupAllowed = new boolean[1];

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage) {
                cleanupStarted.complete(null);

                return cleanupAllowed[0];
            }

            return false;
        });

        log.info("Test: Committing the transaction 0 [tx={}].", txId0);

        tx0.commitAsync();

        assertThat(cleanupStarted, willCompleteSuccessfully());

        // Check that the final tx state COMMITTED is saved to the persistent tx storage.
        assertTrue(waitForCondition(() -> cluster.runningNodes().filter(n -> commitPartitionNodes.contains(n.name())).allMatch(n -> {
            TransactionMeta meta = persistentTxState(n, txId0, commitPartId);

            return meta != null && meta.txState() == COMMITTED;
        }), 10_000));

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

        waitForTxStateVacuum(txId0, commitPartId, true, 10_000);
        waitForTxStateVacuum(txId0, commitPartId, true, 10_000);
    }

    /**
     * Check that RO txns read the correct data consistent with commit timestamps.
     *
     * <ul>
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
    public void testRoReadTheCorrectDataInBetween() throws InterruptedException {
        IgniteImpl node = anyNode();

        Transaction roTx1 = node.transactions().begin(new TransactionOptions().readOnly(true));

        Tuple t1 = Tuple.create().set("key", 1L).set("val", "val1");
        Tuple t2 = Tuple.create().set("key", 1L).set("val", "val2");

        RecordView<Tuple> view = table(node, TABLE_NAME).recordView();

        Transaction rwTx1 = node.transactions().begin();
        view.upsert(rwTx1, t1);
        rwTx1.commit();
        UUID rwTxId1 = txId(rwTx1);

        Transaction roTx2 = node.transactions().begin(new TransactionOptions().readOnly(true));

        Transaction rwTx2 = node.transactions().begin();
        view.upsert(rwTx2, t2);
        rwTx2.commit();
        UUID rwTxId2 = txId(rwTx1);

        Transaction roTx3 = node.transactions().begin(new TransactionOptions().readOnly(true));

        waitForTxStateVacuum(rwTxId1, partitionIdForTuple(node, TABLE_NAME, t1, rwTx1), true, 10_000);
        waitForTxStateVacuum(rwTxId2, partitionIdForTuple(node, TABLE_NAME, t2, rwTx2), true, 10_000);

        Tuple keyRec = Tuple.create().set("key", 1L);

        Tuple r1 = view.get(roTx1, keyRec);
        assertNull(r1);

        Tuple r2 = view.get(roTx2, keyRec);
        assertEquals(t1.stringValue("val"), r2.stringValue("val"));

        Tuple r3 = view.get(roTx3, keyRec);
        assertEquals(t2.stringValue("val"), r3.stringValue("val"));
    }

    private boolean checkVolatileTxStateOnNodes(Set<String> nodeConsistentIds, UUID txId) {
        return cluster.runningNodes()
                .filter(n -> nodeConsistentIds.contains(n.name()))
                .allMatch(n -> volatileTxState(n, txId) != null);
    }

    private boolean checkPersistentTxStateOnNodes(Set<String> nodeConsistentIds, UUID txId, int partId) {
        return cluster.runningNodes()
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
        waitForTxStateVacuum(cluster.runningNodes().map(IgniteImpl::name).collect(toSet()), txId, partId, checkPersistent, timeMs);
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
        boolean r = waitForCondition(() -> {
            boolean result = true;

            for (Iterator<IgniteImpl> iterator = cluster.runningNodes().iterator(); iterator.hasNext();) {
                IgniteImpl node = iterator.next();

                if (!nodeConsistentIds.contains(node.name())) {
                    continue;
                }

                result = result
                        && volatileTxState(node, txId) == null && (!checkPersistent || persistentTxState(node, txId, partId) == null);
            }

            return result;
        }, timeMs);

        if (!r) {
            cluster.runningNodes().forEach(node -> {
                log.info("Test: volatile   state [tx={}, node={}, state={}].", txId, node.name(), volatileTxState(node, txId));
                log.info("Test: persistent state [tx={}, node={}, state={}].", txId, node.name(), persistentTxState(node, txId, partId));
            });
        }

        assertTrue(r);
    }

    private IgniteImpl anyNode() {
        return runningNodes().findFirst().orElseThrow();
    }

    @Nullable
    private static TransactionMeta volatileTxState(IgniteImpl node, UUID txId) {
        TxManagerImpl txManager = (TxManagerImpl) node.txManager();
        return txManager.stateMeta(txId);
    }

    @Nullable
    private TransactionMeta persistentTxState(IgniteImpl node, UUID txId, int partId) {
        TransactionMeta[] meta = new TransactionMeta[1];

        Future f = txStateStorageExecutor.submit(() -> {
            TxStateStorage txStateStorage = table(node, TABLE_NAME).internalTable().txStateStorage().getTxStateStorage(partId);

            assertNotNull(txStateStorage);

            meta[0] = txStateStorage.get(txId);
        });

        try {
            f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        return meta[0];
    }

    private IgniteImpl findNode(Predicate<IgniteImpl> filter) {
        return cluster.runningNodes()
                .filter(n -> n != null && filter.test(n))
                .findFirst()
                .get();
    }
}
