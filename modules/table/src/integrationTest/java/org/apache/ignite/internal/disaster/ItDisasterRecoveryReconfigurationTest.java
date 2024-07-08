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

package org.apache.ignite.internal.disaster;

import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Map.of;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.raftConfigurationAppliedKey;
import static org.apache.ignite.internal.replicator.configuration.ReplicationConfigurationSchema.DEFAULT_IDLE_SAFE_TIME_PROP_DURATION;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.affinity.RendezvousAffinityFunction;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationType;
import org.apache.ignite.internal.metastorage.dsl.Statement;
import org.apache.ignite.internal.metastorage.dsl.Statement.UpdateStatement;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateByNode;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for scenarios where majority of peers is not available. In this class we frequently assert partition distributions, this means that
 * tests code implicitly depends on test names, because we use method names to generate node names, and we use node names to assign
 * partitions.
 */
@Timeout(120)
public class ItDisasterRecoveryReconfigurationTest extends ClusterPerTestIntegrationTest {
    /** Scale-down timeout. */
    private static final int SCALE_DOWN_TIMEOUT_SECONDS = 2;

    /** Test table name. */
    private static final String TABLE_NAME = "TEST";

    private static final String QUALIFIED_TABLE_NAME = "PUBLIC.TEST";

    private static final int ENTRIES = 2;

    private static final int INITIAL_NODES = 1;

    /** Zone name, unique for each test. */
    private String zoneName;

    /** Zone ID that corresponds to {@link #zoneName}. */
    private int zoneId;

    /** ID of the table with name {@link #TABLE_NAME} in zone {@link #zoneId}. */
    private int tableId;

    @Override
    protected int initialNodes() {
        return INITIAL_NODES;
    }

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        Method testMethod = testInfo.getTestMethod().orElseThrow();

        IgniteImpl node0 = cluster.node(0);

        zoneName = "ZONE_" + testMethod.getName().toUpperCase();

        ZoneParams zoneParams = testMethod.getAnnotation(ZoneParams.class);

        startNodesInParallel(IntStream.range(INITIAL_NODES, zoneParams.nodes()).toArray());

        executeSql(format("CREATE ZONE %s with replicas=%d, partitions=%d,"
                        + " data_nodes_auto_adjust_scale_down=%d, data_nodes_auto_adjust_scale_up=%d, storage_profiles='%s'",
                zoneName, zoneParams.replicas(), zoneParams.partitions(), SCALE_DOWN_TIMEOUT_SECONDS, 1, DEFAULT_STORAGE_PROFILE
        ));

        CatalogZoneDescriptor zone = node0.catalogManager().zone(zoneName, node0.clock().nowLong());
        zoneId = requireNonNull(zone).id();
        waitForScale(node0, zoneParams.nodes());

        executeSql(format("CREATE TABLE %s (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE='%s'", TABLE_NAME, zoneName));

        TableManager tableManager = unwrapTableManager(node0.tables());
        tableId = ((TableViewInternal) tableManager.table(TABLE_NAME)).tableId();
    }

    /**
     * Tests the scenario in which a 5-nodes cluster loses 2 nodes, making one of its partitions unavailable for writes. In this situation
     * write should not work, because "changePeers" cannot happen and group leader will not be elected. Partition 0 in this test is always
     * assigned to nodes 0, 1 and 4, according to the {@link RendezvousAffinityFunction}.
     */
    @Test
    @ZoneParams(nodes = 5, replicas = 3, partitions = 1)
    void testInsertFailsIfMajorityIsLost() throws Exception {
        int partId = 0;

        IgniteImpl node0 = cluster.node(0);
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 1, 4);

        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        stopNodesInParallel(1, 4);

        waitForScale(node0, 3);

        assertRealAssignments(node0, partId, 0, 2, 3);

        // Set time in the future to protect us from "getAsync" from the past.
        // Should be replaced with "sleep" when clock skew validation is implemented.
        node0.clock().update(node0.clock().now().addPhysicalTime(
                SECONDS.toMillis(DEFAULT_IDLE_SAFE_TIME_PROP_DURATION) + node0.clockService().maxClockSkewMillis())
        );

        // "forEach" makes "i" effectively final, which is convenient for internal lambda.
        IntStream.range(0, ENTRIES).forEach(i -> {
            //noinspection ThrowableNotThrown
            assertThrows(
                    TimeoutException.class,
                    () -> table.keyValueView().getAsync(null, Tuple.create(of("id", i))).get(500, MILLISECONDS),
                    null
            );
        });

        errors = insertValues(table, partId, 10);

        // We expect insertion errors, because group cannot change its peers.
        assertFalse(errors.isEmpty());
    }

    /**
     * Tests that in a situation from the test {@link #testInsertFailsIfMajorityIsLost()} it is possible to recover partition using a
     * disaster recovery API. In this test, assignments will be (0, 3, 4), according to {@link RendezvousAffinityFunction}.
     */
    @Test
    @ZoneParams(nodes = 5, replicas = 3, partitions = 1)
    void testManualRebalanceIfMajorityIsLost() throws Exception {
        int partId = 0;

        IgniteImpl node0 = cluster.node(0);
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 3, 4);

        stopNodesInParallel(3, 4);

        waitForScale(node0, 3);

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetPartitions(
                zoneName,
                QUALIFIED_TABLE_NAME,
                Set.of()
        );

        assertThat(updateFuture, willSucceedIn(60, SECONDS));

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 1, 2);

        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));
    }

    /**
     * Tests that in a situation from the test {@link #testInsertFailsIfMajorityIsLost()} it is possible to recover specified partition
     * using a disaster recovery API. In this test, assignments will be (0, 2, 4) and (1, 2, 4), according to
     * {@link RendezvousAffinityFunction}.
     */
    @Test
    @ZoneParams(nodes = 5, replicas = 3, partitions = 2)
    void testManualRebalanceIfMajorityIsLostSpecifyPartitions() throws Exception {
        int fixingPartId = 1;
        int anotherPartId = 0;

        IgniteImpl node0 = cluster.node(0);
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, anotherPartId);

        assertRealAssignments(node0, fixingPartId, 0, 2, 4);
        assertRealAssignments(node0, anotherPartId, 1, 2, 4);

        stopNodesInParallel(2, 4);

        waitForScale(node0, 3);

        // Should fail because majority was lost.
        List<Throwable> fixingPartErrorsBeforeReset = insertValues(table, fixingPartId, 0);
        assertThat(fixingPartErrorsBeforeReset, not(empty()));

        List<Throwable> anotherPartErrorsBeforeReset = insertValues(table, anotherPartId, 0);
        assertThat(anotherPartErrorsBeforeReset, not(empty()));

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetPartitions(
                zoneName,
                QUALIFIED_TABLE_NAME,
                Set.of(anotherPartId)
        );

        assertThat(updateFuture, willSucceedIn(60, SECONDS));

        awaitPrimaryReplica(node0, anotherPartId);

        // Shouldn't fail because partition assignments were reset.
        List<Throwable> fixedPartErrors = insertValues(table, anotherPartId, 0);
        assertThat(fixedPartErrors, is(empty()));

        // Was not specified in reset, shouldn't be fixed. */
        List<Throwable> anotherPartErrors = insertValues(table, fixingPartId, 0);
        assertThat(anotherPartErrors, not(empty()));
    }

    /**
     * Tests a scenario where there's a single partition on a node 1, and the node that hosts it is lost. Reconfiguration of the zone should
     * create new raft group on the remaining node, without any data.
     */
    @Test
    @ZoneParams(nodes = 2, replicas = 1, partitions = 1)
    void testManualRebalanceIfPartitionIsLost() throws Exception {
        int partId = 0;

        IgniteImpl node0 = cluster.node(0);
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 1);

        stopNodesInParallel(1);

        waitForScale(node0, 1);

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetPartitions(
                zoneName,
                QUALIFIED_TABLE_NAME,
                Set.of()
        );

        assertThat(updateFuture, willCompleteSuccessfully());

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0);

        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));
    }

    /**
     * Tests a scenario where all stable nodes are lost, yet we have data on one of pending nodes and perform reset partition operation. In
     * this case we should use that pending node as a source of data for recovery.
     *
     * <p>It goes like this:
     * <ul>
     *     <li>We have 6 nodes and a partition on nodes 1, 4 and 5.</li>
     *     <li>We stop nodes 4 and 5, leaving node 1 alone in stable assignments.</li>
     *     <li>New distribution is 0, 1 and 3. Rebalance is started via raft snapshots. It transfers data to node 0, but not node 3.</li>
     *     <li>Node 1 is stopped. Data is only present on node 0.</li>
     *     <li>We execute "resetPartitions" and expect that data from node 0 will be available after that.</li>
     * </ul>
     */
    @Test
    @ZoneParams(nodes = 6, replicas = 3, partitions = 1)
    public void testIncompleteRebalanceAfterResetPartitions() throws Exception {
        int partId = 0;

        Assignments assignment013 = Assignments.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(3).name())
        );

        IgniteImpl node0 = cluster.node(0);
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);
        assertRealAssignments(node0, partId, 1, 4, 5);

        insertValues(table, partId, 0);

        triggerRaftSnapshot(1, partId);
        // Second snapshot causes log truncation.
        triggerRaftSnapshot(1, partId);

        node(1).dropMessages((nodeName, msg) -> node(3).name().equals(nodeName) && msg instanceof SnapshotMvDataResponse);

        stopNodesInParallel(4, 5);
        waitForScale(node0, 4);

        assertRealAssignments(node0, partId, 0, 1, 3);

        cluster.runningNodes().forEach(node -> {
            BiPredicate<String, NetworkMessage> newPredicate = (nodeName, msg) -> stableKeySwitchMessage(msg, partId, assignment013);
            BiPredicate<String, NetworkMessage> oldPredicate = node.dropMessagesPredicate();

            if (oldPredicate == null) {
                node.dropMessages(newPredicate);
            } else {
                node.dropMessages(oldPredicate.or(newPredicate));
            }
        });

        CompletableFuture<Void> resetFuture = node0.disasterRecoveryManager().resetPartitions(zoneName, QUALIFIED_TABLE_NAME, emptySet());
        assertThat(resetFuture, willCompleteSuccessfully());

        waitForPartitionState(node0, GlobalPartitionStateEnum.DEGRADED);

        var localStatesFut = node0.disasterRecoveryManager().localPartitionStates(emptySet(), Set.of(node(3).name()), emptySet());
        assertThat(localStatesFut, willCompleteSuccessfully());

        Map<TablePartitionId, LocalPartitionStateByNode> localStates = localStatesFut.join();
        assertThat(localStates, is(not(anEmptyMap())));
        assertEquals(LocalPartitionStateEnum.INSTALLING_SNAPSHOT, localStates.values().iterator().next().values().iterator().next().state);

        stopNode(1);
        waitForScale(node0, 3);

        waitForPartitionState(node0, GlobalPartitionStateEnum.DEGRADED);

        resetFuture = node0.disasterRecoveryManager().resetPartitions(zoneName, QUALIFIED_TABLE_NAME, emptySet());
        assertThat(resetFuture, willCompleteSuccessfully());

        waitForPartitionState(node0, GlobalPartitionStateEnum.AVAILABLE);

        awaitPrimaryReplica(node0, partId);
        assertRealAssignments(node0, partId, 0, 2, 3);

        // Set time in the future to protect us from "getAsync" from the past.
        // Should be replaced with "sleep" when clock skew validation is implemented.
        node0.clock().update(node0.clock().now().addPhysicalTime(
                SECONDS.toMillis(DEFAULT_IDLE_SAFE_TIME_PROP_DURATION) + node0.clockService().maxClockSkewMillis())
        );

        // TODO https://issues.apache.org/jira/browse/IGNITE-21303
        //  We need wait quite a bit before data is available. Log shows term mismatches, meaning that right now it only works due to some
        //  miracle. For future improvements we must specify "stable" forced sub-assignments explicitly, instead of calculating them as an
        //  intersection.
        Thread.sleep(10_000);

        // "forEach" makes "i" effectively final, which is convenient for internal lambda.
        IntStream.range(0, ENTRIES).forEach(i -> {
            CompletableFuture<Tuple> fut = table.keyValueView().getAsync(null, Tuple.create(of("id", i)));
            assertThat(fut, willCompleteSuccessfully());

            assertNotNull(fut.join());
        });
    }

    private boolean stableKeySwitchMessage(NetworkMessage msg, int partId, Assignments blockedAssignments) {
        if (msg instanceof WriteActionRequest) {
            var writeActionRequest = (WriteActionRequest) msg;
            WriteCommand command = writeActionRequest.deserializedCommand();

            if (command instanceof MultiInvokeCommand) {
                MultiInvokeCommand multiInvokeCommand = (MultiInvokeCommand) command;

                Statement andThen = multiInvokeCommand.iif().andThen();

                if (andThen instanceof UpdateStatement) {
                    UpdateStatement updateStatement = (UpdateStatement) andThen;
                    Collection<Operation> operations = updateStatement.update().operations();

                    ByteArray raftConfigurationAppliedKey = raftConfigurationAppliedKey(new TablePartitionId(tableId, partId));

                    for (Operation operation : operations) {
                        ByteArray opKey = new ByteArray(toByteArray(operation.key()));

                        if (operation.type() == OperationType.PUT && opKey.equals(raftConfigurationAppliedKey)) {
                            return blockedAssignments.equals(ByteUtils.fromBytes(toByteArray(operation.value())));
                        }
                    }
                }
            }
        }

        return false;
    }

    private void waitForPartitionState(IgniteImpl node0, GlobalPartitionStateEnum expectedState) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            CompletableFuture<Map<TablePartitionId, GlobalPartitionState>> statesFuture = node0.disasterRecoveryManager()
                    .globalPartitionStates(Set.of(zoneName), emptySet());

            assertThat(statesFuture, willCompleteSuccessfully());

            Map<TablePartitionId, GlobalPartitionState> map = statesFuture.join();

            GlobalPartitionState state = map.values().iterator().next();

            return state.state == expectedState;
        }, 500, 20_000));
    }

    private void triggerRaftSnapshot(int nodeIdx, int partId) throws InterruptedException, ExecutionException {
        //noinspection resource
        IgniteImpl node = node(nodeIdx);

        var raftNodeId = new RaftNodeId(new TablePartitionId(tableId, partId), new Peer(node.name()));
        var jraftServer = (JraftServerImpl) node.raftManager().server();

        RaftGroupService raftGroupService = jraftServer.raftGroupService(raftNodeId);
        assertNotNull(raftGroupService);

        CompletableFuture<Status> fut = new CompletableFuture<>();
        raftGroupService.getRaftNode().snapshot(fut::complete);

        assertThat(fut, willCompleteSuccessfully());
        assertEquals(RaftError.SUCCESS, fut.get().getRaftError());
    }

    private void awaitPrimaryReplica(IgniteImpl node0, int partId) {
        CompletableFuture<ReplicaMeta> awaitPrimaryReplicaFuture = node0.placementDriver()
                .awaitPrimaryReplica(new TablePartitionId(tableId, partId), node0.clock().now(), 60, SECONDS);

        assertThat(awaitPrimaryReplicaFuture, willSucceedIn(60, SECONDS));
    }

    private void assertRealAssignments(IgniteImpl node0, int partId, Integer... expected) throws InterruptedException {
        assertTrue(waitForCondition(() -> List.of(expected).equals(getRealAssignments(node0, partId)), 2000));
    }

    /**
     * Inserts {@value ENTRIES} values into a table, expecting either a success or specific set of exceptions that would indicate
     * replication issues. Collects such exceptions into a list and returns. Fails if unexpected exception happened.
     */
    private static List<Throwable> insertValues(Table table, int partitionId, int offset) {
        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        List<Throwable> errors = new ArrayList<>();

        for (int i = 0, created = 0; created < ENTRIES; i++) {
            Tuple key = Tuple.create(of("id", i));
            if ((unwrapTableImpl(table)).partition(key) != partitionId) {
                continue;
            }

            //noinspection AssignmentToForLoopParameter
            created++;

            CompletableFuture<Void> insertFuture = keyValueView.putAsync(null, key, Tuple.create(of("val", i + offset)));

            try {
                insertFuture.get(10, SECONDS);

                Tuple value = keyValueView.get(null, key);
                assertNotNull(value);
            } catch (Throwable e) {
                Throwable cause = unwrapCause(e);

                if (cause instanceof IgniteException && isPrimaryReplicaHasChangedException((IgniteException) cause)
                        || cause instanceof TransactionException
                        || cause instanceof TimeoutException
                ) {
                    errors.add(cause);
                } else {
                    fail("Unexpected exception", e);
                }
            }
        }

        return errors;
    }

    private static boolean isPrimaryReplicaHasChangedException(IgniteException cause) {
        return cause.getMessage() != null && cause.getMessage().contains("The primary replica has changed");
    }

    private void startNodesInParallel(int... nodeIndexes) {
        //noinspection resource

        runRace(20_000, IntStream.of(nodeIndexes).<RunnableX>mapToObj(i -> () -> cluster.startNode(i)).toArray(RunnableX[]::new));
    }

    /**
     * It's important to stop nodes in parallel, not only to save time, but to remove them from data nodes at the same time with a single
     * scale-down event. Otherwise tests will start failing randomly.
     */
    private void stopNodesInParallel(int... nodeIndexes) {
        runRace(IntStream.of(nodeIndexes).<RunnableX>mapToObj(i -> () -> cluster.stopNode(i)).toArray(RunnableX[]::new));
    }

    private void waitForScale(IgniteImpl node, int targetDataNodesCount) throws InterruptedException {
        DistributionZoneManager dzManager = node.distributionZoneManager();

        assertTrue(waitForCondition(() -> {
            long causalityToken = node.metaStorageManager().appliedRevision();

            long msSafeTime = node.metaStorageManager().timestampByRevision(causalityToken).longValue();
            int catalogVersion = node.catalogManager().activeCatalogVersion(msSafeTime);

            CompletableFuture<Set<String>> dataNodes = dzManager.dataNodes(causalityToken, catalogVersion, zoneId);

            try {
                return dataNodes.get(10, SECONDS).size() == targetDataNodesCount;
            } catch (Exception e) {
                return false;
            }
        }, 250, SECONDS.toMillis(60)));
    }

    private List<Integer> getRealAssignments(IgniteImpl node0, int partId) {
        CompletableFuture<Map<TablePartitionId, LocalPartitionStateByNode>> partitionStatesFut = node0.disasterRecoveryManager()
                .localPartitionStates(Set.of(zoneName), Set.of(), Set.of());
        assertThat(partitionStatesFut, willCompleteSuccessfully());

        LocalPartitionStateByNode partitionStates = partitionStatesFut.join().get(new TablePartitionId(tableId, partId));

        return partitionStates.keySet()
                .stream()
                .map(cluster::nodeIndex)
                .sorted()
                .collect(Collectors.toList());
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface ZoneParams {
        int replicas();

        int partitions();

        int nodes() default INITIAL_NODES;
    }
}
