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
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationType;
import org.apache.ignite.internal.metastorage.dsl.Statement;
import org.apache.ignite.internal.metastorage.dsl.Statement.UpdateStatement;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.RendezvousDistributionFunction;
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
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
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

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));

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

        executeSql(format("CREATE TABLE %s (id INT PRIMARY KEY, val INT) ZONE %s", TABLE_NAME, zoneName));

        TableManager tableManager = unwrapTableManager(node0.tables());
        tableId = ((TableViewInternal) tableManager.table(TABLE_NAME)).tableId();
    }

    /**
     * Tests the scenario in which a 5-nodes cluster loses 2 nodes, making one of its partitions unavailable for writes. In this situation
     * write should not work, because "changePeers" cannot happen and group leader will not be elected. Partition 0 in this test is always
     * assigned to nodes 0, 1 and 4, according to the {@link RendezvousDistributionFunction}.
     */
    @Test
    @ZoneParams(nodes = 5, replicas = 3, partitions = 1)
    void testInsertFailsIfMajorityIsLost() throws Exception {
        int partId = 0;

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
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
     * disaster recovery API. In this test, assignments will be (0, 3, 4), according to {@link RendezvousDistributionFunction}.
     */
    @Test
    @ZoneParams(nodes = 5, replicas = 3, partitions = 1)
    void testManualRebalanceIfMajorityIsLost() throws Exception {
        int partId = 0;

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 3, 4);

        stopNodesInParallel(3, 4);

        waitForScale(node0, 3);

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetAllPartitions(
                zoneName,
                QUALIFIED_TABLE_NAME,
                true
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
     * {@link RendezvousDistributionFunction}.
     */
    @Test
    @ZoneParams(nodes = 5, replicas = 3, partitions = 2)
    void testManualRebalanceIfMajorityIsLostSpecifyPartitions() throws Exception {
        int fixingPartId = 1;
        int anotherPartId = 0;

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
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
                Set.of(anotherPartId),
                true
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

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 1);

        stopNodesInParallel(1);

        waitForScale(node0, 1);

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetAllPartitions(
                zoneName,
                QUALIFIED_TABLE_NAME,
                true
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

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));

        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();

        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);
        assertRealAssignments(node0, partId, 1, 4, 5);

        insertValues(table, partId, 0);

        triggerRaftSnapshot(1, partId);
        // Second snapshot causes log truncation.
        triggerRaftSnapshot(1, partId);

        unwrapIgniteImpl(node(1)).dropMessages((nodeName, msg) -> {
            Ignite node = nullableNode(3);

            return node != null && node.name().equals(nodeName) && msg instanceof SnapshotMvDataResponse;
        });

        stopNodesInParallel(4, 5);
        waitForScale(node0, 4);

        assertRealAssignments(node0, partId, 0, 1, 3);

        Assignments assignment013 = Assignments.of(timestamp,
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(3).name())
        );

        blockRebalanceStableSwitch(partId, assignment013);

        CompletableFuture<Void> resetFuture = node0.disasterRecoveryManager().resetAllPartitions(zoneName, QUALIFIED_TABLE_NAME, true);
        assertThat(resetFuture, willCompleteSuccessfully());

        waitForPartitionState(node0, partId, GlobalPartitionStateEnum.DEGRADED);

        var localStatesFut = node0.disasterRecoveryManager().localPartitionStates(emptySet(), Set.of(node(3).name()), emptySet());
        assertThat(localStatesFut, willCompleteSuccessfully());

        Map<TablePartitionId, LocalPartitionStateByNode> localStates = localStatesFut.join();
        assertThat(localStates, is(not(anEmptyMap())));
        LocalPartitionStateByNode localPartitionStateByNode = localStates.get(new TablePartitionId(tableId, partId));

        assertEquals(LocalPartitionStateEnum.INSTALLING_SNAPSHOT, localPartitionStateByNode.values().iterator().next().state);

        stopNode(1);
        waitForScale(node0, 3);

        waitForPartitionState(node0, partId, GlobalPartitionStateEnum.DEGRADED);

        resetFuture = node0.disasterRecoveryManager().resetAllPartitions(zoneName, QUALIFIED_TABLE_NAME, true);
        assertThat(resetFuture, willCompleteSuccessfully());

        waitForPartitionState(node0, partId, GlobalPartitionStateEnum.AVAILABLE);

        awaitPrimaryReplica(node0, partId);
        assertRealAssignments(node0, partId, 0, 2, 3);

        // Set time in the future to protect us from "getAsync" from the past.
        // Should be replaced with "sleep" when clock skew validation is implemented.
        node0.clock().update(node0.clock().now().addPhysicalTime(
                SECONDS.toMillis(DEFAULT_IDLE_SAFE_TIME_PROP_DURATION) + node0.clockService().maxClockSkewMillis())
        );

        // TODO https://issues.apache.org/jira/browse/IGNITE-22657
        //  We need wait quite a bit before data is available for some reason.
        Thread.sleep(10_000);

        // "forEach" makes "i" effectively final, which is convenient for internal lambda.
        IntStream.range(0, ENTRIES).forEach(i -> {
            CompletableFuture<Tuple> fut = table.keyValueView().getAsync(null, Tuple.create(of("id", i)));
            assertThat(fut, willCompleteSuccessfully());

            assertNotNull(fut.join());
        });
    }

    /**
     * Tests that in a situation from the test {@link #testInsertFailsIfMajorityIsLost()} it is possible to recover partition using a
     * disaster recovery API, but with manual flag set to false. We expect that in this replica factor won't be restored.
     * In this test, assignments will be (1, 3, 4), according to {@link RendezvousDistributionFunction}.
     */
    @Test
    @ZoneParams(nodes = 5, replicas = 3, partitions = 1)
    void testAutomaticRebalanceIfMajorityIsLost() throws Exception {
        int partId = 0;

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 1, 3, 4);

        stopNodesInParallel(3, 4);

        waitForScale(node0, 3);

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetAllPartitions(
                zoneName,
                QUALIFIED_TABLE_NAME,
                false
        );

        assertThat(updateFuture, willSucceedIn(60, SECONDS));

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 1);

        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        // Check that there is no ongoing or planned rebalance.
        assertNull(getPendingAssignments(node0, partId));

        assertRealAssignments(node0, partId, 1);
    }

    /**
     * Tests a scenario where there's a single partition on a node 1, and the node that hosts it is lost.
     * Not manual reset should do nothing in that case, so no new pending or planned is presented.
     */
    @Test
    @ZoneParams(nodes = 2, replicas = 1, partitions = 1)
    void testAutomaticRebalanceIfPartitionIsLost() throws Exception {
        int partId = 0;

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));

        IgniteImpl node1 = unwrapIgniteImpl(cluster.node(1));

        executeSql(format("ALTER ZONE %s SET data_nodes_auto_adjust_scale_down=%d", zoneName, INFINITE_TIMER_VALUE));

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 1);

        stopNodesInParallel(1);

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetAllPartitions(
                zoneName,
                QUALIFIED_TABLE_NAME,
                false
        );

        assertThat(updateFuture, willCompleteSuccessfully());

        // Check that there is no ongoing or planned rebalance.
        assertNull(getPendingAssignments(node0, partId));

        assertEquals(1, getStableAssignments(node0, partId).nodes().size());

        assertEquals(node1.name(), getStableAssignments(node0, partId).nodes().stream().findFirst().get().consistentId());
    }

    /**
     * Tests a scenario where only one node from stable is left, but we have node in pending nodes and perform reset partition operation.
     * We expect this node from pending being presented after reset, so not manual reset logic take into account pending nodes.
     *
     * <p>It goes like this:
     * <ul>
     *     <li>We have 6 nodes and a partition on nodes 1, 4 and 5.</li>
     *     <li>We stop node 5, so rebalance on 1, 3, 4 is triggered, but blocked and cannot be finished.</li>
     *     <li>Zones scale down is set to infinite value, we stop node 4 and new rebalance is not triggered and majority is lost.</li>
     *     <li>We execute "resetPartitions" and expect that pending assignments will be 1, 3, so node 3 from pending is presented.</li>
     * </ul>
     */
    @Test
    @ZoneParams(nodes = 6, replicas = 3, partitions = 1)
    public void testIncompleteRebalanceBeforeAutomaticResetPartitions() throws Exception {
        int partId = 0;

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));

        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();

        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);
        assertRealAssignments(node0, partId, 1, 4, 5);

        insertValues(table, partId, 0);

        Assignments assignment134 = Assignments.of(timestamp,
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(3).name()),
                Assignment.forPeer(node(4).name())
        );

        blockRebalanceStableSwitch(partId, assignment134);

        stopNode(5);

        waitForScale(node0, 5);

        assertRealAssignments(node0, partId, 1, 3, 4);

        assertPendingAssignments(node0, partId, assignment134);

        assertFalse(getPendingAssignments(node0, partId).force());

        waitForPartitionState(node0, partId, GlobalPartitionStateEnum.AVAILABLE);

        executeSql(format("ALTER ZONE %s SET data_nodes_auto_adjust_scale_down=%d", zoneName, INFINITE_TIMER_VALUE));

        stopNode(4);

        CompletableFuture<Void> resetFuture =
                node0.disasterRecoveryManager().resetAllPartitions(zoneName, QUALIFIED_TABLE_NAME, false);
        assertThat(resetFuture, willCompleteSuccessfully());

        Assignments assignmentForced1 = Assignments.forced(Set.of(Assignment.forPeer(node(1).name())), timestamp);

        assertPendingAssignments(node0, partId, assignmentForced1);

        Assignments assignments13 = Assignments.of(Set.of(
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(3).name())
        ), timestamp);

        assertPlannedAssignments(node0, partId, assignments13);
    }

    /**
     * The test creates a group of 7 nodes, and performs writes in three steps:
     * <ol>
     * <li>Write data to all nodes.</li>
     * <li>Block raft updates on one of the nodes. Write second portion of data to the group.</li>
     * <li>Additionally block updates on another node. Write third portion of data to the group.</li>
     * </ol>
     *
     * <p>As a result, we'll have one node having only data(1), one node having data(2) and the other 5 nodes having
     * data(3).
     * Then we stop 4 out of 5 nodes with data(3) - effectively the majority of 7 nodes, and call resetPartitions.
     * The two phase reset should pick the node with the highest raft log index as the single node for the pending assignments.
     */
    @Test
    @ZoneParams(nodes = 7, replicas = 7, partitions = 1)
    void testThoPhaseResetMaxLogIndex() throws Exception {
        int partId = 0;

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        Set<String> clusterNodeNames = Set.of(
                node(0).name(),
                node(1).name(),
                node(2).name(),
                node(3).name(),
                node(4).name(),
                node(5).name(),
                node(6).name());
        assertRealAssignments(node0, partId, 0, 1, 2, 3, 4, 5, 6);

        // Write data(1) to all seven nodes.
        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);

        // We filter out the leader to be able to reliably block raft state transfer.
        String leaderName = findLeader(1, partId);
        logger().info("Raft group leader is [id={}]", leaderName);

        // All the nodes except the leader - 6 nodes.
        List<String> followerNodes = new ArrayList<>(clusterNodeNames);
        followerNodes.remove(leaderName);

        // The nodes that we block AppendEntriesRequest to.
        Set<String> blockedNodes = new ConcurrentHashSet<>();

        // Exclude one of the nodes from data(2).
        int node0IndexInFollowers = followerNodes.indexOf(node0.name());
        // Make sure node 0 is not stopped. If node0IndexInFollowers==-1, then node0 is the leader.
        blockedNodes.add(followerNodes.remove(node0IndexInFollowers == -1 ? 0 : node0IndexInFollowers));
        logger().info("Blocking updates on nodes [ids={}]", blockedNodes);

        blockMessage((nodeName, msg) -> dataReplicateMessage(nodeName, msg, tablePartitionId, blockedNodes));

        // Write data(2) to 6 nodes.
        errors = insertValues(table, partId, 10);
        assertThat(errors, is(empty()));

        // Exclude one more node from data replication.
        blockedNodes.add(followerNodes.remove(0));
        logger().info("Blocking updates on nodes [ids={}]", blockedNodes);

        // Write data(3) to 5 nodes.
        errors = insertValues(table, partId, 20);
        assertThat(errors, is(empty()));

        // Now followerNodes has 4 elements - without the leader and two blocked nodes. Stop them all.
        int[] nodesToStop = followerNodes.stream()
                .mapToInt(this::nodeIndex)
                .toArray();
        logger().info("Stopping nodes [id={}]", Arrays.toString(nodesToStop));
        stopNodesInParallel(nodesToStop);

        // Only 3 nodes left.
        waitForScale(node0, 3);

        // One of them has the most up to date data, the others fall behind.
        waitForPartitionState(node0, partId, GlobalPartitionStateEnum.READ_ONLY);

        // Collect nodes that will be the part of the planned assignments.
        // These are the leader and two blocked nodes.
        List<String> nodesNamesForFinalAssignments = new ArrayList<>(blockedNodes);
        nodesNamesForFinalAssignments.add(leaderName);

        // Unblock raft.
        blockedNodes.clear();

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetAllPartitions(
                zoneName,
                QUALIFIED_TABLE_NAME,
                true
        );
        assertThat(updateFuture, willCompleteSuccessfully());

        // Pending is the one with the most up to date log index.
        Assignments assignmentPending = Assignments.forced(Set.of(Assignment.forPeer(leaderName)), timestamp);

        assertPendingAssignments(node0, partId, assignmentPending);

        Set<Assignment> peers = nodesNamesForFinalAssignments.stream()
                .map(Assignment::forPeer)
                .collect(Collectors.toSet());

        Assignments assignmentsPlanned = Assignments.of(peers, timestamp);

        assertPlannedAssignments(node0, partId, assignmentsPlanned);

        // Wait for the new stable assignments to take effect.
        executeSql(format("ALTER ZONE %s SET replicas=%d", zoneName, 3));

        waitForPartitionState(node0, partId, GlobalPartitionStateEnum.AVAILABLE);

        // Make sure the data is present.
        IntStream.range(0, ENTRIES).forEach(i -> {
            CompletableFuture<Tuple> fut = table.keyValueView().getAsync(null, Tuple.create(of("id", i)));
            assertThat(fut, willCompleteSuccessfully());

            assertEquals(Tuple.create(of("val", i + 20)), fut.join());
        });
    }

    /**
     * The test creates a group of 7 nodes, and performs writes in two steps:
     * <ol>
     * <li>Write data to all nodes.</li>
     * <li>Block raft updates on one of the nodes. Write second portion of data to the group.</li>
     * </ol>
     *
     * <p>As a result, we'll have one node having only data(1) and the other 6 nodes having
     * data(2).
     * Then we stop 4 nodes with data(2) - effectively the majority of 7 nodes, and call resetPartitions.
     * The two phase reset should pick the node with the highest raft log index as the single node for the pending assignments.
     */
    @Test
    @ZoneParams(nodes = 7, replicas = 7, partitions = 1)
    void testThoPhaseResetEqualLogIndex() throws Exception {
        int partId = 0;

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        Set<String> clusterNodeNames = Set.of(
                node(0).name(),
                node(1).name(),
                node(2).name(),
                node(3).name(),
                node(4).name(),
                node(5).name(),
                node(6).name());
        assertRealAssignments(node0, partId, 0, 1, 2, 3, 4, 5, 6);

        // Write data(1) to all seven nodes.
        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partId);

        // We filter out the leader to be able to reliably block raft state transfer on the other nodes.
        String leaderName = findLeader(1, partId);
        logger().info("Raft group leader is [id={}]", leaderName);

        // All the nodes except the leader - 6 nodes.
        List<String> followerNodes = new ArrayList<>(clusterNodeNames);
        followerNodes.remove(leaderName);

        // The nodes that we block AppendEntriesRequest to.
        Set<String> blockedNodes = new ConcurrentHashSet<>();

        // Exclude one of the nodes from data(2).
        int node0IndexInFollowers = followerNodes.indexOf(node0.name());
        // Make sure node 0 is not stopped. If node0IndexInFollowers==-1, then node0 is the leader.
        String blockedNode = followerNodes.remove(node0IndexInFollowers == -1 ? 0 : node0IndexInFollowers);
        blockedNodes.add(blockedNode);
        logger().info("Blocking updates on nodes [ids={}]", blockedNodes);

        blockMessage((nodeName, msg) -> dataReplicateMessage(nodeName, msg, tablePartitionId, blockedNodes));

        // Write data(2) to 6 nodes.
        errors = insertValues(table, partId, 10);
        assertThat(errors, is(empty()));

        List<String> nodeNamesToStop = new ArrayList<>(followerNodes);
        // Make sure there are 4 elements in this list.
        nodeNamesToStop.remove(0);
        // Stop them all.
        int[] nodesToStop = nodeNamesToStop.stream()
                .mapToInt(this::nodeIndex)
                .toArray();
        logger().info("Stopping nodes [id={}]", Arrays.toString(nodesToStop));
        stopNodesInParallel(nodesToStop);

        // Only 3 nodes left.
        waitForScale(node0, 3);

        // Unblock raft.
        blockedNodes.clear();

        // Two nodes should have the most up to date data, one falls behind.
        waitForPartitionState(node0, partId, GlobalPartitionStateEnum.READ_ONLY);

        // Collect nodes that will be the part of the planned assignments.
        List<String> nodesNamesForFinalAssignments = new ArrayList<>(clusterNodeNames);
        nodesNamesForFinalAssignments.removeAll(nodeNamesToStop);

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetAllPartitions(
                zoneName,
                QUALIFIED_TABLE_NAME,
                false
        );
        assertThat(updateFuture, willCompleteSuccessfully());

        String pendingNodeName = getPendingNodeName(nodesNamesForFinalAssignments, blockedNode);

        // Pending is the one with the most up to date log index.
        Assignments assignmentPending = Assignments.forced(Set.of(Assignment.forPeer(pendingNodeName)), timestamp);

        assertPendingAssignments(node0, partId, assignmentPending);

        Set<Assignment> peers = nodesNamesForFinalAssignments.stream()
                .map(Assignment::forPeer)
                .collect(Collectors.toSet());

        Assignments assignmentsPlanned = Assignments.of(peers, timestamp);

        assertPlannedAssignments(node0, partId, assignmentsPlanned);

        // Wait for the new stable assignments to take effect.
        executeSql(format("ALTER ZONE %s SET replicas=%d", zoneName, 3));

        waitForPartitionState(node0, partId, GlobalPartitionStateEnum.AVAILABLE);

        // Make sure the data is present.
        IntStream.range(0, ENTRIES).forEach(i -> {
            CompletableFuture<Tuple> fut = table.keyValueView().getAsync(null, Tuple.create(of("id", i)));
            assertThat(fut, willCompleteSuccessfully());

            assertEquals(Tuple.create(of("val", i + 10)), fut.join());
        });
    }

    @Test
    @ZoneParams(nodes = 6, replicas = 3, partitions = 1)
    void testTwoPhaseResetOnEmptyNodes() throws Exception {
        int partId = 0;

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 2, 3, 5);

        Assignments blockedRebalance = Assignments.of(timestamp,
                Assignment.forPeer(node(0).name())
        );

        blockRebalanceStableSwitch(partId, blockedRebalance);

        stopNodesInParallel(2, 3, 5);

        waitForScale(node0, 3);

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetAllPartitions(
                zoneName,
                QUALIFIED_TABLE_NAME,
                true
        );

        assertThat(updateFuture, willCompleteSuccessfully());

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 1, 4);

        Assignments assignmentForced1 = Assignments.forced(Set.of(Assignment.forPeer(node(0).name())), timestamp);

        assertPendingAssignments(node0, partId, assignmentForced1);

        Assignments assignments13 = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(4).name())
        ), timestamp);

        assertPlannedAssignments(node0, partId, assignments13);
    }

    private static String getPendingNodeName(List<String> aliveNodes, String blockedNode) {
        List<String> candidates = new ArrayList<>(aliveNodes);
        candidates.remove(blockedNode);

        // Without the blocking node, the other two should have the same raft log index.
        // Pick the one with the name
        candidates.sort(String::compareTo);

        return candidates.get(0);
    }

    private void blockMessage(BiPredicate<String, NetworkMessage> predicate) {
        cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).forEach(node -> {
            BiPredicate<String, NetworkMessage> oldPredicate = node.dropMessagesPredicate();

            if (oldPredicate == null) {
                node.dropMessages(predicate);
            } else {
                node.dropMessages(oldPredicate.or(predicate));
            }
        });
    }

    private static boolean dataReplicateMessage(
            String nodeName,
            NetworkMessage msg,
            TablePartitionId tablePartitionId,
            Set<String> blockedNodes
    ) {
        if (msg instanceof AppendEntriesRequest) {
            var appendEntriesRequest = (AppendEntriesRequest) msg;
            if (tablePartitionId.toString().equals(appendEntriesRequest.groupId())) {
                return blockedNodes.contains(nodeName);
            }
        }
        return false;
    }

    /**
     * Block rebalance, so stable won't be switched to specified pending.
     */
    private void blockRebalanceStableSwitch(int partId, Assignments assignment) {
        blockMessage((nodeName, msg) -> stableKeySwitchMessage(msg, partId, assignment));
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
                    List<Operation> operations = updateStatement.update().operations();

                    ByteArray stablePartAssignmentsKey = stablePartAssignmentsKey(new TablePartitionId(tableId, partId));

                    for (Operation operation : operations) {
                        ByteArray opKey = new ByteArray(toByteArray(operation.key()));

                        if (operation.type() == OperationType.PUT && opKey.equals(stablePartAssignmentsKey)) {
                            return blockedAssignments.equals(Assignments.fromBytes(toByteArray(operation.value())));
                        }
                    }
                }
            }
        }

        return false;
    }

    private void waitForPartitionState(IgniteImpl node0, int partId, GlobalPartitionStateEnum expectedState) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            CompletableFuture<Map<TablePartitionId, GlobalPartitionState>> statesFuture = node0.disasterRecoveryManager()
                    .globalPartitionStates(Set.of(zoneName), emptySet());

            assertThat(statesFuture, willCompleteSuccessfully());

            Map<TablePartitionId, GlobalPartitionState> map = statesFuture.join();

            GlobalPartitionState state = map.get(new TablePartitionId(tableId, partId));

            return state != null && state.state == expectedState;
        }, 500, 20_000),
                () -> "Expected state: " + expectedState
                        + ", actual: " + node0.disasterRecoveryManager().globalPartitionStates(Set.of(zoneName), emptySet()).join()
        );
    }

    private String findLeader(int nodeIdx, int partId) {
        IgniteImpl node = unwrapIgniteImpl(node(nodeIdx));

        var raftNodeId = new RaftNodeId(new TablePartitionId(tableId, partId), new Peer(node.name()));
        var jraftServer = (JraftServerImpl) node.raftManager().server();

        RaftGroupService raftGroupService = jraftServer.raftGroupService(raftNodeId);
        assertNotNull(raftGroupService);
        return raftGroupService.getRaftNode().getLeaderId().getConsistentId();
    }

    private void triggerRaftSnapshot(int nodeIdx, int partId) throws InterruptedException, ExecutionException {
        //noinspection resource
        IgniteImpl node = unwrapIgniteImpl(node(nodeIdx));

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
        assertTrue(
                waitForCondition(() -> List.of(expected).equals(getRealAssignments(node0, partId)), 2000),
                () -> "Expected: " + List.of(expected) + ", actual: " + getRealAssignments(node0, partId)
        );
    }

    private void assertPendingAssignments(IgniteImpl node0, int partId, Assignments expected) throws InterruptedException {
        assertTrue(
                waitForCondition(() -> expected.equals(getPendingAssignments(node0, partId)), 2000),
                () -> "Expected: " + expected + ", actual: " + getPendingAssignments(node0, partId)
        );
    }

    private void assertPlannedAssignments(IgniteImpl node0, int partId, Assignments expected) throws InterruptedException {
        assertTrue(
                waitForCondition(() -> expected.equals(getPlannedAssignments(node0, partId)), 2000),
                () -> "Expected: " + expected + ", actual: " + getPlannedAssignments(node0, partId)
        );
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

            int catalogVersion = node.catalogManager().latestCatalogVersion();

            CompletableFuture<Set<String>> dataNodes = dzManager.dataNodes(causalityToken, catalogVersion, zoneId);

            try {
                return dataNodes.get(10, SECONDS).size() == targetDataNodesCount;
            } catch (Exception e) {
                return false;
            }
        }, 250, SECONDS.toMillis(60)));
    }

    /**
     * Return assignments based on states of partitions in the cluster. It is possible that returned value contains nodes
     * from stable and pending, for example, when rebalance is in progress.
     */
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

    private @Nullable Assignments getPlannedAssignments(IgniteImpl node, int partId) {
        CompletableFuture<Entry> plannedFut = node.metaStorageManager()
                .get(plannedPartAssignmentsKey(new TablePartitionId(tableId, partId)));

        assertThat(plannedFut, willCompleteSuccessfully());

        Entry planned = plannedFut.join();

        return planned.empty() ? null : Assignments.fromBytes(planned.value());
    }

    private @Nullable Assignments getPendingAssignments(IgniteImpl node, int partId) {
        CompletableFuture<Entry> pendingFut = node.metaStorageManager()
                .get(pendingPartAssignmentsKey(new TablePartitionId(tableId, partId)));

        assertThat(pendingFut, willCompleteSuccessfully());

        Entry pending = pendingFut.join();

        return pending.empty() ? null : Assignments.fromBytes(pending.value());
    }

    private @Nullable Assignments getStableAssignments(IgniteImpl node, int partId) {
        CompletableFuture<Entry> stableFut = node.metaStorageManager()
                .get(stablePartAssignmentsKey(new TablePartitionId(tableId, partId)));

        assertThat(stableFut, willCompleteSuccessfully());

        Entry stable = stableFut.join();

        return stable.empty() ? null : Assignments.fromBytes(stable.value());
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface ZoneParams {
        int replicas();

        int partitions();

        int nodes() default INITIAL_NODES;
    }
}
