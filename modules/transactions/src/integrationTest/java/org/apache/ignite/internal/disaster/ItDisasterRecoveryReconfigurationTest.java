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
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.disaster.DisasterRecoveryTestUtil.assertValueOnSpecificNode;
import static org.apache.ignite.internal.disaster.DisasterRecoveryTestUtil.blockMessage;
import static org.apache.ignite.internal.disaster.DisasterRecoveryTestUtil.stableKeySwitchMessage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.replicator.configuration.ReplicationConfigurationSchema.DEFAULT_IDLE_SAFE_TIME_PROP_DURATION;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.awaitility.Awaitility.await;
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterConfiguration.Builder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsChain;
import org.apache.ignite.internal.partitiondistribution.AssignmentsLink;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.partitiondistribution.RendezvousDistributionFunction;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.configuration.ReplicationExtensionConfiguration;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateByNode;
import org.apache.ignite.internal.table.distributed.disaster.TestDisasterRecoveryUtils;
import org.apache.ignite.internal.testframework.failure.FailureManagerExtension;
import org.apache.ignite.internal.testframework.failure.MuteFailureManagerLogging;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for scenarios where majority of peers is not available. In this class we frequently assert partition distributions, this means that
 * tests code implicitly depends on test names, because we use method names to generate node names, and we use node names to assign
 * partitions.
 */
@Timeout(120)
@ExtendWith(FailureManagerExtension.class)
public class ItDisasterRecoveryReconfigurationTest extends ClusterPerTestIntegrationTest {
    /** Scale-down timeout. */
    private static final int SCALE_DOWN_TIMEOUT_SECONDS = 2;

    /** Test table name. */
    private static final String TABLE_NAME = "TEST";

    private static final int ENTRIES = 2;

    private static final int INITIAL_NODES = 1;

    /** Zone name, unique for each test. */
    private String zoneName;

    /** Zone ID that corresponds to {@link #zoneName}. */
    private int zoneId;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("id", NativeTypes.INT32, false)},
            new Column[]{
                    new Column("val", NativeTypes.INT32, false),
            }
    );

    @Override
    protected int initialNodes() {
        return INITIAL_NODES;
    }

    @Override
    protected void customizeConfiguration(Builder clusterConfigurationBuilder) {
        clusterConfigurationBuilder
                .nodeNamingStrategy((conf, index) -> testNodeName(conf.testInfo(), index));
    }

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        Method testMethod = testInfo.getTestMethod().orElseThrow();

        IgniteImpl node0 = igniteImpl(0);

        zoneName = "ZONE_" + testMethod.getName().toUpperCase();

        ZoneParams zoneParams = testMethod.getAnnotation(ZoneParams.class);

        startNodesInParallel(IntStream.range(INITIAL_NODES, zoneParams.nodes()).toArray());

        executeSql(format("CREATE ZONE %s (replicas %d, partitions %d, "
                        + "auto scale down %d, auto scale up %d, consistency mode '%s') storage profiles ['%s']",
                zoneName, zoneParams.replicas(), zoneParams.partitions(), SCALE_DOWN_TIMEOUT_SECONDS, 1,
                zoneParams.consistencyMode().name(), DEFAULT_STORAGE_PROFILE
        ));

        CatalogZoneDescriptor zone = node0.catalogManager().activeCatalog(node0.clock().nowLong()).zone(zoneName);
        zoneId = requireNonNull(zone).id();
        waitForScale(node0, zoneParams.nodes());

        executeSql(format("CREATE TABLE %s (id INT PRIMARY KEY, val INT) ZONE %s", TABLE_NAME, zoneName));
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

        IgniteImpl node0 = igniteImpl(0);
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

        IgniteImpl node0 = igniteImpl(0);
        Table table = node0.tables().table(TABLE_NAME);
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 3, 4);

        stopNodesInParallel(3, 4);

        waitForScale(node0, 3);

        CompletableFuture<?> updateFuture = node0.disasterRecoveryManager().resetPartitions(
                zoneName,
                emptySet(),
                true,
                -1
        );

        assertThat(updateFuture, willSucceedIn(60, SECONDS));

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 1, 2);

        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        // No fromReset flag is set on stable.
        Assignments assignmentsStable = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(2).name())
        ), timestamp);

        assertStableAssignments(node0, partId, assignmentsStable);
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

        IgniteImpl node0 = igniteImpl(0);
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
                Set.of(anotherPartId),
                true,
                -1
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

        IgniteImpl node0 = igniteImpl(0);
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 1);

        stopNodesInParallel(1);

        waitForScale(node0, 1);

        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<?> updateFuture = disasterRecoveryManager.resetPartitions(
                zoneName,
                emptySet(),
                true,
                0
        );

        assertThat(updateFuture, willCompleteSuccessfully());

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0);

        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));
    }

    @Test
    @ZoneParams(nodes = 5, replicas = 3, partitions = 1)
    @MuteFailureManagerLogging
    void testManualRebalanceRecovery() throws Exception {
        int partId = 0;
        // Disable scale down to avoid unwanted rebalance.
        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, INFINITE_TIMER_VALUE));

        IgniteImpl node0 = igniteImpl(0);
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        Assignments allAssignments = Assignments.of(timestamp,
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(2).name())
        );

        // The partition is replicated to 3 nodes.
        assertRealAssignments(node0, partId, 0, 1, 2);

        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        // Make sure node 0 has the same data as node 1 and 2.
        assertTrue(
                waitForCondition(() -> getRaftLogIndex(0, partId).equals(getRaftLogIndex(1, partId)), SECONDS.toMillis(20)),
                () -> "Node 0 log index = " + getRaftLogIndex(0, partId) + " node 1 log index= " + getRaftLogIndex(1, partId)
        );
        assertTrue(
                waitForCondition(() -> getRaftLogIndex(0, partId).equals(getRaftLogIndex(2, partId)), SECONDS.toMillis(20)),
                () -> "Node 0 log index = " + getRaftLogIndex(0, partId) + " node 2 log index= " + getRaftLogIndex(2, partId)
        );

        // Stop 1 and 2, only 0 survived.
        stopNodesInParallel(1, 2);

        Assignments assignment0 = Assignments.of(timestamp,
                Assignment.forPeer(node(0).name())
        );

        // Blocking stable switch to the first phase or reset,
        // so that we'll have force pending assignments unexecuted.
        blockMessage(cluster, (nodeName, msg) -> stableKeySwitchMessage(msg, partitionGroupId(partId), assignment0));

        // Init reset:
        // pending = [0, force]
        // planned = [0, 3, 4]
        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<?> updateFuture = disasterRecoveryManager.resetPartitions(
                zoneName,
                emptySet(),
                true,
                -1
        );

        assertThat(updateFuture, willCompleteSuccessfully());

        awaitPrimaryReplica(node0, partId);

        // Since stable switch to 0 is blocked, data is stored on 0 node only.
        assertRealAssignments(node0, partId, 0);

        // And stable is in its initial state.
        assertStableAssignments(node0, partId, allAssignments);

        // Insert new data on the node 0.
        errors = insertValues(table, partId, 10);
        assertThat(errors, is(empty()));

        // Start nodes 1 and 2 back, but they should not start their replicas.
        startNode(1);
        startNode(2);

        // Make sure 1 and 2 did not start.
        assertRealAssignments(node0, partId, 0);
    }

    @Test
    @ZoneParams(nodes = 4, replicas = 3, partitions = 1)
    @MuteFailureManagerLogging
    void testManualRebalanceRecoveryNoPending() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        Assignments allAssignments = Assignments.of(timestamp,
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(3).name())
        );

        // The partition is replicated to 3 nodes.
        assertRealAssignments(node0, partId, 0, 1, 3);

        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        // Make sure node 0 has the same data as node 1 and 2.
        assertTrue(
                waitForCondition(() -> getRaftLogIndex(0, partId).equals(getRaftLogIndex(1, partId)), SECONDS.toMillis(20)),
                () -> "Node 0 log index = " + getRaftLogIndex(0, partId) + " node 1 log index= " + getRaftLogIndex(1, partId)
        );
        assertTrue(
                waitForCondition(() -> getRaftLogIndex(0, partId).equals(getRaftLogIndex(3, partId)), SECONDS.toMillis(20)),
                () -> "Node 0 log index = " + getRaftLogIndex(0, partId) + " node 2 log index= " + getRaftLogIndex(3, partId)
        );

        Assignments assignmentPending = Assignments.of(timestamp,
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(2).name())
        );

        // Blocking stable switch to the first phase or reset,
        // so that we'll have force pending assignments unexecuted.
        blockMessage(cluster, (nodeName, msg) -> stableKeySwitchMessage(msg, partitionGroupId(partId), assignmentPending));

        // Stop 3. Nodes 0 and 1 survived.
        stopNode(3);

        waitForScale(node0, 3);

        assertRealAssignments(node0, partId, 0, 1, 2);

        assertPendingAssignments(node0, partId, assignmentPending);
        assertStableAssignments(node0, partId, allAssignments);

        // On start we check `pending.contains(node) || (stable.contains(node) && !pending.isForce())` condition.
        // The node is in stable, not in pending and pending is not forced => start it.
        startNode(3);

        assertRealAssignments(node0, partId, 0, 1, 2, 3);
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

        IgniteImpl node0 = igniteImpl(0);

        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();

        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);
        assertRealAssignments(node0, partId, 1, 4, 5);

        insertValues(table, partId, 0);

        triggerRaftSnapshot(1, partId, true);

        igniteImpl(1).dropMessages((nodeName, msg) -> {
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

        // Reset produces
        // pending = [1, force]
        // planned = [0, 1, 3]
        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<Void> resetFuture = disasterRecoveryManager.resetPartitions(
                zoneName,
                emptySet(),
                true,
                -1
        );
        assertThat(resetFuture, willCompleteSuccessfully());

        waitForZonePartitionState(node0, partId, GlobalPartitionStateEnum.DEGRADED);

        assertZoneLocalState(node0, partId, LocalPartitionStateEnum.INSTALLING_SNAPSHOT);

        // fromReset == true, assert force == false.
        Assignments assignmentsPending = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(3).name())
        ), timestamp, true);

        assertPendingAssignments(node0, partId, assignmentsPending);

        // need to wait
        // Need to verify that other nodes managed to switch to the new configuration.
        // Stopping the leader before the group switched to the new configuration => the other nodes will never progress as they're
        // on the old configuration. In out case - The first seen one, [1,4,5].
        // In other words, need to wait:
        // [StateMachineAdapter] onConfigurationCommitted: idrrt_tirarp_0,idrrt_tirarp_3,idrrt_tirarp_1.
        List<String> expectedPeers = List.of(node(0).name(), node(1).name(), node(3).name());
        assertConfigurationApplied(node0, partId, expectedPeers);

        stopNode(1);
        waitForScale(node0, 3);

        waitForZonePartitionState(node0, partId, GlobalPartitionStateEnum.DEGRADED);

        resetFuture = disasterRecoveryManager.resetPartitions(
                zoneName,
                emptySet(),
                true,
                -1
        );
        assertThat(resetFuture, willCompleteSuccessfully());

        waitForZonePartitionState(node0, partId, GlobalPartitionStateEnum.AVAILABLE);

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

    @Test
    @ZoneParams(nodes = 5, replicas = 3, partitions = 1)
    @MuteFailureManagerLogging
    public void testNewResetOverwritesFlags() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);

        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 1, 4);

        log.info("Test: stopping nodes.");

        stopNodesInParallel(1, 4);
        waitForScale(node0, 3);

        assertRealAssignments(node0, partId, 0, 2, 3);

        Assignments assignments = Assignments.of(timestamp,
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(2).name()),
                Assignment.forPeer(node(3).name())
        );

        blockRebalanceStableSwitch(partId, assignments);

        // Reset produces
        // pending = [0, force]
        // planned = [0, 2, 3]
        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<Void> resetFuture = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), true, -1);
        assertThat(resetFuture, willCompleteSuccessfully());

        waitForZonePartitionState(node0, partId, GlobalPartitionStateEnum.AVAILABLE);

        // fromReset == true, assert force == false.
        Assignments assignmentsPending = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(2).name()),
                Assignment.forPeer(node(3).name())
        ), timestamp, true);

        assertPendingAssignments(node0, partId, assignmentsPending);

        // Any of 3 can be chosen the node for the forced pending, so block them all.
        Assignments blockedRebalance0 = Assignments.of(timestamp,
                Assignment.forPeer(node(0).name())
        );
        blockRebalanceStableSwitch(partId, blockedRebalance0);

        Assignments blockedRebalance2 = Assignments.of(timestamp,
                Assignment.forPeer(node(2).name())
        );
        blockRebalanceStableSwitch(partId, blockedRebalance2);

        Assignments blockedRebalance3 = Assignments.of(timestamp,
                Assignment.forPeer(node(3).name())
        );
        blockRebalanceStableSwitch(partId, blockedRebalance3);

        CompletableFuture<Void> resetFuture2 = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), true, -1);
        assertThat(resetFuture2, willCompleteSuccessfully());

        Assignments pendingAssignments = getPendingAssignments(node0, partId);

        assertTrue(pendingAssignments.force());
        assertFalse(pendingAssignments.fromReset());
    }

    @Test
    @ZoneParams(nodes = 5, replicas = 3, partitions = 1)
    public void testPlannedIsOverwritten() throws Exception {
        // Disable scale down to avoid unwanted rebalance.
        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, INFINITE_TIMER_VALUE));
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);

        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 1, 2);

        stopNodesInParallel(1, 2);

        Assignments assignments = Assignments.of(timestamp,
                Assignment.forPeer(node(0).name())
        );

        blockRebalanceStableSwitch(partId, assignments);

        // Reset produces
        // pending = [0, force]
        // planned = [0, 3, 4]
        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<Void> resetFuture = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), true, -1);
        assertThat(resetFuture, willCompleteSuccessfully());

        Assignments assignmentsPending = Assignments.forced(Set.of(
                Assignment.forPeer(node(0).name())
        ), timestamp);

        assertPendingAssignments(node0, partId, assignmentsPending);

        Assignments assignmentsPlanned = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(3).name()),
                Assignment.forPeer(node(4).name())
        ), timestamp, true);

        assertPlannedAssignments(node0, partId, assignmentsPlanned);

        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, 2));

        Assignments assignmentsPlannedReplaced = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(3).name()),
                Assignment.forPeer(node(4).name())
        ), timestamp);

        assertPlannedAssignments(node0, partId, assignmentsPlannedReplaced, 10_000);
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

        IgniteImpl node0 = igniteImpl(0);
        Table table = node0.tables().table(TABLE_NAME);
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 1, 3, 4);

        stopNodesInParallel(3, 4);

        waitForScale(node0, 3);

        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<?> updateFuture = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), false, 1);

        assertThat(updateFuture, willSucceedIn(60, SECONDS));

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 1);

        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        // Check that there is no ongoing or planned rebalance.
        assertNull(getPendingAssignments(node0, partId));

        assertRealAssignments(node0, partId, 1);

        // No fromReset flag is set on stable.
        Assignments assignmentsStable = Assignments.of(Set.of(
                Assignment.forPeer(node(1).name())
        ), timestamp);

        assertStableAssignments(node0, partId, assignmentsStable);
    }

    /**
     * Tests a scenario where there's a single partition on a node 1, and the node that hosts it is lost.
     * Not manual reset should do nothing in that case, so no new pending or planned is presented.
     */
    @Test
    @ZoneParams(nodes = 2, replicas = 1, partitions = 1)
    void testAutomaticRebalanceIfPartitionIsLost() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);

        IgniteImpl node1 = igniteImpl(1);

        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, INFINITE_TIMER_VALUE));

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 1);

        stopNodesInParallel(1);

        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<?> updateFuture = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), false, 1);

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
    @MuteFailureManagerLogging
    public void testIncompleteRebalanceBeforeAutomaticResetPartitions() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);

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

        waitForZonePartitionState(node0, partId, GlobalPartitionStateEnum.AVAILABLE);

        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, INFINITE_TIMER_VALUE));

        stopNode(4);

        // Given block of assignment stable switch halts rebalances and thus provides an ability to verify pending and planned assignments.
        // Without the block test may fail because rebalance may finish prior to corresponding check. In other words given block eliminates
        // the race between rebalance and assignments verifications that we do below.
        // Worth mentioning that assignments stable switch ignores force flag, thus within blocked assignments force = false is used.
        Assignments assignmentToBlock = Assignments.of(Set.of(Assignment.forPeer(node(1).name())), timestamp);
        blockRebalanceStableSwitch(partId, assignmentToBlock);

        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<Void> resetFuture = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), false, 1);
        assertThat(resetFuture, willCompleteSuccessfully());

        // force == true, fromReset == false.
        Assignments assignmentForced1 = Assignments.forced(Set.of(Assignment.forPeer(node(1).name())), timestamp);

        assertPendingAssignments(node0, partId, assignmentForced1);

        // fromReset == true, force == false.
        Assignments assignments13 = Assignments.of(Set.of(
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(3).name())
        ), timestamp, true);

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
    void testTwoPhaseResetMaxLogIndex() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);
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

        CatalogZoneDescriptor zone = node0.catalogManager().activeCatalog(node0.clock().nowLong()).zone(zoneName);
        Collection<String> dataNodes = new HashSet<>();
        for (int i = 0; i < 7; i++) {
            dataNodes.add(node(i).name());
        }

        logger().info("Zone {}", zone);

        Set<Assignment> allAssignmentsSet = calculateAssignmentForPartition(
                dataNodes, partId, zone.partitions(), zone.replicas(), zone.consensusGroupSize());

        Assignments allAssignments = Assignments.of(allAssignmentsSet, timestamp);

        assertStableAssignments(node0, partId, allAssignments);

        assertAssignmentsChain(node0, partId, null);

        // Write data(1) to all seven nodes.
        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        ZonePartitionId partitionGroupId = partitionGroupId(partId);

        // We filter out the leader to be able to reliably block raft state transfer.
        String leaderName = findLeader(1, partId);
        logger().info("Raft group leader is [id={}]", leaderName);

        // All the nodes except the leader - 6 nodes.
        List<String> followerNodes = new ArrayList<>(clusterNodeNames);
        followerNodes.remove(leaderName);

        // The nodes that we block AppendEntriesRequest to.
        Set<String> blockedNodes = ConcurrentHashMap.newKeySet();

        // Exclude one of the nodes from data(2).
        int node0IndexInFollowers = followerNodes.indexOf(node0.name());
        // Make sure node 0 is not stopped. If node0IndexInFollowers==-1, then node0 is the leader.
        blockedNodes.add(followerNodes.remove(node0IndexInFollowers == -1 ? 0 : node0IndexInFollowers));
        logger().info("Blocking updates on nodes [ids={}]", blockedNodes);

        blockMessage(cluster, (nodeName, msg) -> dataReplicateMessage(nodeName, msg, partitionGroupId, blockedNodes));

        // Write data(2) to 6 nodes.
        errors = insertValues(table, partId, 10);
        assertThat(errors, is(empty()));

        // Exclude one more node from data replication.
        blockedNodes.add(followerNodes.remove(0));
        logger().info("Blocking updates on nodes [ids={}]", blockedNodes);

        // Write data(3) to 5 nodes.
        errors = insertValues(table, partId, 20);
        assertThat(errors, is(empty()));

        // Disable scale down.
        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, INFINITE_TIMER_VALUE));

        // Now followerNodes has 4 elements - without the leader and two blocked nodes. Stop them all.
        int[] nodesToStop = followerNodes.stream()
                .mapToInt(this::nodeIndex)
                .toArray();
        logger().info("Stopping nodes [id={}]", Arrays.toString(nodesToStop));
        stopNodesInParallel(nodesToStop);

        // One of them has the most up to date data, the others fall behind.
        waitForZonePartitionState(node0, partId, GlobalPartitionStateEnum.READ_ONLY);

        // Collect nodes that will be the part of the planned assignments.
        // These are the leader and two blocked nodes.
        List<String> nodesNamesForFinalAssignments = new ArrayList<>(blockedNodes);
        nodesNamesForFinalAssignments.add(leaderName);

        // Given block of assignment stable switch halts rebalances and thus provides an ability to verify pending and planned assignments.
        // Without the block test may fail because rebalance may finish prior to corresponding check. In other words given block eliminates
        // the race between rebalance and assignments verifications that we do below.
        // Worth mentioning that assignments stable switch ignores force flag, thus within blocked assignments force = false is used.
        Assignments assignmentToBlock = Assignments.of(Set.of(Assignment.forPeer(leaderName)), timestamp);
        blockRebalanceStableSwitch(partId, assignmentToBlock);

        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<?> updateFuture = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), true, -1);
        assertThat(updateFuture, willCompleteSuccessfully());

        // It's important to unblock appendEntries requests after resetPartitions, otherwise 2 or even all 3 nodes may align by data/index
        // and thus GroupUpdateRequestHandler#nextAssignment may evaluate second or third node as reset first phase target instead of
        // expected within test leaderName = findLeader(1, partId);

        // Unblock raft.
        blockedNodes.clear();

        // Pending is the one with the most up to date log index.
        Assignments assignmentPending = Assignments.forced(Set.of(Assignment.forPeer(leaderName)), timestamp);

        assertPendingAssignments(node0, partId, assignmentPending);

        Set<Assignment> peers = nodesNamesForFinalAssignments.stream()
                .map(Assignment::forPeer)
                .collect(Collectors.toSet());

        Assignments assignmentsPlanned = Assignments.of(peers, timestamp, true);

        assertPlannedAssignments(node0, partId, assignmentsPlanned);

        // Wait for the new stable assignments to take effect.
        executeSql(format("ALTER ZONE %s SET (replicas %d)", zoneName, 3));

        waitForZonePartitionState(node0, partId, GlobalPartitionStateEnum.AVAILABLE);

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
    void testTwoPhaseResetEqualLogIndex() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);
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

        CatalogZoneDescriptor zone = node0.catalogManager().activeCatalog(node0.clock().nowLong()).zone(zoneName);
        Collection<String> dataNodes = new HashSet<>();
        for (int i = 0; i < 7; i++) {
            dataNodes.add(node(i).name());
        }

        logger().info("Zone {}", zone);

        Set<Assignment> allAssignmentsSet = calculateAssignmentForPartition(
                dataNodes, partId, zone.partitions(), zone.replicas(), zone.consensusGroupSize());

        Assignments allAssignments = Assignments.of(allAssignmentsSet, timestamp);

        assertStableAssignments(node0, partId, allAssignments);
        // Write data(1) to all seven nodes.
        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        ZonePartitionId partitionGroupId = partitionGroupId(partId);

        // We filter out the leader to be able to reliably block raft state transfer on the other nodes.
        String leaderName = findLeader(1, partId);
        logger().info("Raft group leader is [id={}]", leaderName);

        // All the nodes except the leader - 6 nodes.
        List<String> followerNodes = new ArrayList<>(clusterNodeNames);
        followerNodes.remove(leaderName);

        // The nodes that we block AppendEntriesRequest to.
        Set<String> blockedNodes = ConcurrentHashMap.newKeySet();

        // Exclude one of the nodes from data(2).
        int node0IndexInFollowers = followerNodes.indexOf(node0.name());
        // Make sure node 0 is not stopped. If node0IndexInFollowers==-1, then node0 is the leader.
        String blockedNode = followerNodes.remove(node0IndexInFollowers == -1 ? 0 : node0IndexInFollowers);
        blockedNodes.add(blockedNode);
        logger().info("Blocking updates on nodes [ids={}]", blockedNodes);

        blockMessage(cluster, (nodeName, msg) -> dataReplicateMessage(nodeName, msg, partitionGroupId, blockedNodes));

        // Write data(2) to 6 nodes.
        errors = insertValues(table, partId, 10);
        assertThat(errors, is(empty()));

        assertInsertedValuesOnSpecificNodes(table.name(), followerNodes, partId, 10);

        // Disable scale down.
        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, INFINITE_TIMER_VALUE));

        List<String> nodeNamesToStop = new ArrayList<>(followerNodes);
        // Make sure there are 4 elements in this list.
        nodeNamesToStop.remove(0);
        // Stop them all.
        int[] nodesToStop = nodeNamesToStop.stream()
                .mapToInt(this::nodeIndex)
                .toArray();
        logger().info("Stopping nodes [id={}]", Arrays.toString(nodesToStop));
        stopNodesInParallel(nodesToStop);

        // Two nodes should have the most up to date data, one falls behind.
        waitForZonePartitionState(node0, partId, GlobalPartitionStateEnum.READ_ONLY);

        // Collect nodes that will be the part of the planned assignments.
        List<String> nodesNamesForFinalAssignments = new ArrayList<>(clusterNodeNames);
        nodesNamesForFinalAssignments.removeAll(nodeNamesToStop);

        String pendingNodeName = getPendingNodeName(nodesNamesForFinalAssignments, blockedNode);

        // Given block of assignment stable switch halts rebalances and thus provides an ability to verify pending and planned assignments.
        // Without the block test may fail because rebalance may finish prior to corresponding check. In other words given block eliminates
        // the race between rebalance and assignments verifications that we do below.
        // Worth mentioning that assignments stable switch ignores force flag, thus within blocked assignments force = false is used.
        Assignments assignmentsToBlock = Assignments.of(Set.of(Assignment.forPeer(pendingNodeName)), timestamp);
        blockRebalanceStableSwitch(partId, assignmentsToBlock);

        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<?> updateFuture = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), false, 1);
        assertThat(updateFuture, willCompleteSuccessfully());

        // It's important to unblock appendEntries requests after resetPartitions, otherwise 2 or even all 3 nodes may align by data/index
        // and thus GroupUpdateRequestHandler#nextAssignment may evaluate second or third node as reset first phase target instead of
        // expected within test leaderName = findLeader(1, partId);

        // Unblock raft.
        blockedNodes.clear();

        // Pending is the one with the most up to date log index.
        Assignments assignmentPending = Assignments.forced(Set.of(Assignment.forPeer(pendingNodeName)), timestamp);

        assertPendingAssignments(node0, partId, assignmentPending);

        Set<Assignment> peers = nodesNamesForFinalAssignments.stream()
                .map(Assignment::forPeer)
                .collect(Collectors.toSet());

        Assignments assignmentsPlanned = Assignments.of(peers, timestamp, true);

        assertPlannedAssignments(node0, partId, assignmentsPlanned);

        // Wait for the new stable assignments to take effect.
        executeSql(format("ALTER ZONE %s SET (replicas %d)", zoneName, 3));

        waitForZonePartitionState(node0, partId, GlobalPartitionStateEnum.AVAILABLE);

        // Make sure the data is present.
        IntStream.range(0, ENTRIES).forEach(i -> {
            CompletableFuture<Tuple> fut = table.keyValueView().getAsync(null, Tuple.create(of("id", i)));
            assertThat(fut, willCompleteSuccessfully());

            assertEquals(Tuple.create(of("val", i + 10)), fut.join());
        });
    }

    @Test
    @ZoneParams(nodes = 6, replicas = 3, partitions = 1)
    @MuteFailureManagerLogging
    void testTwoPhaseResetOnEmptyNodes() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);
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

        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<?> updateFuture = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), true, -1);

        assertThat(updateFuture, willCompleteSuccessfully());

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 1, 4);

        Assignments assignmentForced1 = Assignments.forced(Set.of(Assignment.forPeer(node(0).name())), timestamp);

        assertPendingAssignments(node0, partId, assignmentForced1);

        Assignments assignments13 = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(4).name())
        ), timestamp, true);

        assertPlannedAssignments(node0, partId, assignments13);
    }

    @Test
    @ZoneParams(nodes = 7, replicas = 3, partitions = 1, consistencyMode = ConsistencyMode.HIGH_AVAILABILITY)
    void testAssignmentsChainUpdate() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        assertRealAssignments(node0, partId, 0, 2, 3);

        Assignments initialAssignments = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(2).name()),
                Assignment.forPeer(node(3).name())
        ), timestamp);

        assertStableAssignments(node0, partId, initialAssignments);

        assertAssignmentsChain(node0, partId, AssignmentsChain.of(initialAssignments));

        // Write data(1) to all nodes.
        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        logger().info("Stopping nodes [ids={}].", 3);

        stopNode(3);

        logger().info("Stopped nodes [ids={}].", 3);

        Assignments link2Assignments = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(2).name())
        ), timestamp);

        assertRealAssignments(node0, partId, 0, 1, 2);

        assertStableAssignments(node0, partId, link2Assignments, 30_000);

        // Graceful change should reinit the assignments chain, in other words there should be only one link
        // in the chain - the current stable assignments.
        assertAssignmentsChain(node0, partId, AssignmentsChain.of(link2Assignments));

        assertIndexAndTermInLastChainLink(node0, partId);

        // Disable scale down to avoid unwanted rebalance.
        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, INFINITE_TIMER_VALUE));

        // Disable automatic rebalance since we want to restore replica factor.
        setDistributionResetTimeout(node0, INFINITE_TIMER_VALUE);

        // Now stop the majority and the automatic reset should kick in.
        logger().info("Stopping nodes [ids={}].", Arrays.toString(new int[]{1, 2}));

        Assignments resetAssignments = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(4).name()),
                Assignment.forPeer(node(5).name())
        ), timestamp);

        AtomicBoolean blockedLink = new AtomicBoolean(true);

        // Block stable switch to check that we initially add reset phase 1 assignments to the chain.
        blockMessage(
                cluster,
                (nodeName, msg) -> blockedLink.get() && stableKeySwitchMessage(msg, partitionGroupId(partId), resetAssignments)
        );

        stopNodesInParallel(1, 2);

        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();

        CompletableFuture<Void> updateFuture = disasterRecoveryManager.resetPartitions(
                zoneName,
                emptySet(),
                true,
                -1
        );

        assertThat(updateFuture, willCompleteSuccessfully());

        Assignments linkFirstPhaseReset = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name())
        ), timestamp);

        assertStableAssignments(node0, partId, linkFirstPhaseReset, 60_000);

        // Assignments chain consists of stable and the first phase of reset.
        assertAssignmentsChain(node0, partId, AssignmentsChain.of(link2Assignments, linkFirstPhaseReset));

        assertIndexAndTermInLastChainLink(node0, partId);

        // Unblock stable switch, wait for reset phase 2 assignments to replace phase 1 assignments in the chain.
        blockedLink.set(false);

        logger().info("Unblocked stable switch.");

        assertRealAssignments(node0, partId, 0, 4, 5);

        assertStableAssignments(node0, partId, resetAssignments, 60_000);

        // Assignments chain consists of stable and the second phase of reset.
        assertAssignmentsChain(node0, partId, AssignmentsChain.of(link2Assignments, resetAssignments));

        assertIndexAndTermInLastChainLink(node0, partId);
    }

    private void assertIndexAndTermInLastChainLink(IgniteImpl node0, int partId) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

            TableManager tableManager = node0.distributedTableManager();

            RaftGroupConfiguration raftGroupConfiguration = raftGroupConfigurationConverter.fromBytes(
                    tableManager.cachedTable(TABLE_NAME).internalTable().storage().getMvPartition(partId).committedGroupConfiguration()
            );

            long term = raftGroupConfiguration.term();
            long index = raftGroupConfiguration.index();

            Iterator<AssignmentsLink> assignmentsChain = getAssignmentsChain(node0, partId).iterator();
            AssignmentsLink lastLink = null;

            while (assignmentsChain.hasNext()) {
                lastLink = assignmentsChain.next();
            }

            assertNotNull(lastLink);

            return index == lastLink.configurationIndex() && term == lastLink.configurationTerm();
        }, 10_000));
    }

    private void assertConfigurationApplied(IgniteImpl node0, int partId, List<String> peers) {
        await().atMost(10, SECONDS)
                .until(() -> {
                    RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

                    TableManager tableManager = node0.distributedTableManager();

                    RaftGroupConfiguration raftGroupConfiguration = raftGroupConfigurationConverter.fromBytes(
                            tableManager.cachedTable(TABLE_NAME).internalTable().storage().getMvPartition(partId)
                                    .committedGroupConfiguration()
                    );

                    logger().info("Configuration Peers: {}", raftGroupConfiguration.peers());
                    return peers.containsAll(raftGroupConfiguration.peers());
                });
    }

    @Test
    @ZoneParams(nodes = 7, replicas = 7, partitions = 1, consistencyMode = ConsistencyMode.HIGH_AVAILABILITY)
    void testAssignmentsChainUpdatedOnAutomaticReset() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        // Disable scale down to avoid unwanted rebalance.
        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, INFINITE_TIMER_VALUE));

        assertRealAssignments(node0, partId, 0, 1, 2, 3, 4, 5, 6);

        CatalogZoneDescriptor zone = node0.catalogManager().activeCatalog(node0.clock().nowLong()).zone(zoneName);
        Collection<String> dataNodeNames = new HashSet<>();
        for (int i = 0; i < 7; i++) {
            dataNodeNames.add(node(i).name());
        }

        logger().info("Zone {}", zone);

        Set<Assignment> allAssignmentsSet = calculateAssignmentForPartition(
                dataNodeNames, partId, zone.partitions(), zone.replicas(), zone.consensusGroupSize());

        Assignments allAssignments = Assignments.of(allAssignmentsSet, timestamp);

        // Assignments chain is equal to the stable assignments.
        assertAssignmentsChain(node0, partId, AssignmentsChain.of(allAssignments));

        // Write data(1) to all nodes.
        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        assertInsertedValuesOnSpecificNodes(table.name(), dataNodeNames, partId, 0);

        Assignments link2Assignments = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(2).name())
        ), timestamp);

        AtomicBoolean blockedLink2 = new AtomicBoolean(true);

        // Block stable switch to check that we initially add reset phase 1 assignments to the chain.
        blockMessage(
                cluster,
                (nodeName, msg) -> blockedLink2.get() && stableKeySwitchMessage(msg, partitionGroupId(partId), link2Assignments)
        );

        logger().info("Stopping nodes [ids={}].", Arrays.toString(new int[]{3, 4, 5, 6}));

        stopNodesInParallel(3, 4, 5, 6);

        // Wait for first phase of reset to complete.
        // The reset selects the node with the highest raft log index (or lexicographically first on tie).
        Set<String> aliveNodes = Set.of(node(0).name(), node(1).name(), node(2).name());
        await().atMost(60, SECONDS)
                .until(() -> {
                    Assignments stable = getStableAssignments(node0, partId);
                    return stable != null
                            && stable.nodes().size() == 1
                            && aliveNodes.contains(stable.nodes().iterator().next().consistentId());
                });

        // Read the actual stable assignments - this is what the system selected.
        Assignments link2FirstPhaseReset = getStableAssignments(node0, partId);
        String selectedNode = link2FirstPhaseReset.nodes().iterator().next().consistentId();
        logger().info("Reset selected node [name={}].", selectedNode);

        // Assignments chain consists of stable and the first phase of reset.
        assertAssignmentsChain(node0, partId, AssignmentsChain.of(allAssignments, link2FirstPhaseReset));

        // Unblock stable switch, wait for reset phase 2 assignments to replace phase 1 assignments in the chain.
        blockedLink2.set(false);

        assertStableAssignments(node0, partId, link2Assignments, 30_000);

        // Assignments chain consists of stable and the second phase of reset.
        assertAssignmentsChain(node0, partId, AssignmentsChain.of(allAssignments, link2Assignments));

        logger().info("Stopping nodes [ids={}].", Arrays.toString(new int[]{1, 2}));
        stopNodesInParallel(1, 2);

        Assignments link3Assignments = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name())
        ), timestamp);

        assertStableAssignments(node0, partId, link3Assignments, 30_000);

        assertAssignmentsChain(node0, partId, AssignmentsChain.of(allAssignments, link2Assignments, link3Assignments));
    }

    @Test
    @ZoneParams(nodes = 7, replicas = 7, partitions = 1, consistencyMode = ConsistencyMode.HIGH_AVAILABILITY)
    void testSecondResetRewritesUnfinishedFirstPhaseReset() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        // Disable automatic reset since we want to check manual ones.
        setDistributionResetTimeout(node0, INFINITE_TIMER_VALUE);
        // Disable scale down to avoid unwanted rebalance.
        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, INFINITE_TIMER_VALUE));

        assertRealAssignments(node0, partId, 0, 1, 2, 3, 4, 5, 6);

        CatalogZoneDescriptor zone = node0.catalogManager().activeCatalog(node0.clock().nowLong()).zone(zoneName);
        Collection<String> dataNodes = new HashSet<>();
        for (int i = 0; i < 7; i++) {
            dataNodes.add(node(i).name());
        }

        logger().info("Zone {}", zone);

        Set<Assignment> allAssignmentsSet = calculateAssignmentForPartition(
                dataNodes, partId, zone.partitions(), zone.replicas(), zone.consensusGroupSize());

        Assignments allAssignments = Assignments.of(allAssignmentsSet, timestamp);

        assertStableAssignments(node0, partId, allAssignments);

        assertAssignmentsChain(node0, partId, AssignmentsChain.of(allAssignments));

        // Write data(1) to all seven nodes.
        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        assertInsertedValuesOnSpecificNodes(table.name(), dataNodes, partId, 0);

        Assignments blockedRebalance = Assignments.of(timestamp,
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(2).name())
        );

        blockRebalanceStableSwitch(partId, blockedRebalance);

        logger().info("Stopping nodes [ids={}].", Arrays.toString(new int[]{3, 4, 5, 6}));
        stopNodesInParallel(3, 4, 5, 6);

        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<?> updateFuture = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), true, -1);

        assertThat(updateFuture, willCompleteSuccessfully());

        // First phase of reset. The second phase stable switch is blocked.
        // Wait for stable assignments to contain exactly one node from the alive set.
        // The reset selects the node with the highest raft log index (or lexicographically first on tie).
        Set<String> aliveNodes = Set.of(node(0).name(), node(1).name(), node(2).name());
        await().atMost(30, SECONDS)
                .until(() -> {
                    Assignments stable = getStableAssignments(node0, partId);
                    return stable != null
                            && stable.nodes().size() == 1
                            && aliveNodes.contains(stable.nodes().iterator().next().consistentId());
                });

        // Read the actual stable assignments - this is what the system selected.
        Assignments link2Assignments = getStableAssignments(node0, partId);
        String selectedNode = link2Assignments.nodes().iterator().next().consistentId();
        logger().info("Reset selected node [name={}].", selectedNode);

        assertAssignmentsChain(node0, partId, AssignmentsChain.of(allAssignments, link2Assignments));

        Assignments assignmentsPending = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(2).name())
        ), timestamp, true);

        assertPendingAssignments(node0, partId, assignmentsPending);

        logger().info("Stopping nodes [ids={}].", Arrays.toString(new int[]{2}));
        stopNode(2);

        CompletableFuture<?> updateFuture2 = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), true, -1);

        assertThat(updateFuture2, willCompleteSuccessfully());

        Assignments link3Assignments = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name())
        ), timestamp);

        assertStableAssignments(node0, partId, link3Assignments, 30_000);

        assertAssignmentsChain(node0, partId, AssignmentsChain.of(allAssignments, link2Assignments, link3Assignments));
    }

    @Test
    @ZoneParams(nodes = 6, replicas = 3, partitions = 1, consistencyMode = ConsistencyMode.HIGH_AVAILABILITY)
    void testGracefulRewritesChainAfterForceReset() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);
        int catalogVersion = node0.catalogManager().latestCatalogVersion();
        long timestamp = node0.catalogManager().catalog(catalogVersion).time();
        Table table = node0.tables().table(TABLE_NAME);

        awaitPrimaryReplica(node0, partId);

        // Disable automatic reset since we want to check manual ones.
        setDistributionResetTimeout(node0, INFINITE_TIMER_VALUE);
        // Disable scale down to avoid unwanted rebalance.
        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, INFINITE_TIMER_VALUE));

        assertRealAssignments(node0, partId, 2, 3, 5);

        Assignments initialAssignments = Assignments.of(Set.of(
                Assignment.forPeer(node(2).name()),
                Assignment.forPeer(node(3).name()),
                Assignment.forPeer(node(5).name())
        ), timestamp);

        assertStableAssignments(node0, partId, initialAssignments);

        assertAssignmentsChain(node0, partId, AssignmentsChain.of(initialAssignments));

        // Write data(1) to all nodes.
        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        logger().info("Stopping nodes [ids={}].", Arrays.toString(new int[]{2, 3}));

        stopNodesInParallel(2, 3);

        logger().info("Stopped nodes [ids={}].", Arrays.toString(new int[]{2, 3}));

        DisasterRecoveryManager disasterRecoveryManager = node0.disasterRecoveryManager();
        CompletableFuture<?> updateFuture2 = disasterRecoveryManager.resetPartitions(zoneName, emptySet(), true, -1);

        assertThat(updateFuture2, willCompleteSuccessfully());

        Assignments link2Assignments = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(1).name()),
                Assignment.forPeer(node(5).name())
        ), timestamp);

        assertRealAssignments(node0, partId, 0, 1, 5);

        assertStableAssignments(node0, partId, link2Assignments, 30_000);

        assertAssignmentsChain(node0, partId, AssignmentsChain.of(initialAssignments, link2Assignments));

        // Return back scale down.
        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", zoneName, 1));

        // Now stop one and check graceful rebalance.
        logger().info("Stopping nodes [ids={}].", 1);

        stopNode(1);

        Assignments finalAssignments = Assignments.of(Set.of(
                Assignment.forPeer(node(0).name()),
                Assignment.forPeer(node(4).name()),
                Assignment.forPeer(node(5).name())
        ), timestamp);

        assertRealAssignments(node0, partId, 0, 4, 5);

        assertStableAssignments(node0, partId, finalAssignments, 30_000);

        // Graceful change should reinit the assignments chain, in other words there should be only one link
        // in the chain - the current stable assignments.
        assertAssignmentsChain(node0, partId, AssignmentsChain.of(finalAssignments));
    }

    /**
     * Lease agreement message should be the first message sent after reset.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-25644")
    @ZoneParams(nodes = 3, replicas = 3, partitions = 1)
    void testLeaseResendOnManualReset() throws Exception {
        int partId = 0;

        IgniteImpl node0 = igniteImpl(0);
        Table table = node0.tables().table(TABLE_NAME);

        ReplicaMeta primaryBeforeReset = awaitPrimaryReplica(node0, partId);
        logger().info("Primary replica before reset is {}", primaryBeforeReset);

        assertRealAssignments(node0, partId, 0, 1, 2);

        List<Throwable> errors = insertValues(table, partId, 0);
        assertThat(errors, is(empty()));

        stopNodesInParallel(1, 2);

        // Reset the partition to make it operable.
        CompletableFuture<?> resetFuture = node0.disasterRecoveryManager().resetPartitions(
                zoneName,
                emptySet(),
                true,
                -1
        );

        assertThat(resetFuture, willCompleteSuccessfully());

        ReplicationConfiguration config = node0
                .clusterConfiguration()
                .getConfiguration(ReplicationExtensionConfiguration.KEY)
                .replication();
        // Should be a new lease after the reset.
        assertTrue(waitForCondition(() -> {
            try {
                ReplicaMeta primaryAfterReset = awaitPrimaryReplica(node0, partId);
                // We expect that a new lease is issued for the new stable.
                return primaryAfterReset.getStartTime().compareTo(primaryBeforeReset.getStartTime()) > 0;
            } catch (ExecutionException | InterruptedException e) {
                fail(e);
            }
            return false;
        }, config.leaseExpirationIntervalMillis().value() * 2));
    }

    private static void setDistributionResetTimeout(IgniteImpl node, long timeout) {
        CompletableFuture<Void> changeFuture = node
                .clusterConfiguration()
                .getConfiguration(SystemDistributedExtensionConfiguration.KEY)
                .system().change(c0 -> c0.changeProperties()
                        .createOrUpdate(PARTITION_DISTRIBUTION_RESET_TIMEOUT,
                                c1 -> c1.changePropertyValue(String.valueOf(timeout)))
                );

        assertThat(changeFuture, willCompleteSuccessfully());
    }

    private static String getPendingNodeName(List<String> aliveNodes, String blockedNode) {
        List<String> candidates = new ArrayList<>(aliveNodes);
        candidates.remove(blockedNode);

        // Without the blocking node, the other two should have the same raft log index.
        // Pick the one with the name
        candidates.sort(String::compareTo);

        return candidates.get(0);
    }

    private static boolean dataReplicateMessage(
            String nodeName,
            NetworkMessage msg,
            ZonePartitionId partitionGroupId,
            Set<String> blockedNodes
    ) {
        if (msg instanceof AppendEntriesRequest) {
            var appendEntriesRequest = (AppendEntriesRequest) msg;
            if (partitionGroupId.toString().equals(appendEntriesRequest.groupId())) {
                return blockedNodes.contains(nodeName);
            }
        }
        return false;
    }

    /**
     * Block rebalance, so stable won't be switched to specified pending.
     */
    private void blockRebalanceStableSwitch(int partId, Assignments assignment) {
        blockMessage(cluster, (nodeName, msg) -> stableKeySwitchMessage(msg, partitionGroupId(partId), assignment));
    }

    private void waitForZonePartitionState(IgniteImpl node0, int partId, GlobalPartitionStateEnum expectedState)
            throws InterruptedException {
        AtomicReference<GlobalPartitionState> partitionState = new AtomicReference<>(null);

        assertTrue(waitForCondition(() -> {
            CompletableFuture<Map<ZonePartitionId, GlobalPartitionState>> statesFuture = node0.disasterRecoveryManager()
                    .globalPartitionStates(Set.of(zoneName), emptySet());

            assertThat(statesFuture, willCompleteSuccessfully());

            Map<ZonePartitionId, GlobalPartitionState> map = statesFuture.join();

            GlobalPartitionState state = map.get(new ZonePartitionId(zoneId, partId));

            partitionState.set(state);

            return state != null && state.state == expectedState;
        }, 500, 20_000),
                () -> "Expected state: " + expectedState + ", actual: " + partitionState.get()
        );
    }

    private void assertZoneLocalState(IgniteImpl node0, int partId, LocalPartitionStateEnum state) {
        var localStatesFut = node0.disasterRecoveryManager().localPartitionStates(emptySet(), Set.of(node(3).name()), emptySet());
        assertThat(localStatesFut, willCompleteSuccessfully());

        Map<ZonePartitionId, LocalPartitionStateByNode> localStates = localStatesFut.join();
        assertThat(localStates, is(not(anEmptyMap())));
        LocalPartitionStateByNode localPartitionStateByNode = localStates.get(new ZonePartitionId(zoneId, partId));

        assertEquals(state, localPartitionStateByNode.values().iterator().next().state);
    }

    private String findLeader(int nodeIdx, int partId) {
        IgniteImpl node = igniteImpl(nodeIdx);

        var raftNodeId = new RaftNodeId(partitionGroupId(partId), new Peer(node.name()));
        var jraftServer = (JraftServerImpl) node.raftManager().server();

        RaftGroupService raftGroupService = jraftServer.raftGroupService(raftNodeId);
        assertNotNull(raftGroupService);
        return raftGroupService.getRaftNode().getLeaderId().getConsistentId();
    }

    private NodeImpl getRaftNode(int nodeIdx, int partId) {
        IgniteImpl node = igniteImpl(nodeIdx);

        var raftNodeId = new RaftNodeId(partitionGroupId(partId), new Peer(node.name()));
        var jraftServer = (JraftServerImpl) node.raftManager().server();

        RaftGroupService raftGroupService = jraftServer.raftGroupService(raftNodeId);
        assertNotNull(raftGroupService);

        return (NodeImpl) raftGroupService.getRaftNode();
    }

    private LogId getRaftLogIndex(int nodeIdx, int partId) {
        return getRaftNode(nodeIdx, partId).lastLogIndexAndTerm();
    }

    private void triggerRaftSnapshot(int nodeIdx, int partId, boolean forced) throws InterruptedException, ExecutionException {
        IgniteImpl node = igniteImpl(nodeIdx);

        var raftNodeId = new RaftNodeId(partitionGroupId(partId), new Peer(node.name()));
        var jraftServer = (JraftServerImpl) node.raftManager().server();

        RaftGroupService raftGroupService = jraftServer.raftGroupService(raftNodeId);
        assertNotNull(raftGroupService);

        CompletableFuture<Status> fut = new CompletableFuture<>();
        raftGroupService.getRaftNode().snapshot(fut::complete, forced);

        assertThat(fut, willCompleteSuccessfully());
        assertEquals(RaftError.SUCCESS, fut.get().getRaftError());
    }

    private ZonePartitionId partitionGroupId(int partId) {
        return new ZonePartitionId(zoneId, partId);
    }

    private ReplicaMeta awaitPrimaryReplica(IgniteImpl node0, int partId) throws ExecutionException, InterruptedException {
        CompletableFuture<ReplicaMeta> awaitPrimaryReplicaFuture = node0.placementDriver()
                .awaitPrimaryReplica(partitionGroupId(partId), node0.clock().now(), 60, SECONDS);

        assertThat(awaitPrimaryReplicaFuture, willSucceedIn(60, SECONDS));

        return awaitPrimaryReplicaFuture.get();
    }

    private void assertRealAssignments(IgniteImpl node0, int partId, Integer... expected) throws InterruptedException {
        assertTrue(
                waitForCondition(() -> List.of(expected).equals(getRealAssignments(node0, partId)), 5000),
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
        assertPlannedAssignments(node0, partId, expected, 2000);
    }

    private void assertPlannedAssignments(IgniteImpl node0, int partId, Assignments expected, long timeout) throws InterruptedException {
        assertTrue(
                waitForCondition(() -> expected.equals(getPlannedAssignments(node0, partId)), timeout),
                () -> "Expected: " + expected + ", actual: " + getPlannedAssignments(node0, partId)
        );
    }

    private void assertStableAssignments(IgniteImpl node0, int partId, Assignments expected) throws InterruptedException {
        assertStableAssignments(node0, partId, expected, 2000);
    }

    private void assertStableAssignments(IgniteImpl node0, int partId, Assignments expected, long timeoutMillis)
            throws InterruptedException {
        assertTrue(
                waitForCondition(() -> expected.equals(getStableAssignments(node0, partId)), timeoutMillis),
                () -> "Expected: " + expected + ", actual: " + getStableAssignments(node0, partId)
        );
    }

    private void assertAssignmentsChain(IgniteImpl node0, int partId, @Nullable AssignmentsChain expected) throws InterruptedException {
        assertTrue(
                waitForCondition(() -> compareAssignmentsChainInTests(expected, getAssignmentsChain(node0, partId)), 2000),
                () -> "Expected: " + expected + ", actual: " + getAssignmentsChain(node0, partId)
        );
    }

    private static boolean compareAssignmentsChainInTests(@Nullable AssignmentsChain expected, @Nullable AssignmentsChain actual) {
        if (expected == null || actual == null) {
            return expected == actual;
        }

        boolean same = true;

        Iterator<AssignmentsLink> expectedIter = expected.iterator();

        Iterator<AssignmentsLink> actualIter = actual.iterator();

        while (expectedIter.hasNext()) {
            if (!actualIter.hasNext() || !compareAssignmentLinks(expectedIter.next(), actualIter.next())) {
                same = false;
                break;
            }
        }

        return same;
    }

    /**
     * In tests, we do not use equals() method of AssignmentsLink, because we do not care about index and term.
     */
    private static boolean compareAssignmentLinks(AssignmentsLink expected, AssignmentsLink actual) {
        return Objects.equals(expected.assignments(), actual.assignments());
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
            if ((unwrapTableImpl(table)).partitionId(key) != partitionId) {
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
        return ExceptionUtils.extractCodeFrom(cause) == Replicator.REPLICA_MISS_ERR;
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
            HybridTimestamp timestamp = node.metaStorageManager().timestampByRevisionLocally(causalityToken);

            int catalogVersion = node.catalogManager().latestCatalogVersion();

            CompletableFuture<Set<String>> dataNodes = dzManager.dataNodes(timestamp, catalogVersion, zoneId);

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
        Set<String> nodeNames =
                TestDisasterRecoveryUtils.getRealAssignments(node0.disasterRecoveryManager(), zoneName, zoneId, partId);

        return nodeNames
                .stream()
                .map(cluster::nodeIndex)
                .sorted()
                .collect(Collectors.toList());
    }

    private @Nullable Assignments getPlannedAssignments(IgniteImpl node, int partId) {
        CompletableFuture<Entry> plannedFut = node.metaStorageManager()
                .get(plannedPartAssignmentsKey(partitionGroupId(partId)));

        assertThat(plannedFut, willCompleteSuccessfully());

        Entry planned = plannedFut.join();

        return planned.empty() ? null : Assignments.fromBytes(planned.value());
    }

    private @Nullable Assignments getPendingAssignments(IgniteImpl node, int partId) {
        CompletableFuture<Entry> pendingFut = node.metaStorageManager()
                .get(pendingPartAssignmentsQueueKey(partitionGroupId(partId)));

        assertThat(pendingFut, willCompleteSuccessfully());

        Entry pending = pendingFut.join();

        logger().info("Pending is {}", pending);

        // TODO Remove pending.value() == null check https://issues.apache.org/jira/browse/IGNITE-25479
        return pending.empty() || pending.value() == null ? null : AssignmentsQueue.fromBytes(pending.value()).poll();
    }

    private @Nullable Assignments getStableAssignments(IgniteImpl node, int partId) {
        CompletableFuture<Entry> stableFut = node.metaStorageManager()
                .get(stablePartAssignmentsKey(partitionGroupId(partId)));

        assertThat(stableFut, willCompleteSuccessfully());

        Entry stable = stableFut.join();

        return stable.empty() ? null : Assignments.fromBytes(stable.value());
    }

    private @Nullable AssignmentsChain getAssignmentsChain(IgniteImpl node, int partId) {
        CompletableFuture<Entry> chainFut;

        chainFut = node.metaStorageManager()
                .get(ZoneRebalanceUtil.assignmentsChainKey(new ZonePartitionId(zoneId, partId)));

        assertThat(chainFut, willCompleteSuccessfully());

        Entry chain = chainFut.join();

        return chain.empty() ? null : AssignmentsChain.fromBytes(chain.value());
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface ZoneParams {
        int replicas();

        int partitions();

        int nodes() default INITIAL_NODES;

        ConsistencyMode consistencyMode() default ConsistencyMode.STRONG_CONSISTENCY;
    }

    private void assertInsertedValuesOnSpecificNodes(
            String tableName,
            Collection<String> nodesNames,
            int partitionId,
            int offset
    ) throws Exception {
        Set<IgniteImpl> nodes = cluster.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .filter(node -> nodesNames.contains(node.name()))
                .collect(Collectors.toSet());

        for (IgniteImpl node : nodes) {
            Table table = node.tables().table(tableName);

            for (int i = 0, created = 0; created < ENTRIES; i++) {
                Tuple key = Tuple.create(of("id", i));
                if ((unwrapTableImpl(table)).partitionId(key) != partitionId) {
                    continue;
                }

                //noinspection AssignmentToForLoopParameter
                created++;

                assertValueOnSpecificNode(tableName, node, i, i + offset, SCHEMA);
            }
        }
    }
}
