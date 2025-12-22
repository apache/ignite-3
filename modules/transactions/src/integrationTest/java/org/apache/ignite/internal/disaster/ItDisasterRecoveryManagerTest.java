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

import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.ClusterPerClassIntegrationTest.awaitPartitionsToBeHealthy;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.disaster.DisasterRecoveryTestUtil.blockMessage;
import static org.apache.ignite.internal.disaster.DisasterRecoveryTestUtil.stableKeySwitchMessage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.alterZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateByNode;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** For {@link DisasterRecoveryManager} integration testing. */
// TODO https://issues.apache.org/jira/browse/IGNITE-22332 Add test cases.
public class ItDisasterRecoveryManagerTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    public static final String TABLE_NAME = "TEST_TABLE";

    private static final String ZONE_NAME = "ZONE_" + TABLE_NAME;

    private static final int INITIAL_NODES = 1;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("id", NativeTypes.INT32, false)},
            new Column[]{
                    new Column("valInt", NativeTypes.INT32, false),
            }
    );

    @Override
    protected int initialNodes() {
        return INITIAL_NODES;
    }

    @BeforeEach
    void setUp(TestInfo testInfo) {
        Method testMethod = testInfo.getTestMethod().orElseThrow();

        ZoneParams zoneParams = testMethod.getAnnotation(ZoneParams.class);

        if (zoneParams != null) {
            IntStream.range(INITIAL_NODES, zoneParams.nodes()).forEach(i -> cluster.startNode(i));
        }

        int replicas = zoneParams != null ? zoneParams.replicas() : initialNodes();

        int partitions = zoneParams != null ? zoneParams.partitions() : initialNodes();

        executeSql(String.format(
                "CREATE ZONE %s (REPLICAS %s, PARTITIONS %s) STORAGE PROFILES ['%s']",
                ZONE_NAME, replicas, partitions, DEFAULT_STORAGE_PROFILE
        ));

        executeSql(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, valInt INT) ZONE %s",
                TABLE_NAME,
                ZONE_NAME
        ));
    }

    private static String findPrimaryNodeName(IgniteImpl ignite, ZonePartitionId replicationGroupId) {
        assertThat(awaitPrimaryReplicaForNow(ignite, replicationGroupId), willCompleteSuccessfully());

        CompletableFuture<ReplicaMeta> primary = ignite.placementDriver().getPrimaryReplica(replicationGroupId, ignite.clock().now());

        assertThat(primary, willCompleteSuccessfully());

        return primary.join().getLeaseholder();
    }

    private Ignite findPrimaryIgniteNode(IgniteImpl ignite, ZonePartitionId replicationGroupId) {
        return cluster.runningNodes()
                .filter(node -> node.name().equals(findPrimaryNodeName(ignite, replicationGroupId)))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No node found that is a primary replica for the specified options."));
    }

    @Test
    void testRestartZonePartitions() {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        int partitionId = 0;

        CompletableFuture<Void> restartPartitionsFuture = node.disasterRecoveryManager().restartPartitions(
                Set.of(node.name()),
                ZONE_NAME,
                Set.of(partitionId)
        );

        assertThat(restartPartitionsFuture, willCompleteSuccessfully());
        assertThat(awaitPrimaryReplicaForNow(node, new ZonePartitionId(zoneId(node), partitionId)), willCompleteSuccessfully());

        insert(2, 2);
        insert(3, 3);

        assertThat(selectAll(), hasSize(4));
    }

    @Test
    @ZoneParams(nodes = 2, replicas = 1, partitions = 2)
    void testEstimatedRowsZone() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        // Wait for replication to finish.
        assertTrue(waitForCondition(() -> {
                    CompletableFuture<Map<ZonePartitionId, LocalPartitionStateByNode>> localStateFuture =
                            node.disasterRecoveryManager().localPartitionStates(Set.of(ZONE_NAME), emptySet(), emptySet());

                    assertThat(localStateFuture, willCompleteSuccessfully());

                    Map<ZonePartitionId, LocalPartitionStateByNode> localState;
                    try {
                        localState = localStateFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }

                    Set<Long> size = localState.values().stream()
                            .flatMap(localPartitionStateByNode -> localPartitionStateByNode.values().stream())
                            .map(state -> state.estimatedRows)
                            .collect(Collectors.toSet());
                    // There are 2 nodes, 2 partitions and 1 replica, so we should have 2 entries in localState (one for each partition),
                    // LocalTablePartitionStateByNode should have a entry for either the first or the second node with 1 row.
                    return size.size() == 1 && size.contains(1L) && localState.size() == 2;
                },
                20_000
        ));
    }

    @Test
    @ZoneParams(nodes = 2, replicas = 2, partitions = 2)
    void testLocalPartitionStateZone() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        CompletableFuture<Map<ZonePartitionId, LocalPartitionStateByNode>> localStateTableFuture =
                node.disasterRecoveryManager().localPartitionStates(emptySet(), emptySet(), emptySet());

        assertThat(localStateTableFuture, willCompleteSuccessfully());
        Map<ZonePartitionId, LocalPartitionStateByNode> localState = localStateTableFuture.get();

        // A  custom zone, which was created in `BeforeEach` with 2 partitions due to this test's `ZoneParam` annotation's parameter.
        assertThat(localState, aMapWithSize(2));

        int zoneId = zoneId(node);

        // Partitions size is 2.
        for (int partitionId = 0; partitionId < 2; partitionId++) {
            LocalPartitionStateByNode partitionStateByNode = localState.get(new ZonePartitionId(zoneId, partitionId));
            // 2 nodes.
            assertThat(partitionStateByNode.values(), hasSize(2));

            for (LocalPartitionState state : partitionStateByNode.values()) {
                assertThat(state.zoneId, is(zoneId));
                assertThat(state.zoneName, is(ZONE_NAME));
                assertThat(state.partitionId, is(partitionId));
                assertThat(state.state, is(LocalPartitionStateEnum.HEALTHY));
            }
        }
    }

    @Test
    @ZoneParams(nodes = 2, replicas = 2, partitions = 2)
    void testGlobalPartitionStateZone() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        CompletableFuture<Map<ZonePartitionId, GlobalPartitionState>> globalStatesFuture =
                node.disasterRecoveryManager().globalPartitionStates(emptySet(), emptySet());

        assertThat(globalStatesFuture, willCompleteSuccessfully());
        Map<ZonePartitionId, GlobalPartitionState> globalState = globalStatesFuture.get();

        // A  custom zone, which was created in `BeforeEach` with 2 partitions due to this test's `ZoneParam` annotation's parameter.
        assertThat(globalState, aMapWithSize(2));

        int zoneId = zoneId(node);

        // Partitions size is 2.
        for (int partitionId = 0; partitionId < 2; partitionId++) {
            GlobalPartitionState state = globalState.get(new ZonePartitionId(zoneId, partitionId));
            assertThat(state.zoneId, is(zoneId));
            assertThat(state.zoneName, is(ZONE_NAME));
            assertThat(state.partitionId, is(partitionId));
            assertThat(state.state, is(GlobalPartitionStateEnum.AVAILABLE));
        }
    }

    private void insert(int id, int val) {
        insert(id, val, TABLE_NAME);
    }

    private void insert(int id, int val, String tableName) {
        executeSql(String.format(
                "INSERT INTO %s (id, valInt) VALUES (%s, %s)",
                tableName, id, val
        ));
    }

    private List<List<Object>> selectAll() {
        return selectAll(TABLE_NAME);
    }

    private List<List<Object>> selectAll(String tableName) {
        return executeSql(String.format(
                "SELECT * FROM %s",
                tableName
        ));
    }

    private static int tableId(IgniteImpl node) {
        return tableId(node, TABLE_NAME);
    }

    private static int tableId(IgniteImpl node, String tableName) {
        return ((Wrapper) node.tables().table(tableName)).unwrap(TableImpl.class).tableId();
    }

    private static int zoneId(CatalogManager catalogManager, String zoneName) {
        return catalogManager.catalog(catalogManager.latestCatalogVersion()).zone(zoneName).id();
    }

    private static int zoneId(IgniteImpl node) {
        return node.catalogManager().catalog(node.catalogManager().latestCatalogVersion()).zone(ZONE_NAME).id();
    }

    private IgniteImpl findZoneNodeConformingOptions(String testZone, boolean primaryReplica, boolean raftLeader)
            throws InterruptedException {
        Ignite nodeToCleanup;
        IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());
        ZonePartitionId replicationGroupId = new ZonePartitionId(zoneId(ignite.catalogManager(), testZone), 0);
        String primaryNodeName = findPrimaryNodeName(ignite, replicationGroupId);
        String raftLeaderNodeName = cluster.leaderServiceFor(replicationGroupId).getServerId().getConsistentId();

        if (primaryReplica) {
            nodeToCleanup = findPrimaryIgniteNode(ignite, replicationGroupId);
        } else if (raftLeader) {
            nodeToCleanup = cluster.runningNodes()
                    .filter(node -> node.name().equals(raftLeaderNodeName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No node found that is a raft leader for the specified options."));
        } else {
            nodeToCleanup = cluster.runningNodes()
                    .filter(node -> !node.name().equals(raftLeaderNodeName) && !node.name().equals(primaryNodeName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No node found that is not a primary replica and not a raft leader."));
        }

        return unwrapIgniteImpl(nodeToCleanup);
    }

    private static CompletableFuture<ReplicaMeta> awaitPrimaryReplicaForNow(IgniteImpl node, ZonePartitionId replicationGroupId) {
        return node.placementDriver().awaitPrimaryReplica(replicationGroupId, node.clock().now(), 60, SECONDS);
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface ZoneParams {
        int replicas() default INITIAL_NODES;

        int partitions() default INITIAL_NODES;

        int nodes() default INITIAL_NODES;
    }

    private static void assertValueOnSpecificNodes(String tableName, Set<IgniteImpl> nodes, int id, int val) throws Exception {
        DisasterRecoveryTestUtil.assertValueOnSpecificNodes(tableName, nodes, id, val, SCHEMA);
    }

    @Test
    void testRestartPartitionsWithCleanUpFails() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        Set<IgniteImpl> runningNodes = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(Collectors.toSet());

        assertValueOnSpecificNodes(TABLE_NAME, runningNodes, 0, 0);
        assertValueOnSpecificNodes(TABLE_NAME, runningNodes, 1, 1);

        int partitionId = 0;

        CompletableFuture<Void> restartPartitionsWithCleanupFuture = node.disasterRecoveryManager().restartPartitionsWithCleanup(
                Set.of(node.name()),
                ZONE_NAME,
                Set.of(partitionId)
        );

        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> restartPartitionsWithCleanupFuture.get(10_000, MILLISECONDS)
        );

        assertInstanceOf(DisasterRecoveryException.class, exception.getCause());

        assertThat(exception.getCause().getMessage(), is("Not enough alive nodes to perform reset with clean up."));
    }

    @Test
    void testRestartHaPartitionsWithCleanUpFails() {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        String testZone = "TEST_ZONE";

        createZone(node.catalogManager(), testZone, 1, 1, null, null, ConsistencyMode.HIGH_AVAILABILITY);

        String tableName = "TABLE_NAME";

        node.sql().executeScript(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, valInt INT) ZONE %s",
                tableName,
                testZone
        ));

        int partitionId = 0;

        CompletableFuture<Void> restartPartitionsWithCleanupFuture = node.disasterRecoveryManager().restartPartitionsWithCleanup(
                Set.of(node.name()),
                testZone,
                Set.of(partitionId)
        );

        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> restartPartitionsWithCleanupFuture.get(10_000, MILLISECONDS)
        );

        assertInstanceOf(DisasterRecoveryException.class, exception.getCause());

        assertThat(exception.getCause().getMessage(), is("Not enough alive nodes to perform reset with clean up."));
    }

    @ParameterizedTest(name = "consistencyMode={0}, primaryReplica={1}, raftLeader={2}")
    @CsvSource({
            "STRONG_CONSISTENCY, true, false",
            "STRONG_CONSISTENCY, false, true",
            "STRONG_CONSISTENCY, false, false",
            "HIGH_AVAILABILITY, true, false",
            "HIGH_AVAILABILITY, false, true",
            "HIGH_AVAILABILITY, false, false"
    })
    void testRestartPartitionsWithCleanUp(
            ConsistencyMode consistencyMode,
            boolean primaryReplica,
            boolean raftLeader
    ) throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());
        cluster.startNode(1);

        String testZone = "TEST_ZONE";

        if (consistencyMode == ConsistencyMode.HIGH_AVAILABILITY) {
            createZone(node.catalogManager(), testZone, 1, 2, null, null, ConsistencyMode.HIGH_AVAILABILITY);
        } else {
            cluster.startNode(2);

            createZone(node.catalogManager(), testZone, 1, 3);
        }

        Set<IgniteImpl> runningNodes = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(Collectors.toSet());

        String tableName = "TABLE_NAME";

        node.sql().executeScript(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, valInt INT) ZONE %s",
                tableName,
                testZone
        ));

        insert(0, 0, tableName);

        assertValueOnSpecificNodes(tableName, runningNodes, 0, 0);

        IgniteImpl nodeToCleanup = findZoneNodeConformingOptions(testZone, primaryReplica, raftLeader);

        CompletableFuture<Void> restartPartitionsWithCleanupFuture = nodeToCleanup.disasterRecoveryManager().restartPartitionsWithCleanup(
                Set.of(nodeToCleanup.name()),
                testZone,
                Set.of(0)
        );

        assertThat(restartPartitionsWithCleanupFuture, willCompleteSuccessfully());

        insert(1, 1, tableName);

        assertValueOnSpecificNodes(tableName, runningNodes, 0, 0);

        assertValueOnSpecificNodes(tableName, runningNodes, 1, 1);
    }

    @ParameterizedTest(name = "consistencyMode={0}, primaryReplica={1}")
    @CsvSource({
            "STRONG_CONSISTENCY, true",
            "STRONG_CONSISTENCY, false",
            "HIGH_AVAILABILITY, true",
            "HIGH_AVAILABILITY, false",
    })
    void testRestartPartitionsWithCleanUpTxRollback(ConsistencyMode consistencyMode, boolean primaryReplica) throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        cluster.startNode(1);

        String testZone = "TEST_ZONE";

        if (consistencyMode == ConsistencyMode.HIGH_AVAILABILITY) {
            createZone(node.catalogManager(), testZone, 1, 2, null, null, ConsistencyMode.HIGH_AVAILABILITY);
        } else {
            cluster.startNode(2);

            createZone(node.catalogManager(), testZone, 1, 3);
        }

        Set<IgniteImpl> runningNodes = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(Collectors.toSet());

        String tableName = "TABLE_NAME";

        node.sql().executeScript(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, valInt INT) ZONE %s",
                tableName,
                testZone
        ));

        insert(0, 0, tableName);

        assertValueOnSpecificNodes(tableName, runningNodes, 0, 0);

        IgniteImpl primaryNode =
                unwrapIgniteImpl(findPrimaryIgniteNode(node, new ZonePartitionId(zoneId(node.catalogManager(), testZone), 0)));

        IgniteImpl nodeToCleanup;

        if (primaryReplica) {
            nodeToCleanup = primaryNode;
        } else {
            nodeToCleanup = cluster.runningNodes()
                    .filter(n -> !n.name().equals(primaryNode.name()))
                    .map(TestWrappers::unwrapIgniteImpl)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No node found that is not a primary replica."));
        }

        Transaction tx = nodeToCleanup.transactions().begin();

        nodeToCleanup.sql().execute(tx, String.format("INSERT INTO %s VALUES (2, 2)", tableName));

        CompletableFuture<Void> restartPartitionsWithCleanupFuture =
                nodeToCleanup.disasterRecoveryManager().restartPartitionsWithCleanup(
                        Set.of(nodeToCleanup.name()),
                        testZone,
                        Set.of(0)
                );

        assertThat(restartPartitionsWithCleanupFuture, willCompleteSuccessfully());

        if (primaryReplica) {
            // We expect here that tx will be rolled back because we have restarted primary replica. This is ensured by the fact that we
            // use ReplicaManager.weakStopReplica(RESTART) in restartPartitionsWithCleanup, and this mechanism
            // waits for replica expiration and stops lease prolongation. As a result, the transaction will not be able to commit
            // because the primary replica has expired.
            assertThrows(TransactionException.class, tx::commit, "Primary replica has expired, transaction will be rolled back");

            assertValueOnSpecificNodes(tableName, runningNodes, 0, 0);

            for (IgniteImpl igniteImpl : runningNodes) {
                assertEquals(1L, igniteImpl.sql().execute(null, "SELECT count(*) as cnt FROM TABLE_NAME").next().longValue("cnt"));
            }
        } else {
            tx.commit();

            assertValueOnSpecificNodes(tableName, runningNodes, 0, 0);
            assertValueOnSpecificNodes(tableName, runningNodes, 2, 2);

            for (IgniteImpl igniteImpl : runningNodes) {
                assertEquals(2L, igniteImpl.sql().execute(null, "SELECT count(*) as cnt FROM TABLE_NAME").next().longValue("cnt"));
            }
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-27268")
    void testRestartPartitionsWithCleanUpConcurrentRebalance() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        cluster.startNode(1);
        cluster.startNode(2);
        cluster.startNode(3);

        String testZone = "TEST_ZONE";

        createZone(node.catalogManager(), testZone, 1, 4);

        Set<IgniteImpl> runningNodes = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(Collectors.toSet());

        assertEquals(4, runningNodes.size(), "Expected 4 running nodes after zone alteration");

        awaitPartitionsToBeHealthy(cluster, testZone, Set.of());

        String tableName = "TABLE_NAME";

        node.sql().executeScript(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, valInt INT) ZONE %s",
                tableName,
                testZone
        ));

        insert(0, 0, tableName);

        assertValueOnSpecificNodes(tableName, runningNodes, 0, 0);

        IgniteImpl node4 = unwrapIgniteImpl(cluster.startNode(4));

        runningNodes = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(Collectors.toSet());

        assertEquals(5, runningNodes.size(), "Expected 5 running nodes 5th node started");

        int catalogVersion = node.catalogManager().latestCatalogVersion();

        long timestamp = node.catalogManager().catalog(catalogVersion).time();

        CatalogZoneDescriptor zoneDescriptor = node.catalogManager().catalog(catalogVersion).zone(testZone);

        Set<Assignment> calculatedAssignments = calculateAssignmentForPartition(
                runningNodes.stream().map(IgniteImpl::name).collect(Collectors.toSet()),
                0,
                zoneDescriptor.partitions(),
                5,
                zoneDescriptor.consensusGroupSize()
        );

        Assignments assignmentsPending = Assignments.of(calculatedAssignments, timestamp);

        ZonePartitionId replicationGroupId = new ZonePartitionId(zoneId(node.catalogManager(), testZone), 0);

        AtomicBoolean blocked = new AtomicBoolean(true);

        AtomicBoolean reached = new AtomicBoolean(false);

        blockMessage(cluster, (nodeName, msg) ->
                blocked.get() && stableKeySwitchMessage(msg, replicationGroupId, assignmentsPending, reached)
        );

        alterZone(node.catalogManager(), testZone, 5);

        waitForCondition(reached::get, 10_000L);

        CompletableFuture<Void> restartPartitionsWithCleanupFuture = node4.disasterRecoveryManager().restartPartitionsWithCleanup(
                Set.of(node4.name()),
                testZone,
                Set.of(0)
        );

        assertThat(restartPartitionsWithCleanupFuture, willCompleteSuccessfully());

        insert(1, 1, tableName);

        blocked.set(false);

        runningNodes = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(Collectors.toSet());

        assertEquals(5, runningNodes.size(), "Expected 5 running nodes after zone alteration");

        assertValueOnSpecificNodes(tableName, runningNodes, 0, 0);

        assertValueOnSpecificNodes(tableName, runningNodes, 1, 1);
    }
}
