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

package org.apache.ignite.internal.table.distributed.disaster;

import static java.util.Collections.emptySet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignments;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.table.TableTestUtils.getTableId;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager.RECOVERY_TRIGGER_KEY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.versioned.VersionedSerialization;

/** Parent for tests of HA zones feature. */
public abstract class AbstractHighAvailablePartitionsRecoveryTest extends ClusterPerTestIntegrationTest {
    static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

    static final String HA_ZONE_NAME = "HA_ZONE";

    static final String HA_TABLE_NAME = "HA_TABLE";

    static final String SC_ZONE_NAME = "SC_ZONE";

    private static final String SC_TABLE_NAME = "SC_TABLE";

    private static final int PARTITIONS_NUMBER = 2;

    static Set<Integer> PARTITION_IDS = IntStream
            .range(0, PARTITIONS_NUMBER)
            .boxed()
            .collect(Collectors.toUnmodifiableSet());

    protected final HybridClock clock = new HybridClockImpl();

    final void assertRecoveryRequestForHaZoneTable(IgniteImpl node) {
        assertRecoveryRequestForZoneTable(node, HA_ZONE_NAME, HA_TABLE_NAME);
    }

    private void assertRecoveryRequestForZoneTable(IgniteImpl node, String zoneName, String tableName) {
        Entry recoveryTriggerEntry = getRecoveryTriggerKey(node);

        GroupUpdateRequest request = (GroupUpdateRequest) VersionedSerialization.fromBytes(
                recoveryTriggerEntry.value(), DisasterRecoveryRequestSerializer.INSTANCE);

        Catalog catalog = node.catalogManager().activeCatalog(clock.nowLong());

        int zoneId = catalog.zone(zoneName).id();
        int tableId = catalog.table(SCHEMA_NAME, tableName).id();

        assertEquals(zoneId, request.zoneId());
        assertEquals(Map.of(tableId, PARTITION_IDS), request.partitionIds());
        assertFalse(request.manualUpdate());
    }

    static void assertRecoveryRequestWasOnlyOne(IgniteImpl node) {
        assertEquals(
                1,
                node
                        .metaStorageManager()
                        .getLocally(RECOVERY_TRIGGER_KEY.bytes(), 0L, Long.MAX_VALUE).size()
        );
    }

    final void waitAndAssertStableAssignmentsOfPartitionEqualTo(
            IgniteImpl gatewayNode, String tableName, Set<Integer> partitionIds, Set<String> nodes) {
        partitionIds.forEach(p -> {
            try {
                waitAndAssertStableAssignmentsOfPartitionEqualTo(gatewayNode, tableName, p, nodes);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private void waitAndAssertStableAssignmentsOfPartitionEqualTo(
            IgniteImpl gatewayNode, String tableName, int partNum, Set<String> nodes) throws InterruptedException {

        assertTrue(waitForCondition(() ->
                        nodes.equals(
                                getPartitionClusterNodes(gatewayNode, tableName, partNum)
                                        .stream()
                                        .map(Assignment::consistentId)
                                        .collect(Collectors.toUnmodifiableSet())
                        ),
                500,
                30_000
        ), "Expected set of nodes: " + nodes + " actual: " + getPartitionClusterNodes(gatewayNode, tableName, partNum)
                .stream()
                .map(Assignment::consistentId)
                .collect(Collectors.toUnmodifiableSet()));
    }

    /**
     * Wait for the 2 facts simultaneously.
     *
     * <ul>
     *     <li>All planned rebalances have finished (pending and planned keys is empty).</li>
     *     <li>Stable assignments is equal to expected</li>
     * </ul>
     *
     * @param gatewayNode Node for communication with cluster and components.
     * @param tableName Table name.
     * @param partitionIds Set of target partition ids to check.
     * @param nodes Expected set of nodes in stable assignments.
     */
    final void waitThatAllRebalancesHaveFinishedAndStableAssignmentsEqualsToExpected(
            IgniteImpl gatewayNode, String tableName, Set<Integer> partitionIds, Set<String> nodes) {
        partitionIds.forEach(p -> {
            try {
                waitThatAllRebalancesHaveFinishedAndStableAssignmentsEqualsToExpectedForPartition(gatewayNode, tableName, p, nodes);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * The same as the previous one, but for concrete partition.
     *
     * @see AbstractHighAvailablePartitionsRecoveryTest#waitThatAllRebalancesHaveFinishedAndStableAssignmentsEqualsToExpected
     */
    private void waitThatAllRebalancesHaveFinishedAndStableAssignmentsEqualsToExpectedForPartition(
            IgniteImpl gatewayNode, String tableName, int partNum, Set<String> nodes
    ) throws InterruptedException {
        AtomicReference<Set<String>> stableNodes = new AtomicReference<>();
        AtomicReference<Set<String>> pendingNodes = new AtomicReference<>();
        AtomicReference<Set<String>> plannedNodes = new AtomicReference<>();

        assertTrue(
                waitForCondition(() -> {
                            Integer tableId = getTableId(gatewayNode.catalogManager(), tableName, clock.nowLong());

                            assert tableId != null;

                            TablePartitionId tablePartitionId = new TablePartitionId(tableId, partNum);

                            ByteArray stableKey = stablePartAssignmentsKey(tablePartitionId);
                            ByteArray pendingKey = pendingPartAssignmentsKey(tablePartitionId);
                            ByteArray plannedKey = plannedPartAssignmentsKey(tablePartitionId);

                            Map<ByteArray, Entry> results = await(gatewayNode.metaStorageManager()
                                    .getAll(Set.of(stableKey, pendingKey, plannedKey)), 1, TimeUnit.SECONDS);

                            boolean isStableAsExpected = nodes.equals(assignmentsFromEntry(results.get(stableKey)));
                            boolean isPendingEmpty = results.get(pendingKey).value() == null;
                            boolean isPlannedEmpty = results.get(plannedKey).value() == null;

                            stableNodes.set(assignmentsFromEntry(results.get(stableKey)));
                            pendingNodes.set(assignmentsFromEntry(results.get(pendingKey)));
                            plannedNodes.set(assignmentsFromEntry(results.get(plannedKey)));

                            return isStableAsExpected && isPendingEmpty && isPlannedEmpty;
                        },
                        500,
                        30_000
                ), IgniteStringFormatter.format(
                        "Expected: (stable: {}, pending: [], planned: []), but actual: (stable: {}, pending: {}, planned: {})",
                        nodes, stableNodes, pendingNodes, plannedNodes
                )
        );
    }

    static Entry getRecoveryTriggerKey(IgniteImpl node) {
        return node.metaStorageManager().getLocally(RECOVERY_TRIGGER_KEY, Long.MAX_VALUE);
    }

    private static Set<String> assignmentsFromEntry(Entry entry) {
        return (entry.value() != null)
                ? Assignments.fromBytes(entry.value()).nodes()
                        .stream()
                        .map(Assignment::consistentId)
                        .collect(Collectors.toUnmodifiableSet())
                : emptySet();
    }

    private Set<Assignment> getPartitionClusterNodes(IgniteImpl node, String tableName, int partNum) {
        return Optional.ofNullable(getTableId(node.catalogManager(), tableName, clock.nowLong()))
                .map(tableId -> partitionAssignments(node.metaStorageManager(), tableId, partNum).join())
                .orElse(Set.of());
    }

    final int zoneIdByName(CatalogService catalogService, String zoneName) {
        return catalogService.activeCatalog(clock.nowLong()).zone(zoneName).id();
    }

    private void createHaZoneWithTables(
            String zoneName, List<String> tableNames, String filter, Set<String> targetNodes) throws InterruptedException {
        createHaZoneWithTables(zoneName, tableNames, filter, DEFAULT_STORAGE_PROFILE, targetNodes);
    }

    private void createHaZoneWithTables(
            String zoneName, List<String> tableNames, String filter, String storageProfiles, Set<String> targetNodes
    ) throws InterruptedException {
        executeSql(String.format(
                "CREATE ZONE %s WITH REPLICAS=%s, PARTITIONS=%s, STORAGE_PROFILES='%s', "
                        + "CONSISTENCY_MODE='HIGH_AVAILABILITY', DATA_NODES_FILTER='%s'",
                zoneName, targetNodes.size(), PARTITIONS_NUMBER, storageProfiles, filter
        ));

        Set<Integer> tableIds = new HashSet<>();

        for (String tableName : tableNames) {
            executeSql(String.format(
                    "CREATE TABLE %s (id INT PRIMARY KEY, val INT) ZONE %s",
                    tableName, zoneName
            ));

            tableIds.add(getTableId(igniteImpl(0).catalogManager(), tableName, clock.nowLong()));
        }

        awaitForAllNodesTableGroupInitialization(tableIds, targetNodes.size());

        tableNames.forEach(t ->
                waitAndAssertStableAssignmentsOfPartitionEqualTo(unwrapIgniteImpl(node(0)), t, PARTITION_IDS, targetNodes)
        );
    }

    final void createHaZoneWithTables(String zoneName, List<String> tableNames) throws InterruptedException {
        Set<String> allNodes = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        createHaZoneWithTables(zoneName, tableNames, DEFAULT_FILTER, allNodes);
    }

    final void createHaZoneWithTable(String filter, Set<String> targetNodes) throws InterruptedException {
        createHaZoneWithTables(HA_ZONE_NAME, List.of(HA_TABLE_NAME), filter, targetNodes);
    }

    final void createHaZoneWithTable(String zoneName, String tableName) throws InterruptedException {
        createHaZoneWithTables(zoneName, List.of(tableName));
    }

    final void createHaZoneWithTable(int replicas, String filter, Set<String> targetNodes) throws InterruptedException {
        createHaZoneWithTables(
                HA_ZONE_NAME, List.of(HA_TABLE_NAME), filter, targetNodes
        );
    }

    final void createHaZoneWithTable() throws InterruptedException {
        createHaZoneWithTable(HA_ZONE_NAME, HA_TABLE_NAME);
    }

    final void createHaZoneWithTableWithStorageProfile(int replicas, String storageProfiles, Set<String> targetNodes)
            throws InterruptedException {
        createHaZoneWithTables(
                HA_ZONE_NAME, List.of(HA_TABLE_NAME), DEFAULT_FILTER, storageProfiles, targetNodes
        );
    }

    final void createScZoneWithTable() {
        executeSql(String.format(
                "CREATE ZONE %s WITH REPLICAS=%s, PARTITIONS=%s, STORAGE_PROFILES='%s', CONSISTENCY_MODE='STRONG_CONSISTENCY'",
                SC_ZONE_NAME, initialNodes(), PARTITIONS_NUMBER, DEFAULT_STORAGE_PROFILE
        ));

        executeSql(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, val INT) ZONE %s",
                SC_TABLE_NAME, SC_ZONE_NAME
        ));

        Set<String> allNodes = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        waitAndAssertStableAssignmentsOfPartitionEqualTo(unwrapIgniteImpl(node(0)), SC_TABLE_NAME, PARTITION_IDS, allNodes);
    }

    static void assertRecoveryKeyIsEmpty(IgniteImpl gatewayNode) {
        assertTrue(getRecoveryTriggerKey(gatewayNode).empty());
    }

    static void waitAndAssertRecoveryKeyIsNotEmpty(IgniteImpl gatewayNode) throws InterruptedException {
        waitAndAssertRecoveryKeyIsNotEmpty(gatewayNode, 5_000);
    }

    static void waitAndAssertRecoveryKeyIsNotEmpty(IgniteImpl gatewayNode, long timeoutMillis) throws InterruptedException {
        assertTrue(waitForCondition(() -> !getRecoveryTriggerKey(gatewayNode).empty(), timeoutMillis));
    }

    void stopNodes(Integer... nodes) {
        Arrays.stream(nodes).forEach(this::stopNode);
    }

    static void changePartitionDistributionTimeout(IgniteImpl gatewayNode, int timeout) {
        CompletableFuture<Void> changeFuture = gatewayNode
                .clusterConfiguration()
                .getConfiguration(SystemDistributedExtensionConfiguration.KEY)
                .system().change(c0 -> c0.changeProperties()
                        .createOrUpdate(PARTITION_DISTRIBUTION_RESET_TIMEOUT,
                                c1 -> c1.changePropertyValue(String.valueOf(timeout)))
                );

        assertThat(changeFuture, willCompleteSuccessfully());
    }

    final long waitForSpecificZoneTopologyUpdateAndReturnUpdateRevision(
            IgniteImpl gatewayNode, String zoneName, Set<String> targetTopologyUpdate
    ) throws InterruptedException {
        int zoneId = zoneIdByName(gatewayNode.catalogManager(), zoneName);

        AtomicLong revision = new AtomicLong();

        assertTrue(waitForCondition(() -> {
            var state = gatewayNode
                    .distributionZoneManager()
                    .zonesState()
                    .get(zoneId);

            if (state != null) {
                var lastEntry = state.topologyAugmentationMap().lastEntry();

                var isTheSameAsTarget = lastEntry.getValue().nodes()
                        .stream()
                        .map(Node::nodeName)
                        .collect(Collectors.toUnmodifiableSet())
                        .equals(targetTopologyUpdate);

                if (isTheSameAsTarget) {
                    revision.set(lastEntry.getKey());
                }

                return isTheSameAsTarget;
            }
            return false;
        }, 10_000));

        assert revision.get() != 0;

        return revision.get();
    }

    private void awaitForAllNodesTableGroupInitialization(Set<Integer> tableIds, int replicas) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            AtomicInteger numberOfInitializedReplicas = new AtomicInteger(0);

            runningNodes().forEach(ignite -> {
                IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
                igniteImpl.raftManager().localNodes().forEach((raftNodeId) -> {

                    if (raftNodeId.groupId() instanceof TablePartitionId
                            && tableIds.contains(((TablePartitionId) raftNodeId.groupId()).tableId())) {
                        try {
                            if (igniteImpl.raftManager().raftNodeIndex(raftNodeId).index() > 0) {
                                numberOfInitializedReplicas.incrementAndGet();
                            }
                        } catch (NodeStoppingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

            });

            return PARTITIONS_NUMBER * replicas * tableIds.size() == numberOfInitializedReplicas.get();
        }, 10_000));
    }

    final Set<String> nodeNames(Integer... indexes) {
        return Arrays
                .stream(indexes)
                .map(i -> node(i).name())
                .collect(Collectors.toUnmodifiableSet());
    }
}
