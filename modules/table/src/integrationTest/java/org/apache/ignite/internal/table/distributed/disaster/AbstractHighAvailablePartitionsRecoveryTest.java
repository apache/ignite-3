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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.distributionzones.configuration.DistributionZonesHighAvailabilityConfiguration.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignments;
import static org.apache.ignite.internal.table.TableTestUtils.getTableId;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager.RECOVERY_TRIGGER_KEY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.versioned.VersionedSerialization;

/** Parent for tests of HA zones feature. */
public abstract class AbstractHighAvailablePartitionsRecoveryTest extends ClusterPerTestIntegrationTest {
    static final String HA_ZONE_NAME = "HA_ZONE";

    static final String HA_TABLE_NAME = "HA_TABLE";

    static final String SC_ZONE_NAME = "SC_ZONE";

    private static final String SC_TABLE_NAME = "SC_TABLE";

    private static final int PARTITIONS_NUMBER = 2;

    protected final HybridClock clock = new HybridClockImpl();

    final void assertRecoveryRequestForHaZone(IgniteImpl node) {
        Entry recoveryTriggerEntry = getRecoveryTriggerKey(node);

        GroupUpdateRequest request = (GroupUpdateRequest) VersionedSerialization.fromBytes(
                recoveryTriggerEntry.value(), DisasterRecoveryRequestSerializer.INSTANCE);

        int zoneId = node.catalogManager().zone(HA_ZONE_NAME, clock.nowLong()).id();
        int tableId = node.catalogManager().table(HA_TABLE_NAME, clock.nowLong()).id();

        assertEquals(zoneId, request.zoneId());
        assertEquals(tableId, request.tableId());
        assertEquals(Set.of(0, 1), request.partitionIds());
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

    static Entry getRecoveryTriggerKey(IgniteImpl node) {
        return node.metaStorageManager().getLocally(RECOVERY_TRIGGER_KEY, Long.MAX_VALUE);
    }

    private Set<Assignment> getPartitionClusterNodes(IgniteImpl node, String tableName, int partNum) {
        return Optional.ofNullable(getTableId(node.catalogManager(), tableName, clock.nowLong()))
                .map(tableId -> partitionAssignments(node.metaStorageManager(), tableId, partNum).join())
                .orElse(Set.of());
    }

    final int zoneIdByName(CatalogService catalogService, String zoneName) {
        return catalogService.zone(zoneName, clock.nowLong()).id();
    }

    final void createHaZoneWithTable(String filter, Set<String> targetNodes) throws InterruptedException {
        executeSql(String.format(
                "CREATE ZONE %s WITH REPLICAS=%s, PARTITIONS=%s, STORAGE_PROFILES='%s', "
                        + "CONSISTENCY_MODE='HIGH_AVAILABILITY', DATA_NODES_FILTER='%s'",
                HA_ZONE_NAME, targetNodes.size(), PARTITIONS_NUMBER, DEFAULT_STORAGE_PROFILE, filter
        ));

        executeSql(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, val INT) ZONE %s",
                HA_TABLE_NAME, HA_ZONE_NAME
        ));

        awaitForAllNodesTableGroupInitialization(targetNodes.size());

        waitAndAssertStableAssignmentsOfPartitionEqualTo(unwrapIgniteImpl(node(0)), HA_TABLE_NAME, Set.of(0, 1), targetNodes);
    }

    final void createHaZoneWithTable() throws InterruptedException {
        Set<String> allNodes = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        createHaZoneWithTable(DEFAULT_FILTER, allNodes);
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

        waitAndAssertStableAssignmentsOfPartitionEqualTo(unwrapIgniteImpl(node(0)), SC_TABLE_NAME, Set.of(0, 1), allNodes);
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

    private void awaitForAllNodesTableGroupInitialization(int replicas) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            AtomicInteger numberOfInitializedReplicas = new AtomicInteger(0);

            runningNodes().forEach(ignite -> {
                IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
                igniteImpl.raftManager().forEach((raftNodeId, raftGroupService) -> {

                    if (raftNodeId.groupId() instanceof TablePartitionId) {
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

            return PARTITIONS_NUMBER * replicas == numberOfInitializedReplicas.get();
        }, 10_000));
    }

    final Set<String> nodeNames(Integer... indexes) {
        return Arrays
                .stream(indexes)
                .map(i -> node(i).name())
                .collect(Collectors.toUnmodifiableSet());
    }
}
