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
import static java.util.Map.of;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.pendingPartitionAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.plannedPartitionAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.stablePartitionAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartitionAssignments;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.table.TableTestUtils.getTableId;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager.RECOVERY_TRIGGER_KEY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneTimer;
import org.apache.ignite.internal.distributionzones.DistributionZoneTimer.DistributionZoneTimerSerializer;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.rebalance.AssignmentUtil;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/** Parent for tests of HA zones feature. */
public abstract class AbstractHighAvailablePartitionsRecoveryTest extends ClusterPerTestIntegrationTest {
    static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

    static final String HA_ZONE_NAME = "HA_ZONE";

    static final String HA_TABLE_NAME = "HA_TABLE";

    static final String SC_ZONE_NAME = "SC_ZONE";

    private static final String SC_TABLE_NAME = "SC_TABLE";

    private static final int PARTITIONS_NUMBER = 2;

    private static final int ENTRIES = 2;

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

        if (colocationEnabled()) {
            assertEquals(of(zoneId, PARTITION_IDS), request.partitionIds());
        } else {
            assertEquals(of(tableId, PARTITION_IDS), request.partitionIds());
        }

        assertEquals(zoneId, request.zoneId());
        assertFalse(request.manualUpdate());
    }

    static void assertRecoveryRequestWasOnlyOne(IgniteImpl node) {
        assertRecoveryRequestCount(node, 1);
    }

    static void assertRecoveryRequestCount(IgniteImpl node, int count) {
        MetaStorageManager metaStorageManager = node.metaStorageManager();

        KeyValueStorage storage = ((MetaStorageManagerImpl) metaStorageManager).storage();

        int revisions = 0;

        long nextUpperBound = storage.revision();
        while (true) {
            Entry entry = storage.get(RECOVERY_TRIGGER_KEY.bytes(), nextUpperBound);

            if (entry.empty()) {
                break;
            }

            revisions++;
            nextUpperBound = entry.revision() - 1;
        }

        assertEquals(count, revisions);
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

                            int zoneId = TableTestUtils.getZoneIdByTableNameStrict(
                                    gatewayNode.catalogManager(),
                                    tableName,
                                    clock.nowLong()
                            );

                            PartitionGroupId replicationGroupId;

                            if (colocationEnabled()) {
                                replicationGroupId = new ZonePartitionId(zoneId, partNum);
                            } else {
                                replicationGroupId = new TablePartitionId(tableId, partNum);
                            }

                            ByteArray stableKey = stablePartitionAssignmentsKey(replicationGroupId);
                            ByteArray pendingKey = pendingPartitionAssignmentsKey(replicationGroupId);
                            ByteArray plannedKey = plannedPartitionAssignmentsKey(replicationGroupId);

                            Map<ByteArray, Entry> results = await(gatewayNode.metaStorageManager()
                                    .getAll(Set.of(stableKey, pendingKey, plannedKey)), 1, TimeUnit.SECONDS);

                            boolean isStableAsExpected = nodes.equals(assignmentsFromEntry(results.get(stableKey)));
                            boolean isPendingEmpty = results.get(pendingKey).value() == null;
                            boolean isPlannedEmpty = results.get(plannedKey).value() == null;

                            stableNodes.set(assignmentsFromEntry(results.get(stableKey)));
                            pendingNodes.set(assignmentsFromPendingEntry(results.get(pendingKey)));
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

    private static Set<String> assignmentsFromPendingEntry(Entry entry) {
        return (entry.value() != null)
                ? AssignmentsQueue.fromBytes(entry.value()).poll().nodes()
                        .stream()
                        .map(Assignment::consistentId)
                        .collect(Collectors.toUnmodifiableSet())
                : emptySet();
    }

    Set<Assignment> getPartitionClusterNodes(IgniteImpl node, String tableName, int partNum) {
        if (colocationEnabled()) {
            int zoneId = TableTestUtils.getZoneIdByTableNameStrict(node.catalogManager(), tableName, clock.nowLong());

            CompletableFuture<Set<Assignment>> zonePartAssignmentsFut =
                    ZoneRebalanceUtil.zonePartitionAssignments(node.metaStorageManager(), zoneId, partNum);

            assertThat(zonePartAssignmentsFut, willCompleteSuccessfully());

            return zonePartAssignmentsFut.join();
        } else {
            return getTablePartitionClusterNodes(node, tableName, partNum);
        }
    }

    private Set<Assignment> getTablePartitionClusterNodes(IgniteImpl node, String tableName, int partNum) {
        return Optional.ofNullable(getTableId(node.catalogManager(), tableName, clock.nowLong()))
                .map(tableId -> stablePartitionAssignments(node.metaStorageManager(), tableId, partNum).join())
                .orElse(Set.of());
    }

    final int zoneIdByName(CatalogService catalogService, String zoneName) {
        return catalogService.activeCatalog(clock.nowLong()).zone(zoneName).id();
    }

    private void createHaZoneWithTables(
            String zoneName, List<String> tableNames, String filter, Set<String> targetNodes) throws InterruptedException {
        createHaZoneWithTables(zoneName, tableNames, PARTITIONS_NUMBER, null, filter, DEFAULT_STORAGE_PROFILE, targetNodes);
    }

    private void createHaZoneWithTables(
            String zoneName,
            List<String> tableNames,
            int partitionNumber,
            @Nullable Integer quorumSize,
            String filter,
            String storageProfiles,
            Set<String> targetNodes
    ) throws InterruptedException {
        if (quorumSize != null) {
            executeSql(String.format(
                    "CREATE ZONE %s (REPLICAS %s, PARTITIONS %s, QUORUM SIZE %s, CONSISTENCY MODE 'HIGH_AVAILABILITY', NODES FILTER '%s') "
                            + "STORAGE PROFILES ['%s']",
                    zoneName, targetNodes.size(), partitionNumber, quorumSize, filter, storageProfiles
            ));
        } else {
            executeSql(String.format(
                    "CREATE ZONE %s (REPLICAS %s, PARTITIONS %s, CONSISTENCY MODE 'HIGH_AVAILABILITY', NODES FILTER '%s') "
                            + "STORAGE PROFILES ['%s']",
                    zoneName, targetNodes.size(), partitionNumber, filter, storageProfiles
            ));
        }

        Set<Integer> tableIds = new HashSet<>();

        for (String tableName : tableNames) {
            executeSql(String.format(
                    "CREATE TABLE %s (id INT PRIMARY KEY, val INT) ZONE %s",
                    tableName, zoneName
            ));

            tableIds.add(getTableId(igniteImpl(0).catalogManager(), tableName, clock.nowLong()));
        }

        int zoneId = DistributionZonesTestUtil.getZoneId(igniteImpl(0).catalogManager(), zoneName, clock.nowLong());

        Set<Integer> partitionIds = Arrays.stream(AssignmentUtil.partitionIds(partitionNumber)).boxed().collect(Collectors.toSet());

        if (colocationEnabled()) {
            awaitForAllNodesZoneGroupInitialization(zoneId, partitionNumber, targetNodes.size());
            waitAndAssertStableAssignmentsOfPartitionEqualTo(unwrapIgniteImpl(node(0)), tableNames.get(0), partitionIds, targetNodes);
        } else {
            awaitForAllNodesTableGroupInitialization(tableIds, partitionNumber, targetNodes.size());
            tableNames.forEach(t ->
                    waitAndAssertStableAssignmentsOfPartitionEqualTo(unwrapIgniteImpl(node(0)), t, partitionIds, targetNodes)
            );
        }
    }

    final void createHaZoneWithTables(String zoneName, List<String> tableNames) throws InterruptedException {
        Set<String> allNodes = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        createHaZoneWithTables(zoneName, tableNames, DEFAULT_FILTER, allNodes);
    }

    final void createHaZoneWithTables(
            String zoneName,
            int partitionNumber,
            String filter,
            @Nullable Integer quorumSize,
            List<String> tableNames,
            Set<String> targetNodes
    ) throws InterruptedException {
        createHaZoneWithTables(zoneName, tableNames, partitionNumber, quorumSize, filter, DEFAULT_STORAGE_PROFILE, targetNodes);
    }

    final void createHaZoneWithTable(String filter, Set<String> targetNodes) throws InterruptedException {
        createHaZoneWithTables(HA_ZONE_NAME, List.of(HA_TABLE_NAME), filter, targetNodes);
    }

    final void createHaZoneWithTable(String zoneName, String tableName) throws InterruptedException {
        createHaZoneWithTables(zoneName, List.of(tableName));
    }

    final void createHaZoneWithTable() throws InterruptedException {
        createHaZoneWithTable(HA_ZONE_NAME, HA_TABLE_NAME);
    }

    final void createHaZoneWithTableWithStorageProfile(String storageProfiles, Set<String> targetNodes)
            throws InterruptedException {
        createHaZoneWithTables(HA_ZONE_NAME, List.of(HA_TABLE_NAME), PARTITIONS_NUMBER, null, DEFAULT_FILTER, storageProfiles, targetNodes);
    }

    final void createScZoneWithTable() {
        executeSql(String.format(
                "CREATE ZONE %s (REPLICAS %s, PARTITIONS %s, CONSISTENCY MODE 'STRONG_CONSISTENCY') STORAGE PROFILES ['%s']",
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

    static void changePartitionDistributionTimeout(IgniteImpl gatewayNode, int timeoutSeconds) {
        CompletableFuture<Void> changeFuture = gatewayNode
                .clusterConfiguration()
                .getConfiguration(SystemDistributedExtensionConfiguration.KEY)
                .system().change(c0 -> c0.changeProperties()
                        .createOrUpdate(PARTITION_DISTRIBUTION_RESET_TIMEOUT,
                                c1 -> c1.changePropertyValue(String.valueOf(timeoutSeconds)))
                );

        assertThat(changeFuture, willCompleteSuccessfully());
    }

    final long waitForSpecificZoneTopologyReductionAndReturnUpdateRevision(
            IgniteImpl gatewayNode, String zoneName, Set<String> targetTopologyReduction
    ) throws InterruptedException {
        int zoneId = zoneIdByName(gatewayNode.catalogManager(), zoneName);

        assertTrue(waitForCondition(() -> {
            Entry e = gatewayNode.metaStorageManager().getLocally(zoneScaleDownTimerKey(zoneId));

            if (e == null || e.value() == null) {
                return false;
            }

            DistributionZoneTimer scaleDownTimer = DistributionZoneTimerSerializer.deserialize(e.value());

            return scaleDownTimer.nodes().stream()
                    .map(NodeWithAttributes::nodeName)
                    .collect(Collectors.toUnmodifiableSet())
                    .containsAll(targetTopologyReduction);
        }, 10_000));

        return gatewayNode.metaStorageManager().appliedRevision();
    }

    private void awaitForAllNodesTableGroupInitialization(
            Set<Integer> tableIds,
            int partitionNumber,
            int replicas
    ) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            AtomicInteger numberOfInitializedReplicas = new AtomicInteger(0);

            runningNodes().forEach(ignite -> {
                IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
                igniteImpl.raftManager().localNodes().forEach((raftNodeId) -> {

                    if (raftNodeId.groupId() instanceof TablePartitionId
                            && tableIds.contains(((TablePartitionId) raftNodeId.groupId()).tableId())) {
                        incrementReplicaCountIfHasLog(raftNodeId, igniteImpl, numberOfInitializedReplicas);
                    }
                });

            });

            return partitionNumber * replicas * tableIds.size() == numberOfInitializedReplicas.get();
        }, 10_000));
    }

    private void awaitForAllNodesZoneGroupInitialization(int zoneId, int partitionNumber, int replicas) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            AtomicInteger numberOfInitializedReplicas = new AtomicInteger(0);

            runningNodes().forEach(ignite -> {
                IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
                igniteImpl.raftManager().localNodes().forEach((raftNodeId) -> {

                    if (raftNodeId.groupId() instanceof ZonePartitionId
                            && zoneId == ((ZonePartitionId) raftNodeId.groupId()).zoneId()
                    ) {
                        incrementReplicaCountIfHasLog(raftNodeId, igniteImpl, numberOfInitializedReplicas);
                    }
                });

            });

            return partitionNumber * replicas == numberOfInitializedReplicas.get();
        }, 10_000));
    }

    private static void incrementReplicaCountIfHasLog(
            RaftNodeId raftNodeId,
            IgniteImpl igniteImpl,
            AtomicInteger numberOfInitializedReplicas
    ) {
        try {
            if (igniteImpl.raftManager().raftNodeIndex(raftNodeId).index() > 0) {
                numberOfInitializedReplicas.incrementAndGet();
            }
        } catch (NodeStoppingException e) {
            throw new RuntimeException(e);
        }
    }

    final Set<String> nodeNames(Integer... indexes) {
        return Arrays
                .stream(indexes)
                .map(i -> node(i).name())
                .collect(Collectors.toUnmodifiableSet());
    }

    static List<Throwable> insertValues(Table table, int offset) {
        KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();

        List<Throwable> errors = new ArrayList<>();

        for (int i = 0; i < ENTRIES; i++) {
            Tuple key = Tuple.create(of("id", i));

            CompletableFuture<Void> insertFuture = keyValueView.putAsync(null, key, Tuple.create(of("val", i + offset)));

            try {
                assertThat(insertFuture, willCompleteSuccessfully());

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

    void assertValuesPresentOnNodes(HybridTimestamp ts, Table table, Integer... indexes) {
        for (Integer index : indexes) {
            assertValuesOnNode(table, ts, index, fut -> fut.join() != null);
        }
    }

    void assertPartitionsAreEmpty(String tableName,  Set<Integer> partitionIds, Integer... indexes) throws InterruptedException {
        for (Integer index : indexes) {
            for (Integer partId : partitionIds) {
                assertTrue(waitForCondition(() -> partitionIsEmpty(index, tableName, partId), 10_000));
            }
        }
    }

    private boolean partitionIsEmpty(Integer index, String tableName, int partId) {
        IgniteImpl targetNode = unwrapIgniteImpl(node(index));
        TableManager tableManager = unwrapTableManager(targetNode.tables());

        MvPartitionStorage storage = tableManager.tableView(QualifiedName.fromSimple(tableName))
                .internalTable()
                .storage()
                .getMvPartition(partId);

        return storage == null || bypassingThreadAssertions(() -> storage.closestRowId(RowId.lowestRowId(partId))) == null;
    }

    private void assertValuesOnNode(
            Table table,
            HybridTimestamp ts,
            int targetNodeIndex,
            Predicate<CompletableFuture<BinaryRow>> dataCondition
    ) {
        IgniteImpl targetNode = unwrapIgniteImpl(node(targetNodeIndex));

        TableImpl tableImpl = unwrapTableImpl(table);
        InternalTable internalTable = tableImpl.internalTable();

        for (int i = 0; i < ENTRIES; i++) {
            CompletableFuture<BinaryRow> fut =
                    internalTable.get(marshalKey(tableImpl, Tuple.create(of("id", i))), ts, targetNode.node());
            assertThat(fut, willCompleteSuccessfully());

            assertTrue(dataCondition.test(fut));
        }
    }

    private static Row marshalKey(TableViewInternal table, Tuple key) {
        SchemaRegistry schemaReg = table.schemaView();

        var marshaller = new TupleMarshallerImpl(schemaReg.lastKnownSchema());

        return marshaller.marshal(key, null);
    }

    private static boolean isPrimaryReplicaHasChangedException(IgniteException cause) {
        return ExceptionUtils.extractCodeFrom(cause) == Replicator.REPLICA_MISS_ERR;
    }

    void triggerManualReset(IgniteImpl node) {
        DisasterRecoveryManager disasterRecoveryManager = node.disasterRecoveryManager();
        CompletableFuture<?> updateFuture = disasterRecoveryManager.resetTablePartitions(
                HA_ZONE_NAME,
                SCHEMA_NAME,
                HA_TABLE_NAME,
                emptySet(),
                true,
                -1
        );

        assertThat(updateFuture, willCompleteSuccessfully());
    }
}
