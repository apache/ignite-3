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

package org.apache.ignite.internal.partition.replicator;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.alterZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToPublisher;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.replicator.RemoteResourceIds;
import org.apache.ignite.internal.table.distributed.storage.PartitionScanPublisher;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.FullyQualifiedResourceId;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry.RemotelyTriggeredResource;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Replica lifecycle test.
 */
@Timeout(60)
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication
public class ItReplicaLifecycleTest extends ItAbstractColocationTest {
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24374")
    @Test
    public void testZoneReplicaListener(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Assignment replicaAssignment = (Assignment) calculateAssignmentForPartition(
                nodes.values().stream().map(n -> n.name).collect(toList()), 0, 1, 1).toArray()[0];

        Node node = getNode(replicaAssignment.consistentId());

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        createZone(node, "test_zone", 1, 1);
        DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        long key = 1;

        {
            createTable(node, "test_zone", "test_table");
            int tableId = TableTestUtils.getTableId(node.catalogManager, "test_table", node.hybridClock.nowLong());

            KeyValueView<Long, Integer> keyValueView = node.tableManager.table(tableId).keyValueView(Long.class, Integer.class);

            int val = 100;

            node.transactions().runInTransaction(tx -> {
                assertDoesNotThrow(() -> keyValueView.put(tx, key, val));

                assertEquals(val, keyValueView.get(tx, key));
            });

            node.transactions().runInTransaction(tx -> {
                // Check the replica read inside the another transaction
                assertEquals(val, keyValueView.get(tx, key));
            });
        }

        {
            createTable(node, "test_zone", "test_table1");
            int tableId = TableTestUtils.getTableId(node.catalogManager, "test_table1", node.hybridClock.nowLong());

            KeyValueView<Long, Integer> keyValueView = node.tableManager.table(tableId).keyValueView(Long.class, Integer.class);

            int val = 200;

            node.transactions().runInTransaction(tx -> {
                assertDoesNotThrow(() -> keyValueView.put(tx, key, val));

                assertEquals(val, keyValueView.get(tx, key));
            });
        }
    }

    @Test
    void testAlterReplicaTrigger(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Node node = getNode(0);

        createZone(node, "test_zone", 1, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );

        CatalogManager catalogManager = node.catalogManager;

        alterZone(catalogManager, "test_zone", 2);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()).size(),
                2,
                20_000L
        );
    }

    @Test
    void testAlterReplicaTriggerDefaultZone(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Node node = getNode(0);

        CatalogManager catalogManager = node.catalogManager;

        Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        CatalogZoneDescriptor defaultZone = catalog.defaultZone();

        MetaStorageManager metaStorageManager = node.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(defaultZone.id(), 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()).size(),
                1,
                20_000L
        );

        alterZone(catalogManager, defaultZone.name(), 2);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()).size(),
                2,
                20_000L
        );
    }

    @Test
    void testAlterReplicaExtensionTrigger(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Node node = getNode(0);

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        assertTrue(waitForCondition(() -> node.distributionZoneManager.logicalTopology().size() == 3, 10_000L));

        createZone(node, "test_zone", 2, 2);

        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()).size(),
                2,
                20_000L
        );

        CatalogManager catalogManager = node.catalogManager;

        alterZone(catalogManager, "test_zone", 3);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );
    }

    @Test
    void testAlterFilterTrigger(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Node node = getNode(0);

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        createZone(node, "test_zone", 2, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );

        CatalogManager catalogManager = node.catalogManager;

        String newFilter = "$[?(@.region == \"US\" && @.storage == \"SSD\")]";

        alterZone(catalogManager, "test_zone", null, null, newFilter);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(nodes.get(0).name),
                20_000L
        );
    }

    @Test
    void testTableReplicaListenersCreationAfterRebalance(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Assignment replicaAssignment = (Assignment) calculateAssignmentForPartition(
                nodes.values().stream().map(n -> n.name).collect(toList()), 0, 1, 1).toArray()[0];

        Node node = getNode(replicaAssignment.consistentId());

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        DistributionZonesTestUtil.createZone(node.catalogManager, "test_zone", 1, 1);

        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        assertTrue(waitForCondition(() -> assertTableListenersCount(node, zoneId, 0), 10_000L));

        createTable(node, "test_zone", "test_table");

        assertTrue(waitForCondition(() -> assertTableListenersCount(node, zoneId, 1), 10_000L));

        alterZone(node.catalogManager, "test_zone", 3);

        assertTrue(waitForCondition(
                () -> IntStream.range(0, 3).allMatch(i -> assertTableListenersCount(getNode(i), zoneId, 1)),
                30_000L
        ));
    }

    @Test
    void testTableReplicaListenersRemoveAfterRebalance(TestInfo testInfo) throws Exception {
        String zoneName = "TEST_ZONE";
        String tableName = "TEST_TABLE";

        startNodes(testInfo, 3);

        Assignment replicaAssignment = (Assignment) calculateAssignmentForPartition(
                nodes.values().stream().map(n -> n.name).collect(toList()), 0, 1, 3).toArray()[0];

        Node node = getNode(replicaAssignment.consistentId());

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        DistributionZonesTestUtil.createZone(node.catalogManager, zoneName, 1, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, zoneName, node.hybridClock.nowLong());

        assertTrue(waitForCondition(() -> assertTableListenersCount(node, zoneId, 0), 10_000L));

        createTable(node, zoneName, tableName);

        assertTrue(waitForCondition(
                () -> IntStream.range(0, 3).allMatch(i -> getNode(i).tableManager.table(tableName) != null),
                30_000L
        ));

        assertTrue(waitForCondition(
                () -> IntStream.range(0, 3).allMatch(i -> assertTableListenersCount(getNode(i), zoneId, 1)),
                30_000L
        ));

        nodes.values().forEach(n -> checkNoDestroyPartitionStoragesInvokes(n, tableName, 0));

        alterZone(node.catalogManager, zoneName, 1);

        nodes.values().stream().filter(n -> !replicaAssignment.consistentId().equals(n.name)).forEach(
                n -> checkDestroyPartitionStoragesInvokes(n, tableName, 0));

    }

    @Test
    void testReplicaIsStartedOnNodeStart(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Node node0 = getNode(0);

        createZone(node0, "test_zone", 2, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node0.catalogManager, "test_zone", node0.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node0.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );

        stopNode(2);

        Node node2 = startNode(testInfo, 2);

        assertTrue(waitForCondition(() -> node2.replicaManager.isReplicaStarted(partId), 10_000L));
    }

    @Test
    void testStableAreWrittenAfterRestart(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 1);

        Node node0 = getNode(0);

        AtomicBoolean reached = new AtomicBoolean();

        node0.setInvokeInterceptor((condition, success, failure) -> {
            if (skipMetaStorageInvoke(success, STABLE_ASSIGNMENTS_PREFIX)) {
                reached.set(true);

                return true;
            }

            return null;
        });

        createZone(node0, "test_zone", 2, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node0.catalogManager, "test_zone", node0.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node0.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertTrue(reached.get());

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                Assignments::fromBytes,
                null,
                20_000L
        );

        stopNode(0);

        startNodes(testInfo, 1);

        node0 = getNode(0);

        metaStorageManager = node0.metaStorageManager;

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );

        assertTrue(waitForCondition(() -> getNode(0).replicaManager.isReplicaStarted(partId), 10_000L));
    }

    @Test
    void testStableAreWrittenAfterRestartAndConcurrentStableUpdate(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 1);

        Node node0 = getNode(0);

        AtomicBoolean reached = new AtomicBoolean();

        node0.setInvokeInterceptor((condition, success, failure) -> {
            if (skipMetaStorageInvoke(success, STABLE_ASSIGNMENTS_PREFIX)) {
                reached.set(true);

                return true;
            }

            return null;
        });

        createZone(node0, "test_zone", 1, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node0.catalogManager, "test_zone", node0.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node0.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertTrue(reached.get());

        reached.set(false);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                Assignments::fromBytes,
                null,
                20_000L
        );

        stopNode(0);

        startNodes(testInfo, 1, (condition, success, failure) -> {
            if (skipMetaStorageInvoke(success, stablePartAssignmentsKey(partId).toString())) {
                reached.set(true);

                Node node = nodes.get(0);

                int catalogVersion = node.catalogManager.latestCatalogVersion();
                long timestamp = node.catalogManager.catalog(catalogVersion).time();

                node.metaStorageManager.put(
                        stablePartAssignmentsKey(partId),
                        Assignments.of(timestamp, Assignment.forPeer(node.name)).toBytes()
                );
            }

            return null;
        });

        node0 = getNode(0);

        metaStorageManager = node0.metaStorageManager;

        assertTrue(reached.get());

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );

        assertTrue(waitForCondition(() -> getNode(0).replicaManager.isReplicaStarted(partId), 10_000L));
    }

    @Test
    public void testTableEstimatedSize(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 1);

        Assignment replicaAssignment1 = (Assignment) calculateAssignmentForPartition(
                nodes.values().stream().map(n -> n.name).collect(toList()), 0, 2, 1).toArray()[0];

        Node node = getNode(replicaAssignment1.consistentId());

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        createZone(node, "test_zone", 2, 1);
        DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        {
            createTable(node, "test_zone", "test_table_1");
            int tableId1 = TableTestUtils.getTableId(node.catalogManager, "test_table_1", node.hybridClock.nowLong());

            createTable(node, "test_zone", "test_table_2");
            int tableId2 = TableTestUtils.getTableId(node.catalogManager, "test_table_2", node.hybridClock.nowLong());

            KeyValueView<Long, Integer> keyValueView1 = node.tableManager.table(tableId1).keyValueView(Long.class, Integer.class);
            KeyValueView<Long, Integer> keyValueView2 = node.tableManager.table(tableId2).keyValueView(Long.class, Integer.class);

            Map<Long, Integer> kv1 = new HashMap<>();
            Map<Long, Integer> kv2 = new HashMap<>();

            for (int i = 1; i <= 5; ++i) {
                kv1.put((long) i, i * 100);
            }

            for (int i = 10; i <= 15; ++i) {
                kv2.put((long) i, i * 100);
            }

            node.transactions().runInTransaction(tx -> {
                assertDoesNotThrow(() -> keyValueView1.putAll(tx, kv1));

                assertDoesNotThrow(() -> keyValueView2.putAll(tx, kv2));
            });

            // Read the key from another transaction to trigger write intent resolution, and so incrementing the estimated size.
            // TODO https://issues.apache.org/jira/browse/IGNITE-24384 Perhaps, it should be reworked some way
            // when the write intent resolution will be 're-implemented' using colocation feature.
            node.transactions().runInTransaction(tx -> {
                keyValueView1.getAll(tx, kv1.keySet());

                keyValueView2.getAll(tx, kv2.keySet());
            });

            CompletableFuture<Long> sizeFuture1 = node.tableManager.table(tableId1).internalTable().estimatedSize();
            CompletableFuture<Long> sizeFuture2 = node.tableManager.table(tableId2).internalTable().estimatedSize();

            assertEquals(kv1.size(), sizeFuture1.get());
            assertEquals(kv2.size(), sizeFuture2.get());
        }
    }

    @Test
    public void testScanCloseReplicaRequest(TestInfo testInfo) throws Exception {
        // Prepare a single node cluster.
        startNodes(testInfo, 1);
        Node node = getNode(0);
        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        // Prepare a zone.
        String zoneName = "test_zone";
        createZone(node, zoneName, 1, 1);
        DistributionZonesTestUtil.getZoneId(node.catalogManager, zoneName, node.hybridClock.nowLong());

        // Create a table to work with.
        String tableName = "test_table";
        createTable(node, zoneName, tableName);
        int tableId = TableTestUtils.getTableId(node.catalogManager, tableName, node.hybridClock.nowLong());
        TableViewInternal tableViewInternal = node.tableManager.table(tableId);
        KeyValueView<Long, Integer> tableView = tableViewInternal.keyValueView(Long.class, Integer.class);

        // Write 2 rows to the table.
        Map<Long, Integer> valuesToPut = Map.of(0L, 0, 1L, 1);
        assertDoesNotThrow(() -> tableView.putAll(null, valuesToPut));

        InternalTable table = tableViewInternal.internalTable();

        // Be sure that the only partition identifier is 0.
        assertEquals(1, table.partitions());
        int partId = 0;

        // We have to use explicit transaction to check scan close scenario.
        InternalTransaction tx = (InternalTransaction) node.transactions().begin();

        // Prepare a subscription on table scan in explicit transaction.
        List<BinaryRow> scannedRows = new ArrayList<>();
        PartitionScanPublisher<BinaryRow> publisher = (PartitionScanPublisher<BinaryRow>) table.scan(partId, tx);
        CompletableFuture<Void> scanned = new CompletableFuture<>();
        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        // Let's request a row to be ensure later that rows could be read.
        subscription.request(1);

        // Waiting while request will be done.
        assertTrue(waitForCondition(() -> !scannedRows.isEmpty(), AWAIT_TIMEOUT_MILLIS));

        // By the cursor ID we can track if the cursor will be closed after subscription cancel.
        FullyQualifiedResourceId cursorId = RemoteResourceIds.cursorId(tx.id(), publisher.scanId(subscription));

        // Check that cursor is opened.
        assertNotNull(getVersionedStorageCursor(node, cursorId));

        // Check there that scanned rows count is equals to read ones.
        assertEquals(1, scannedRows.size());

        // Triggers scan close with the corresponding request.
        assertDoesNotThrow(subscription::cancel);

        // Wait while ScanCloseReplicaRequest will pass through all handlers and PartitionReplicaListener#processScanCloseAction will
        // close the cursor and remove it from the registry.
        assertTrue(waitForCondition(() -> getVersionedStorageCursor(node, cursorId) == null, AWAIT_TIMEOUT_MILLIS));

        // Commit the open transaction.
        assertDoesNotThrow(tx::commit);
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24558")
    public void testCatalogCompaction(TestInfo testInfo) throws Exception {
        // How often we update the low water mark.
        long lowWatermarkUpdateInterval = 500;
        updateLowWatermarkConfiguration(lowWatermarkUpdateInterval * 2, lowWatermarkUpdateInterval);

        // Prepare a single node cluster.
        startNodes(testInfo, 1);
        Node node = getNode(0);

        List<Set<Assignment>> assignments = PartitionDistributionUtils.calculateAssignments(
                nodes.values().stream().map(n -> n.name).collect(toList()), 1, 1);

        List<TokenizedAssignments> tokenizedAssignments = assignments.stream()
                .map(a -> new TokenizedAssignmentsImpl(a, Integer.MIN_VALUE))
                .collect(toList());

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());
        placementDriver.setAssignments(tokenizedAssignments);

        forceCheckpoint(node);

        String zoneName = "test-zone";
        createZone(node, zoneName, 1, 1);
        DistributionZonesTestUtil.getZoneId(node.catalogManager, zoneName, node.hybridClock.nowLong());

        int catalogVersion1 = getLatestCatalogVersion(node);

        String tableName1 = "test_table_1";
        createTable(node, zoneName, tableName1);

        String tableName2 = "test_table_2";
        createTable(node, zoneName, tableName2);

        int tableId = TableTestUtils.getTableId(node.catalogManager, tableName2, node.hybridClock.nowLong());
        TableViewInternal tableViewInternal = node.tableManager.table(tableId);
        KeyValueView<Long, Integer> tableView = tableViewInternal.keyValueView(Long.class, Integer.class);

        // Write 2 rows to the table.
        Map<Long, Integer> valuesToPut = Map.of(0L, 0, 1L, 1);
        assertDoesNotThrow(() -> tableView.putAll(null, valuesToPut));

        forceCheckpoint(node);

        int catalogVersion2 = getLatestCatalogVersion(node);
        assertThat("The catalog version did not changed [initial=" + catalogVersion1 + ", latest=" + catalogVersion2 + ']',
                catalogVersion2, greaterThan(catalogVersion1));

        expectEarliestCatalogVersion(node, catalogVersion2 - 1);
    }

    private static void expectEarliestCatalogVersion(Node node, int expectedVersion) throws Exception {
        boolean result = waitForCondition(() -> getEarliestCatalogVersion(node) == expectedVersion, 10_000);

        assertTrue(result,
                "Failed to wait for the expected catalog version [expected=" + expectedVersion
                        + ", earliest=" + getEarliestCatalogVersion(node)
                        + ", latest=" + getLatestCatalogVersion(node) + ']');
    }

    private static int getLatestCatalogVersion(Node node) {
        Catalog catalog = getLatestCatalog(node);

        return catalog.version();
    }

    private static int getEarliestCatalogVersion(Node node) {
        CatalogManager catalogManager = node.catalogManager;

        int ver = catalogManager.earliestCatalogVersion();

        Catalog catalog = catalogManager.catalog(ver);

        Objects.requireNonNull(catalog);

        return catalog.version();
    }

    private static Catalog getLatestCatalog(Node node) {
        CatalogManager catalogManager = node.catalogManager;

        int ver = catalogManager.activeCatalogVersion(node.hybridClock.nowLong());

        Catalog catalog = catalogManager.catalog(ver);

        Objects.requireNonNull(catalog);

        return catalog;
    }

    private static RemotelyTriggeredResource getVersionedStorageCursor(Node node, FullyQualifiedResourceId cursorId) {
        return node.resourcesRegistry.resources().get(cursorId);
    }

    private static boolean skipMetaStorageInvoke(Collection<Operation> ops, String prefix) {
        return ops.stream().anyMatch(op -> new String(toByteArray(op.key()), StandardCharsets.UTF_8).startsWith(prefix));
    }

    private static boolean assertTableListenersCount(Node node, int zoneId, int count) {
        try {
            CompletableFuture<Replica> replicaFut = node.replicaManager.replica(new ZonePartitionId(zoneId, 0));

            if (replicaFut == null) {
                return false;
            }

            Replica replica = replicaFut.get(1, SECONDS);

            return replica != null && (((ZonePartitionReplicaListener) replica.listener()).tableReplicaListeners().size() == count);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private static InternalTable getInternalTable(Node node, String tableName) {
        Table table = node.tableManager.table(tableName);

        assertNotNull(table, tableName);

        return ((TableViewInternal) table).internalTable();
    }

    private static void checkNoDestroyPartitionStoragesInvokes(Node node, String tableName, int partitionId) {
        InternalTable internalTable = getInternalTable(node, tableName);

        verify(internalTable.storage(), never())
                .destroyPartition(partitionId);
        verify(internalTable.txStateStorage(), never())
                .destroyTxStateStorage(partitionId);
    }

    private static void checkDestroyPartitionStoragesInvokes(Node node, String tableName, int partitionId) {
        InternalTable internalTable = getInternalTable(node, tableName);

        verify(internalTable.storage(), timeout(AWAIT_TIMEOUT_MILLIS).atLeast(1))
                .destroyPartition(partitionId);
        verify(internalTable.txStateStorage(), timeout(AWAIT_TIMEOUT_MILLIS).atLeast(1))
                .destroyTxStateStorage(partitionId);
    }

    /**
     * Update low water mark configuration.
     *
     * @param dataAvailabilityTime Data availability time.
     * @param updateInterval Update interval.
     */
    private void updateLowWatermarkConfiguration(long dataAvailabilityTime, long updateInterval) {
        CompletableFuture<?> updateFuture = gcConfiguration.lowWatermark().change(change -> {
            change.changeDataAvailabilityTime(dataAvailabilityTime);
            change.changeUpdateInterval(updateInterval);
        });

        assertThat(updateFuture, willSucceedFast());
    }

    /**
     * Start the new checkpoint immediately on the provided node.
     *
     * @param node Node to start the checkpoint on.
     */
    private static void forceCheckpoint(Node node) {
        PersistentPageMemoryStorageEngine storageEngine = (PersistentPageMemoryStorageEngine) node
                .dataStorageManager()
                .engineByStorageProfile(DEFAULT_STORAGE_PROFILE);

        assertThat(storageEngine.checkpointManager().forceCheckpoint("test-reason").futureFor(FINISHED),
                willSucceedIn(10, SECONDS));
    }

    @Test
    public void enabledColocationTest() {
        assertTrue(enabledColocation());
        System.setProperty(COLOCATION_FEATURE_FLAG, Boolean.FALSE.toString());
        assertFalse(enabledColocation());
        System.setProperty(COLOCATION_FEATURE_FLAG, Boolean.TRUE.toString());
        assertTrue(enabledColocation());
    }
}
