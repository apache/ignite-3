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

package org.apache.ignite.internal.rebalance;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.table.TableTestUtils.getTableId;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getZoneIdByTableNameStrict;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for recovery of the rebalance procedure.
 */
public class ItRebalanceTriggersRecoveryTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "TEST";

    private static final int PARTITION_ID = 0;

    private static final String US_NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} },\n"
            + "  nodeAttributes: {\n"
            + "    nodeAttributes: {region: US, zone: global}\n"
            + "  },\n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    private static final String GLOBAL_NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} },\n"
            + "  nodeAttributes: {\n"
            + "    nodeAttributes: {zone: global}\n"
            + "  },\n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testRebalanceTriggersRecoveryAfterFilterUpdate() throws Exception {
        // The nodes from different regions/zones needed to implement the predictable way of nodes choice.
        startNode(1, US_NODE_BOOTSTRAP_CFG_TEMPLATE);
        startNode(2, GLOBAL_NODE_BOOTSTRAP_CFG_TEMPLATE);

        cluster.doInSession(0, session -> {
            session.execute(null, "CREATE ZONE "+ ZONE_NAME +" WITH PARTITIONS=1, REPLICAS=2, DATA_NODES_FILTER='$[?(@.region == \"US\")]', "
                    + "STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");
            session.execute(null, "CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name INT) ZONE " + ZONE_NAME);
            session.execute(null, "INSERT INTO " + TABLE_NAME + " VALUES (0, 0)");
        });

        assertTrue(waitForCondition(() -> containsPartition(cluster.node(1)), 10_000));
        assertFalse(containsPartition(cluster.node(2)));

        // By this we guarantee, that there will no any partition data nodes, which will be available to perform the rebalance.
        // To run the actual changePeersAndLearnersAsync we need the partition leader, which catch the metastore event about new pending
        // keys.
        WatchListenerInhibitor.metastorageEventsInhibitor(cluster.node(1)).startInhibit();
        WatchListenerInhibitor.metastorageEventsInhibitor(cluster.node(2)).startInhibit();

        cluster.doInSession(0, session -> {
            session.execute(null, "ALTER ZONE " + ZONE_NAME + " SET DATA_NODES_FILTER='$[?(@.zone == \"global\")]'");
        });

        // Check that metastore node schedule the rebalance procedure.
        assertTrue(waitForCondition(
                (() -> getPartitionPendingClusterNodes(unwrapIgniteImpl(node(0)), PARTITION_ID).equals(Set.of(
                        Assignment.forPeer(node(2).name()),
                        Assignment.forPeer(node(1).name())))),
                10_000));

        // Remove the pending keys in a barbarian way. So, the rebalance can be triggered only by the recovery logic now.
        removePendingPartAssignmentsQueueKey(TABLE_NAME, PARTITION_ID);

        restartNode(1);
        restartNode(2);

        // Check that new replica from 'global' zone received the data and rebalance really happened.
        assertTrue(waitForCondition(() -> containsPartition(cluster.node(2)), 10_000));
    }

    @Test
    void testRebalanceTriggersRecoveryAfterReplicasUpdate() throws Exception {
        // The nodes from different regions/zones needed to implement the predictable way of nodes choice.
        startNode(1, US_NODE_BOOTSTRAP_CFG_TEMPLATE);
        startNode(2, GLOBAL_NODE_BOOTSTRAP_CFG_TEMPLATE);

        cluster.doInSession(0, session -> {
            session.execute(null, "CREATE ZONE " + ZONE_NAME + " WITH PARTITIONS=1, REPLICAS=1, "
                    + "DATA_NODES_FILTER='$[?(@.zone == \"global\")]', STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");
            session.execute(null, "CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name INT) ZONE " + ZONE_NAME);
            session.execute(null, "INSERT INTO " + TABLE_NAME + " VALUES (0, 0)");
        });

        assertTrue(waitForCondition(() -> containsPartition(cluster.node(1)), 10_000));
        assertFalse(containsPartition(cluster.node(2)));

        // By this we guarantee, that there will no any partition data nodes, which will be available to perform the rebalance.
        // To run the actual changePeersAndLearnersAsync we need the partition leader, which catch the metastore event about new pending
        // keys.
        WatchListenerInhibitor.metastorageEventsInhibitor(cluster.node(1)).startInhibit();
        WatchListenerInhibitor.metastorageEventsInhibitor(cluster.node(2)).startInhibit();

        cluster.doInSession(0, session -> {
            session.execute(null, "ALTER ZONE " + ZONE_NAME + " SET REPLICAS=2");
        });

        // Check that metastore node schedule the rebalance procedure.
        assertTrue(waitForCondition(
                (() -> getPartitionPendingClusterNodes(unwrapIgniteImpl(node(0)), PARTITION_ID).equals(Set.of(
                        Assignment.forPeer(node(2).name()),
                        Assignment.forPeer(node(1).name())))),
                10_000));

        // Remove the pending keys in a barbarian way. So, the rebalance can be triggered only by the recovery logic now.
        removePendingPartAssignmentsQueueKey(TABLE_NAME, PARTITION_ID);

        restartNode(1);
        restartNode(2);

        // Check that new replica from 'global' zone received the data and rebalance really happened.
        assertTrue(waitForCondition(() -> containsPartition(cluster.node(2)), 10_000));
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21596")
    void testRebalanceTriggersRecoveryWhenUpdatesWereProcessedByAnotherNodesAlready() throws Exception {
        // The nodes from different regions/zones needed to implement the predictable way of nodes choice.
        startNode(1, US_NODE_BOOTSTRAP_CFG_TEMPLATE);
        startNode(2, GLOBAL_NODE_BOOTSTRAP_CFG_TEMPLATE);
        startNode(3);

        cluster.doInSession(0, session -> {
            session.execute(null, "CREATE ZONE " + ZONE_NAME + " WITH PARTITIONS=1, REPLICAS=1, "
                    + "DATA_NODES_FILTER='$[?(@.region == \"US\")]', STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");
            session.execute(null, "CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name INT) ZONE " + ZONE_NAME);
            session.execute(null, "INSERT INTO " + TABLE_NAME + " VALUES (0, 0)");
        });

        assertTrue(waitForCondition(() -> containsPartition(cluster.node(1)), 10_000));
        assertFalse(containsPartition(cluster.node(2)));

        stopNode(3);

        cluster.doInSession(0, session -> {
            session.execute(null, "ALTER ZONE " + ZONE_NAME + " SET REPLICAS=2, DATA_NODES_FILTER='$[?(@.zone == \"global\")]'");
        });

        // Check that new replica from 'global' zone received the data and rebalance really happened.
        assertTrue(waitForCondition(() -> containsPartition(cluster.node(2)), 10_000));
        assertTrue(waitForCondition(
                (() -> getPartitionPendingClusterNodes(unwrapIgniteImpl(node(0)), PARTITION_ID).equals(Set.of())),
                10_000));

        long pendingKeysRevisionBeforeRecovery = pendingPartAssignmentsQueueKeyRevision(TABLE_NAME, PARTITION_ID);

        startNode(3, GLOBAL_NODE_BOOTSTRAP_CFG_TEMPLATE);

        long pendingKeysRevisionAfterRecovery = pendingPartAssignmentsQueueKeyRevision(TABLE_NAME, PARTITION_ID);

        // Check that recovered node doesn't produce new rebalances for already processed triggers.
        assertEquals(pendingKeysRevisionBeforeRecovery, pendingKeysRevisionAfterRecovery);
    }

    private static Set<Assignment> getPartitionPendingClusterNodes(IgniteImpl node, int partNum) {
        Optional<Set<Assignment>> pendingAssignments;

        if (enabledColocation()) {
            pendingAssignments = Optional
                    .of(getZoneIdByTableNameStrict(node.catalogManager(), TABLE_NAME, new HybridClockImpl().nowLong()))
                    .map(zoneId -> zonePartitionPendingAssignments(node.metaStorageManager(), zoneId, partNum).join());
        } else {
            pendingAssignments = Optional
                    .ofNullable(getTableId(node.catalogManager(), TABLE_NAME, new HybridClockImpl().nowLong()))
                    .map(tableId -> tablePartitionPendingAssignments(node.metaStorageManager(), tableId, partNum).join());
        }

        return pendingAssignments.orElse(Set.of());
    }

    private static CompletableFuture<Set<Assignment>> tablePartitionPendingAssignments(
            MetaStorageManager metaStorageManager,
            int tableId,
            int partitionNumber
    ) {
        return metaStorageManager
                .get(pendingPartAssignmentsQueueKey(new TablePartitionId(tableId, partitionNumber)))
                .thenApply(e -> (e.value() == null) ? null : AssignmentsQueue.fromBytes(e.value()).poll().nodes());
    }

    private static CompletableFuture<Set<Assignment>> zonePartitionPendingAssignments(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int partitionNumber
    ) {
        return metaStorageManager
                .get(ZoneRebalanceUtil.pendingPartAssignmentsQueueKey(new ZonePartitionId(zoneId, partitionNumber)))
                .thenApply(e -> (e.value() == null) ? null : AssignmentsQueue.fromBytes(e.value()).poll().nodes());
    }

    private long pendingPartAssignmentsQueueKeyRevision(String tableName, int partitionId) throws Exception {
        MetaStorageManager metaStorageManager = unwrapIgniteImpl(node(0)).metaStorageManager();

        CompletableFuture<Entry> pendingKeysRevisionFuture;
        CatalogManager catalogManager = unwrapIgniteImpl(node(0)).catalogManager();

        if (enabledColocation()) {
            int zoneId = getZoneIdByTableNameStrict(catalogManager, tableName, new HybridClockImpl().nowLong());

            pendingKeysRevisionFuture = metaStorageManager
                    .get(ZoneRebalanceUtil.pendingPartAssignmentsQueueKey(new ZonePartitionId(zoneId, partitionId)));
        } else {
            int tableId = getTableIdStrict(catalogManager, tableName, new HybridClockImpl().nowLong());

            pendingKeysRevisionFuture = metaStorageManager
                    .get(pendingPartAssignmentsQueueKey(new TablePartitionId(tableId, partitionId)));
        }

        return pendingKeysRevisionFuture.get(10, TimeUnit.SECONDS).revision();
    }

    private void removePendingPartAssignmentsQueueKey(String tableName, int partitionId) {
        MetaStorageManager metaStorageManager = unwrapIgniteImpl(node(0)).metaStorageManager();
        CatalogManager catalogManager = unwrapIgniteImpl(node(0)).catalogManager();

        ByteArray pendingPartAssignmentsQueueKey;

        if (enabledColocation()) {
            int zoneId = getZoneIdByTableNameStrict(catalogManager, tableName, new HybridClockImpl().nowLong());

            pendingPartAssignmentsQueueKey = ZoneRebalanceUtil.pendingPartAssignmentsQueueKey(new ZonePartitionId(zoneId, PARTITION_ID));
        } else {
            int tableId = getTableIdStrict(catalogManager, tableName, new HybridClockImpl().nowLong());

            pendingPartAssignmentsQueueKey = pendingPartAssignmentsQueueKey(new TablePartitionId(tableId, PARTITION_ID));
        }

        metaStorageManager.remove(pendingPartAssignmentsQueueKey).join();
    }

    private static boolean containsPartition(Ignite node) {
        TableManager tableManager = unwrapTableManager(node.tables());

        MvPartitionStorage storage = tableManager.tableView(QualifiedName.fromSimple(TABLE_NAME))
                .internalTable()
                .storage()
                .getMvPartition(PARTITION_ID);

        return storage != null && bypassingThreadAssertions(() -> storage.closestRowId(RowId.lowestRowId(PARTITION_ID))) != null;
    }
}
