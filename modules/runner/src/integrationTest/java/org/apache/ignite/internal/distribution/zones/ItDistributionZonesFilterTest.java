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

package org.apache.ignite.internal.distribution.zones;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.utils.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.stablePartAssignmentsKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.sql.Session;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

/**
 * Integration test for data nodes' filters functionality.
 */
public class ItDistributionZonesFilterTest extends ClusterPerTestIntegrationTest {
    @Language("JSON")
    private static final String NODE_ATTRIBUTES = "{region:{attribute:\"US\"},storage:{attribute:\"SSD\"}}";

    private static final String COLUMN_KEY = "key";

    private static final String COLUMN_VAL = "val";

    private static final int TIMEOUT_MILLIS = 10_000;

    @Language("JSON")
    private static String createStartConfig(@Language("JSON") String nodeAttributes) {
        return "{\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ {} ]\n"
                + "    }\n"
                + "  },"
                + "  nodeAttributes: {\n"
                + "    nodeAttributes: " + nodeAttributes
                + "  },\n"
                + "}";
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return createStartConfig(NODE_ATTRIBUTES);
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    /**
     * Tests the scenario when filter was applied to data nodes and stable key for rebalance was changed to that filtered value.
     *
     * @throws Exception If failed.
     */
    @Test
    void testFilteredDataNodesPropagatedToStable() throws Exception {
        String filter = "'$[?(@.region == \"US\" && @.storage == \"SSD\")]'";

        // This node do not pass the filter
        @Language("JSON") String firstNodeAttributes = "{region:{attribute:\"EU\"},storage:{attribute:\"SSD\"}}";

        IgniteImpl node = startNode(1, createStartConfig(firstNodeAttributes));

        Session session = node.sql().createSession();

        session.execute(null, "CREATE ZONE \"TEST_ZONE\" WITH "
                + "\"REPLICAS\" = 3, "
                + "\"PARTITIONS\" = 2, "
                + "\"DATA_NODES_FILTER\" = " + filter + ", "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = 0, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = 0");

        String tableName = "table1";

        session.execute(null, "CREATE TABLE " + tableName + "("
                + COLUMN_KEY + " INT PRIMARY KEY, " + COLUMN_VAL + " VARCHAR) WITH PRIMARY_ZONE='TEST_ZONE'");

        MetaStorageManager metaStorageManager = (MetaStorageManager) IgniteTestUtils
                .getFieldValue(node, IgniteImpl.class, "metaStorageMgr");

        TableManager tableManager = (TableManager) IgniteTestUtils.getFieldValue(node, IgniteImpl.class, "distributedTblMgr");

        TableImpl table = (TableImpl) tableManager.table(tableName);

        TablePartitionId partId = new TablePartitionId(table.tableId(), 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v)).size(),
                null,
                TIMEOUT_MILLIS
        );

        @Language("JSON") String secondNodeAttributes = "{region:{attribute:\"US\"},storage:{attribute:\"SSD\"}}";

        // This node pass the filter
        startNode(2, createStartConfig(secondNodeAttributes));

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(1),
                (v) -> ((Map<Node, Integer>) fromBytes(v)).size(),
                3,
                TIMEOUT_MILLIS
        );

        Entry dataNodesEntry1 = metaStorageManager.get(zoneDataNodesKey(1)).get(5_000, MILLISECONDS);

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= dataNodesEntry1.revision(), TIMEOUT_MILLIS));

        // We check that two nodes that pass the filter are presented in the stable key.
        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v)).size(),
                2,
                TIMEOUT_MILLIS * 2
        );

        byte[] stableAssignments = metaStorageManager.get(stablePartAssignmentsKey(partId)).get(5_000, MILLISECONDS).value();

        assertNotNull(stableAssignments);

        Set<String> stable = ((Set<Assignment>) fromBytes(stableAssignments)).stream()
                .map(Assignment::consistentId)
                .collect(Collectors.toSet());

        assertEquals(2, stable.size());

        assertTrue(stable.contains(node(0).name()) && stable.contains(node(2).name()));
    }

    /**
     * Tests the scenario when removal node from the logical topology leads to empty data nodes, but this empty data nodes do not trigger
     * rebalance. Data nodes become empty after applying corresponding filter.
     *
     * @throws Exception If failed.
     */
    @Test
    void testFilteredEmptyDataNodesDoNotTriggerRebalance() throws Exception {
        String filter = "'$[?(@.region == \"EU\" && @.storage == \"HDD\")]'";

        // This node do not pass the filter.
        IgniteImpl node0 = node(0);

        // This node passes the filter
        @Language("JSON") String firstNodeAttributes = "{region:{attribute:\"EU\"},storage:{attribute:\"HDD\"}}";

        IgniteImpl node1 = startNode(1, createStartConfig(firstNodeAttributes));

        Session session = node1.sql().createSession();

        session.execute(null, "CREATE ZONE \"TEST_ZONE\" WITH "
                + "\"REPLICAS\" = 1, "
                + "\"PARTITIONS\" = 1, "
                + "\"DATA_NODES_FILTER\" = " + filter + ", "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = 0, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = 0");

        MetaStorageManager metaStorageManager = (MetaStorageManager) IgniteTestUtils.getFieldValue(
                node0,
                IgniteImpl.class,
                "metaStorageMgr"
        );

        waitDataNodeAndListenersAreHandled(metaStorageManager, 2);

        String tableName = "table1";

        session.execute(null, "CREATE TABLE " + tableName + "("
                + COLUMN_KEY + " INT PRIMARY KEY, " + COLUMN_VAL + " VARCHAR) WITH PRIMARY_ZONE='TEST_ZONE'");

        TableManager tableManager = (TableManager) IgniteTestUtils.getFieldValue(node0, IgniteImpl.class, "distributedTblMgr");

        TableImpl table = (TableImpl) tableManager.table(tableName);

        TablePartitionId partId = new TablePartitionId(table.tableId(), 0);

        // Table was created after both nodes was up, so there wasn't any rebalance.
        assertPendingStableAreNull(metaStorageManager, partId);

        // Stop node, that was only one, that passed the filter, so data nodes after filtering will be empty.
        stopNode(1);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 1);

        //Check that stable and pending are null, so there wasn't any rebalance.
        assertPendingStableAreNull(metaStorageManager, partId);
    }

    @Test
    void testFilteredEmptyDataNodesDoNotTriggerRebalanceOnReplicaUpdate() throws Exception {
        String filter = "'$[?(@.region == \"EU\" && @.storage == \"HDD\")]'";

        // This node do not pass the filter.
        IgniteImpl node0 = node(0);

        // This node passes the filter
        @Language("JSON") String firstNodeAttributes = "{region:{attribute:\"EU\"},storage:{attribute:\"HDD\"}}";

        startNode(1, createStartConfig(firstNodeAttributes));

        Session session = node0.sql().createSession();

        session.execute(null, "CREATE ZONE \"TEST_ZONE\" WITH "
                + "\"REPLICAS\" = 1, "
                + "\"PARTITIONS\" = 1, "
                + "\"DATA_NODES_FILTER\" = " + filter + ", "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = 0, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = 0");

        MetaStorageManager metaStorageManager = (MetaStorageManager) IgniteTestUtils.getFieldValue(
                node0,
                IgniteImpl.class,
                "metaStorageMgr"
        );

        waitDataNodeAndListenersAreHandled(metaStorageManager, 2);

        String tableName = "table1";

        session.execute(null, "CREATE TABLE " + tableName + "("
                + COLUMN_KEY + " INT PRIMARY KEY, " + COLUMN_VAL + " VARCHAR) WITH PRIMARY_ZONE='TEST_ZONE'");

        TableManager tableManager = (TableManager) IgniteTestUtils.getFieldValue(node0, IgniteImpl.class, "distributedTblMgr");

        TableImpl table = (TableImpl) tableManager.table(tableName);

        TablePartitionId partId = new TablePartitionId(table.tableId(), 0);

        // Table was created after both nodes was up, so there wasn't any rebalance.
        assertPendingStableAreNull(metaStorageManager, partId);

        // Stop node, that was only one, that passed the filter, so data nodes after filtering will be empty.
        stopNode(1);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 1);

        //Check that stable and pending are null, so there wasn't any rebalance.
        assertPendingStableAreNull(metaStorageManager, partId);

        session.execute(null, "ALTER ZONE \"TEST_ZONE\" SET "
                + "\"REPLICAS\" = 2");

        DistributionZoneManager distributionZoneManager = (DistributionZoneManager) IgniteTestUtils.getFieldValue(
                node0,
                IgniteImpl.class,
                "distributionZoneManager"
        );

        DistributionZonesConfiguration distributionZonesConfiguration = (DistributionZonesConfiguration) IgniteTestUtils.getFieldValue(
                distributionZoneManager,
                DistributionZoneManager.class,
                "zonesConfiguration"
        );

        CountDownLatch latch = new CountDownLatch(1);

        // We need to be sure, that the first asynchronous configuration change of replica was handled,
        // so we create a listener with a latch, and change replica again and wait for latch, so we can be sure that the first
        // replica was handled.
        distributionZonesConfiguration.distributionZones().any().replicas().listen(ctx -> {
            latch.countDown();

            return CompletableFuture.completedFuture(null);
        });

        session.execute(null, "ALTER ZONE \"TEST_ZONE\" SET \"REPLICAS\" = 3");

        latch.await(10_000, MILLISECONDS);

        //Check that stable and pending are null, so there wasn't any rebalance.
        assertPendingStableAreNull(metaStorageManager, partId);
    }

    private static void waitDataNodeAndListenersAreHandled(
            MetaStorageManager metaStorageManager,
            int expectedDataNodesSize
    ) throws Exception {
        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(1),
                (v) -> ((Map<Node, Integer>) fromBytes(v)).size(),
                expectedDataNodesSize,
                TIMEOUT_MILLIS
        );

        ByteArray fakeKey = new ByteArray("Foo");

        metaStorageManager.put(fakeKey, toBytes("Bar")).get(5_000, MILLISECONDS);

        Entry fakeEntry = metaStorageManager.get(fakeKey).get(5_000, MILLISECONDS);

        // We wait for all data nodes listeners are triggered and all their meta storage activity is done.
        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= fakeEntry.revision(), 5_000));
    }

    private static void assertPendingStableAreNull(
            MetaStorageManager metaStorageManager,
            TablePartitionId partId
    ) throws InterruptedException {
        assertValueInStorage(
                metaStorageManager,
                pendingPartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v)).size(),
                null,
                TIMEOUT_MILLIS
        );

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v)).size(),
                null,
                TIMEOUT_MILLIS
        );
    }
}
