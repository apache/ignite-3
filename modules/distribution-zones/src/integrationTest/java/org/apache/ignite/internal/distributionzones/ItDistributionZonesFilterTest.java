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

package org.apache.ignite.internal.distributionzones;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.Session;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

/**
 * Integration test for data nodes' filters functionality.
 */
public class ItDistributionZonesFilterTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "table1";

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
                + "  clientConnector: { port:{} },\n"
                + "  rest.port: {}\n"
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
        String filter = "$[?(@.region == \"US\" && @.storage == \"SSD\")]";

        // This node do not pass the filter
        @Language("JSON") String firstNodeAttributes = "{region:{attribute:\"EU\"},storage:{attribute:\"SSD\"}}";

        IgniteImpl node = startNode(1, createStartConfig(firstNodeAttributes));

        Session session = node.sql().createSession();

        session.execute(null, createZoneSql(2, 3, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, filter));

        session.execute(null, createTableSql());

        MetaStorageManager metaStorageManager = (MetaStorageManager) IgniteTestUtils
                .getFieldValue(node, IgniteImpl.class, "metaStorageMgr");

        TableManager tableManager = (TableManager) IgniteTestUtils.getFieldValue(node, IgniteImpl.class, "distributedTblMgr");

        TableViewInternal table = (TableViewInternal) tableManager.table(TABLE_NAME);

        TablePartitionId partId = new TablePartitionId(table.tableId(), 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v)).size(),
                1,
                TIMEOUT_MILLIS
        );

        @Language("JSON") String secondNodeAttributes = "{region:{attribute:\"US\"},storage:{attribute:\"SSD\"}}";

        // This node pass the filter
        startNode(2, createStartConfig(secondNodeAttributes));

        int zoneId = getZoneId(node);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(zoneId),
                (v) -> ((Map<Node, Integer>) fromBytes(v)).size(),
                3,
                TIMEOUT_MILLIS
        );

        Entry dataNodesEntry1 = metaStorageManager.get(zoneDataNodesKey(zoneId)).get(5_000, MILLISECONDS);

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= dataNodesEntry1.revision(), TIMEOUT_MILLIS));

        // We check that two nodes that pass the filter are presented in the stable key.
        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v))
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(node(0).name(), node(2).name()),
                TIMEOUT_MILLIS * 2
        );
    }

    /**
     * Tests the scenario when altering filter triggers immediate scale up so data nodes
     * and stable key for rebalance is changed to the new value even if scale up timer is big enough.
     *
     * @throws Exception If failed.
     */
    @Test
    void testAlteringFiltersPropagatedDataNodesToStableImmediately() throws Exception {
        String filter = "$[?(@.region == \"US\" && @.storage == \"SSD\")]";

        IgniteImpl node0 = node(0);

        Session session = node0.sql().createSession();

        session.execute(null, createZoneSql(2, 3, 10_000, 10_000, filter));

        session.execute(null, createTableSql());

        MetaStorageManager metaStorageManager = (MetaStorageManager) IgniteTestUtils
                .getFieldValue(node0, IgniteImpl.class, "metaStorageMgr");

        TableManager tableManager = (TableManager) IgniteTestUtils.getFieldValue(node0, IgniteImpl.class, "distributedTblMgr");

        TableViewInternal table = (TableViewInternal) tableManager.table(TABLE_NAME);

        TablePartitionId partId = new TablePartitionId(table.tableId(), 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v))
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(node(0).name()),
                TIMEOUT_MILLIS
        );

        @Language("JSON") String firstNodeAttributes = "{region:{attribute:\"US\"},storage:{attribute:\"SSD\"}}";

        // This node pass the filter
        startNode(1, createStartConfig(firstNodeAttributes));

        // Expected size is 1 because we have timers equals to 10000, so no scale up will be propagated.
        waitDataNodeAndListenersAreHandled(metaStorageManager, 1, getZoneId(node0));

        session.execute(null, alterZoneSql(DEFAULT_FILTER));

        // We check that all nodes that pass the filter are presented in the stable key because altering filter triggers immediate scale up.
        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v))
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(node(0).name(), node(1).name()),
                TIMEOUT_MILLIS * 2
        );
    }

    /**
     * Tests the scenario when empty data nodes are not propagated to stable after filter is altered, because there are no node that
     * matches the new filter.
     *
     * @throws Exception If failed.
     */
    @Test
    void testEmptyDataNodesDoNotPropagatedToStableAfterAlteringFilter() throws Exception {
        String filter = "$[?(@.region == \"US\" && @.storage == \"SSD\")]";

        IgniteImpl node0 = node(0);

        Session session = node0.sql().createSession();

        session.execute(null, createZoneSql(2, 3, 10_000, 10_000, filter));

        session.execute(null, createTableSql());

        MetaStorageManager metaStorageManager = (MetaStorageManager) IgniteTestUtils
                .getFieldValue(node0, IgniteImpl.class, "metaStorageMgr");

        TableManager tableManager = (TableManager) IgniteTestUtils.getFieldValue(node0, IgniteImpl.class, "distributedTblMgr");

        TableViewInternal table = (TableViewInternal) tableManager.table(TABLE_NAME);

        TablePartitionId partId = new TablePartitionId(table.tableId(), 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v))
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(node(0).name()),
                TIMEOUT_MILLIS
        );

        @Language("JSON") String firstNodeAttributes = "{region:{attribute:\"US\"},storage:{attribute:\"SSD\"}}";

        // This node pass the filter
        startNode(1, createStartConfig(firstNodeAttributes));

        int zoneId = getZoneId(node0);

        // Expected size is 2 because we have timers equals to 10000, so no scale up will be propagated.
        waitDataNodeAndListenersAreHandled(metaStorageManager, 1, zoneId);

        // There is no node that match the filter
        String newFilter = "$[?(@.region == \"FOO\" && @.storage == \"BAR\")]";

        session.execute(null, alterZoneSql(newFilter));

        waitDataNodeAndListenersAreHandled(metaStorageManager, 2, zoneId);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v))
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(node(0).name()),
                TIMEOUT_MILLIS
        );
    }

    /**
     * Tests the scenario when removal node from the logical topology leads to empty data nodes, but this empty data nodes do not trigger
     * rebalance. Data nodes become empty after applying corresponding filter.
     *
     * @throws Exception If failed.
     */
    @Test
    void testFilteredEmptyDataNodesDoNotTriggerRebalance() throws Exception {
        String filter = "$[?(@.region == \"EU\" && @.storage == \"HDD\")]";

        // This node do not pass the filter.
        IgniteImpl node0 = node(0);

        // This node passes the filter
        @Language("JSON") String firstNodeAttributes = "{region:{attribute:\"EU\"},storage:{attribute:\"HDD\"}}";

        IgniteImpl node1 = startNode(1, createStartConfig(firstNodeAttributes));

        Session session = node1.sql().createSession();

        session.execute(null, createZoneSql(1, 1, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, filter));

        MetaStorageManager metaStorageManager = (MetaStorageManager) IgniteTestUtils.getFieldValue(
                node0,
                IgniteImpl.class,
                "metaStorageMgr"
        );

        int zoneId = getZoneId(node0);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 2, zoneId);

        session.execute(null, createTableSql());

        TableManager tableManager = (TableManager) IgniteTestUtils.getFieldValue(node0, IgniteImpl.class, "distributedTblMgr");

        TableViewInternal table = (TableViewInternal) tableManager.table(TABLE_NAME);

        TablePartitionId partId = new TablePartitionId(table.tableId(), 0);

        // Table was created after both nodes was up, so there wasn't any rebalance.
        assertPendingAssignmentsWereNeverExist(metaStorageManager, partId);

        // Stop node, that was only one, that passed the filter, so data nodes after filtering will be empty.
        stopNode(1);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 1, zoneId);

        //Check that pending are null, so there wasn't any rebalance.
        assertPendingAssignmentsWereNeverExist(metaStorageManager, partId);
    }

    @Test
    void testFilteredEmptyDataNodesDoNotTriggerRebalanceOnReplicaUpdate() throws Exception {
        String filter = "$[?(@.region == \"EU\" && @.storage == \"HDD\")]";

        // This node do not pass the filter.
        IgniteImpl node0 = node(0);

        // This node passes the filter
        @Language("JSON") String firstNodeAttributes = "{region:{attribute:\"EU\"},storage:{attribute:\"HDD\"}}";

        startNode(1, createStartConfig(firstNodeAttributes));

        Session session = node0.sql().createSession();

        session.execute(null, createZoneSql(1, 1, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, filter));

        MetaStorageManager metaStorageManager = (MetaStorageManager) IgniteTestUtils.getFieldValue(
                node0,
                IgniteImpl.class,
                "metaStorageMgr"
        );

        int zoneId = getZoneId(node0);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 2, zoneId);

        session.execute(null, createTableSql());

        TableManager tableManager = (TableManager) IgniteTestUtils.getFieldValue(node0, IgniteImpl.class, "distributedTblMgr");

        TableViewInternal table = (TableViewInternal) tableManager.table(TABLE_NAME);

        TablePartitionId partId = new TablePartitionId(table.tableId(), 0);

        // Table was created after both nodes was up, so there wasn't any rebalance.
        assertPendingAssignmentsWereNeverExist(metaStorageManager, partId);

        // Stop node, that was only one, that passed the filter, so data nodes after filtering will be empty.
        stopNode(1);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 1, zoneId);

        //Check that stable and pending are null, so there wasn't any rebalance.
        assertPendingAssignmentsWereNeverExist(metaStorageManager, partId);

        session.execute(null, alterZoneSql(2));

        CountDownLatch latch = new CountDownLatch(1);

        // We need to be sure, that the first asynchronous catalog change of replica was handled,
        // so we create a listener with a latch, and change replica again and wait for latch, so we can be sure that the first
        // replica was handled.
        node0.catalogManager().listen(CatalogEvent.ZONE_ALTER, (parameters, exception) -> {
            latch.countDown();

            return falseCompletedFuture();
        });

        session.execute(null, alterZoneSql(3));

        assertTrue(latch.await(10_000, MILLISECONDS));

        //Check that stable and pending are null, so there wasn't any rebalance.
        assertPendingAssignmentsWereNeverExist(metaStorageManager, partId);
    }

    private static void waitDataNodeAndListenersAreHandled(
            MetaStorageManager metaStorageManager,
            int expectedDataNodesSize,
            int zoneId
    ) throws Exception {
        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(zoneId),
                (v) -> ((Map<Node, Integer>) fromBytes(v)).size(),
                expectedDataNodesSize,
                TIMEOUT_MILLIS
        );

        ByteArray fakeKey = new ByteArray("Foo");

        metaStorageManager.put(fakeKey, toBytes("Bar")).get(5_000, MILLISECONDS);

        Entry fakeEntry = metaStorageManager.get(fakeKey).get(5_000, MILLISECONDS);

        // We wait for all data nodes listeners are triggered and all their meta storage activity is done.
        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= fakeEntry.revision(), 5_000));

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(zoneId),
                (v) -> ((Map<Node, Integer>) fromBytes(v)).size(),
                expectedDataNodesSize,
                TIMEOUT_MILLIS
        );
    }

    private static void assertPendingAssignmentsWereNeverExist(
            MetaStorageManager metaStorageManager,
            TablePartitionId partId
    ) throws InterruptedException, ExecutionException {
        assertTrue(metaStorageManager.get(pendingPartAssignmentsKey(partId)).get().empty());
    }

    private static String createZoneSql(int partitions, int replicas, int scaleUp, int scaleDown, String filter) {
        String sqlFormat = "CREATE ZONE \"%s\" WITH "
                + "\"REPLICAS\" = %s, "
                + "\"PARTITIONS\" = %s, "
                + "\"DATA_NODES_FILTER\" = '%s', "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = %s, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = %s";

        return String.format(sqlFormat, ZONE_NAME, replicas, partitions, filter, scaleUp, scaleDown);
    }

    private static String alterZoneSql(String filter) {
        return String.format("ALTER ZONE \"%s\" SET \"DATA_NODES_FILTER\" = '%s'", ZONE_NAME, filter);
    }

    private static String alterZoneSql(int replicas) {
        return String.format("ALTER ZONE \"%s\" SET \"REPLICAS\" = %s", ZONE_NAME, replicas);
    }

    private static String createTableSql() {
        return String.format(
                "CREATE TABLE %s(%s INT PRIMARY KEY, %s VARCHAR) WITH PRIMARY_ZONE='%s'",
                TABLE_NAME, COLUMN_KEY, COLUMN_VAL, ZONE_NAME
        );
    }

    private static int getZoneId(IgniteImpl node) {
        return DistributionZonesTestUtil.getZoneIdStrict(node.catalogManager(), ZONE_NAME, node.clock().nowLong());
    }
}
