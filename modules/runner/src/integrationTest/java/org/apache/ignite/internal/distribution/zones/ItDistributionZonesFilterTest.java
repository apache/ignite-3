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
import static org.apache.ignite.internal.utils.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.stablePartAssignmentsKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.NodeWithAttributes;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.Session;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

/**
 * Integration test for filters functionality.
 */
public class ItDistributionZonesFilterTest extends ClusterPerTestIntegrationTest {
    @Language("JSON")
    private static final String NODE_ATTRIBUTES = "{region:{attribute:\"US\"},storage:{attribute:\"SSD\"}}";

    protected static final String COLUMN_KEY = "key";

    protected static final String COLUMN_VAL = "val";

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

    @Test
    void testFilteredDataNodesPropagatedToStable() throws Exception {
        // This node do not pass the filter
        @Language("JSON") String firstNodeAttributes = "{region:{attribute:\"EU\"},storage:{attribute:\"SSD\"}}";

        IgniteImpl node = startNode(1, createStartConfig(firstNodeAttributes));

        Session session = node.sql().createSession();

        session.execute(null, "CREATE ZONE \"TEST_ZONE\" WITH "
                + "\"REPLICAS\" = 3, "
                + "\"PARTITIONS\" = 2, "
                + "\"DATA_NODES_FILTER\" = '$[?(@.storage == \"SSD\" && @.region == \"US\")]', "
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
                10_000
        );

        @Language("JSON") String secondNodeAttributes = "{region:{attribute:\"US\"},storage:{attribute:\"SSD\"}}";

        startNode(2, createStartConfig(secondNodeAttributes));

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(1),
                (v) -> ((Map<String, Integer>) fromBytes(v)).size(),
                3,
                10_000
        );

        Entry dataNodesEntry = metaStorageManager.get(zoneDataNodesKey(1)).get(5_000, MILLISECONDS);

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= dataNodesEntry.revision(), 10_000));

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v)).size(),
                2,
                10_000
        );

        byte[] stableAssignments = metaStorageManager.get(stablePartAssignmentsKey(partId)).get(5_000, MILLISECONDS).value();

        assertNotNull(stableAssignments);

        Set<String> stable = ((Set<Assignment>) fromBytes(stableAssignments)).stream()
                .map(Assignment::consistentId)
                .collect(Collectors.toSet());

        assertEquals(2, stable.size());

        assertTrue(stable.contains(node(0).name()) && stable.contains(node(2).name()));
    }

    @Test
    void testFilteredEmptyDataNodesDoNotTriggerRebalance() throws Exception {
        IgniteImpl node0 = node(0);

        // This node pass the filter
        @Language("JSON") String firstNodeAttributes = "{region:{attribute:\"EU\"},storage:{attribute:\"HDD\"}}";

        IgniteImpl node1 = startNode(1, createStartConfig(firstNodeAttributes));

        Session session = node1.sql().createSession();

        session.execute(null, "CREATE ZONE \"TEST_ZONE\" WITH "
                + "\"REPLICAS\" = 1, "
                + "\"PARTITIONS\" = 1, "
                + "\"DATA_NODES_FILTER\" = '$[?(@.storage == \"HDD\" && @.region == \"EU\")]', "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = 0, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = 0");

        String tableName = "table1";

        session.execute(null, "CREATE TABLE " + tableName + "("
                + COLUMN_KEY + " INT PRIMARY KEY, " + COLUMN_VAL + " VARCHAR) WITH PRIMARY_ZONE='TEST_ZONE'");

        MetaStorageManager metaStorageManager = (MetaStorageManager) IgniteTestUtils
                .getFieldValue(node0, IgniteImpl.class, "metaStorageMgr");

        TableManager tableManager = (TableManager) IgniteTestUtils.getFieldValue(node0, IgniteImpl.class, "distributedTblMgr");

        TableImpl table = (TableImpl) tableManager.table(tableName);

        TablePartitionId partId = new TablePartitionId(table.tableId(), 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v)).size(),
                null,
                5_000
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(1),
                (v) -> ((Map<NodeWithAttributes, Integer>) fromBytes(v)).size(),
                2,
                5_000
        );

        Entry dataNodesEntry = metaStorageManager.get(zoneDataNodesKey(1)).get(5_000, MILLISECONDS);

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= dataNodesEntry.revision(), 5_000));

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v)).size(),
                null,
                5_000
        );

        stopNode(1);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(1),
                (v) -> ((Map<NodeWithAttributes, Integer>) fromBytes(v)).size(),
                1,
                10_000
        );

        Entry newDataNodesEntry = metaStorageManager.get(zoneDataNodesKey(1)).get(5_000, MILLISECONDS);

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= newDataNodesEntry.revision(), 5_000));

        assertValueInStorage(
                metaStorageManager,
                pendingPartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v)).size(),
                null,
                5_000
        );

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> ((Set<Assignment>) fromBytes(v)).size(),
                null,
                5_000
        );
    }
}
