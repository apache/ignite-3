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

import java.util.Set;
import org.apache.ignite.internal.app.IgniteImpl;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** Test suite for the cases with a recovery of the group replication factor after reset by zone filter update. */
public class ItHighAvailablePartitionsRecoveryByFilterUpdateTest extends AbstractHighAvailablePartitionsRecoveryTest {
    private static final String GLOBAL_EU_NODES_CONFIG =
            nodeConfig("{region = EU, zone = global}", "{segmented_aipersist.engine = aipersist}");

    private static final String EU_ONLY_NODES_CONFIG = nodeConfig("{region = EU}", null);

    private static final String GLOBAL_NODES_CONFIG = nodeConfig("{zone = global}", null);

    private static final String ROCKS_NODES_CONFIG = nodeConfig(null, "{lru_rocks.engine = rocksdb}");

    private static final String AIPERSIST_NODES_CONFIG = nodeConfig(null, "{segmented_aipersist.engine = aipersist}");

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return GLOBAL_EU_NODES_CONFIG;
    }

    @Test
    void testScaleUpAfterZoneFilterUpdate() throws InterruptedException {
        startNode(1, EU_ONLY_NODES_CONFIG);
        startNode(2, EU_ONLY_NODES_CONFIG);
        startNode(3, GLOBAL_NODES_CONFIG);
        startNode(4, GLOBAL_NODES_CONFIG);

        String euFilter = "$[?(@.region == \"EU\")]";

        Set<String> euNodes = nodeNames(0, 1, 2);

        createHaZoneWithTable(euFilter, euNodes);

        IgniteImpl node = igniteImpl(0);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, euNodes);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1, 2);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, nodeNames(0));

        String globalFilter = "$[?(@.zone == \"global\")]";

        alterZoneSql(globalFilter, HA_ZONE_NAME);

        Set<String> globalNodes = nodeNames(0, 3, 4);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, globalNodes);
    }

    @Test
    void testThatPartitionResetZoneFilterAware() throws InterruptedException {
        startNode(1, EU_ONLY_NODES_CONFIG);
        startNode(2, GLOBAL_NODES_CONFIG);

        String euFilter = "$[?(@.region == \"EU\")]";

        Set<String> euNodes = nodeNames(0, 1);

        createHaZoneWithTable(2, euFilter, euNodes);

        IgniteImpl node = igniteImpl(0);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, euNodes);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1);

        // Due to the fact that only one [0] node is suitable according to filter:
        waitThatAllRebalancesHaveFinishedAndStableAssignmentsEqualsToExpected(node, HA_TABLE_NAME, PARTITION_IDS, nodeNames(0));
    }

    @Test
    void testThatPartitionResetZoneStorageProfileFilterAware() throws InterruptedException {
        startNode(1, AIPERSIST_NODES_CONFIG);
        startNode(2, ROCKS_NODES_CONFIG);

        Set<String> nodesWithAiProfile = nodeNames(0, 1);

        createHaZoneWithTableWithStorageProfile(2, "segmented_aipersist", nodesWithAiProfile);

        IgniteImpl node = igniteImpl(0);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, nodesWithAiProfile);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1);

        // Due to the fact that only one [0] node is suitable according to storage profiles:
        waitThatAllRebalancesHaveFinishedAndStableAssignmentsEqualsToExpected(node, HA_TABLE_NAME, PARTITION_IDS, nodeNames(0));
    }

    private void alterZoneSql(String filter, String zoneName) {
        executeSql(String.format("ALTER ZONE \"%s\" SET \"DATA_NODES_FILTER\" = '%s'", zoneName, filter));
    }

    private static String nodeConfig(
            @Nullable @Language("HOCON") String nodeAtrributes,
            @Nullable @Language("HOCON") String storageProfiles
    ) {
        return "ignite {\n"
                + "  nodeAttributes.nodeAttributes: " + nodeAtrributes + ",\n"
                + "  storage.profiles: " + storageProfiles + ",\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ {} ]\n"
                + "    },\n"
                + "    membership: {\n"
                + "      membershipSyncInterval: 1000,\n"
                + "      failurePingInterval: 500,\n"
                + "      scaleCube: {\n"
                + "        membershipSuspicionMultiplier: 1,\n"
                + "        failurePingRequestMembers: 1,\n"
                + "        gossipInterval: 10\n"
                + "      },\n"
                + "    }\n"
                + "  },\n"
                + "  clientConnector: { port:{} }, \n"
                + "  rest.port: {}\n"
                + "}";
    }
}
