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
import org.junit.jupiter.api.Test;

/** Test suite for the cases with a recovery of the group replication factor after reset by zone filter update. */
public class ItHighAvailablePartitionsRecoveryByFilterUpdateTest extends AbstractHighAvailablePartitionsRecoveryTest {
    public static final String EU_NODES_CONFIG = "ignite {\n"
            + "  nodeAttributes.nodeAttributes: {region.attribute = EU, zone.attribute = global},\n"
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

    public static final String EU_ONLY_NODES_CONFIG = "ignite {\n"
            + "  nodeAttributes.nodeAttributes: {region.attribute = EU},\n"
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

    public static final String GLOBAL_NODES_CONFIG = "ignite {\n"
            + "  nodeAttributes.nodeAttributes: {zone.attribute = global},\n"
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

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return EU_NODES_CONFIG;
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

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, Set.of(0, 1), euNodes);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1, 2);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, Set.of(0, 1), nodeNames(0));

        String globalFilter = "$[?(@.zone == \"global\")]";

        alterZoneSql(globalFilter, HA_ZONE_NAME);

        Set<String> globalNodes = nodeNames(0, 3, 4);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, Set.of(0, 1), globalNodes);
    }

    private void alterZoneSql(String filter, String zoneName) {
        executeSql(String.format("ALTER ZONE \"%s\" SET \"DATA_NODES_FILTER\" = '%s'", zoneName, filter));
    }
}
