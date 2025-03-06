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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.table.Table;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Test suite for the cases with a recovery of the group replication factor after reset by zone filter update. */
public class ItHighAvailablePartitionsRecoveryByFilterUpdateTest extends AbstractHighAvailablePartitionsRecoveryTest {
    private static final String GLOBAL_EU_NODES_CONFIG =
            nodeConfig("{region = EU, zone = global}", "{segmented_aipersist.engine = aipersist}");

    private static final String EU_ONLY_NODES_CONFIG = nodeConfig("{region = EU}", null);

    private static final String US_ONLY_NODES_CONFIG = nodeConfig("{region = US}", null);

    private static final String GLOBAL_NODES_CONFIG = nodeConfig("{zone = global}", null);

    private static final String CUSTOM_NODES_CONFIG = nodeConfig("{zone = custom}", null);

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

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24111")
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

    /**
     * Test scenario.
     * <ol>
     *   <li>Create a zone in HA mode (7 nodes, A, B, C, D, E, F, G) - phase 1</li>
     *   <li>Insert data and wait for replication to all nodes.</li>
     *   <li>Stop a majority of nodes (4 nodes A, B, C, D)</li>
     *   <li>Wait for the partition to become available (E, F, G), no new writes - phase 2</li>
     *   <li>Stop a majority of nodes once again (E, F)</li>
     *   <li>Wait for the partition to become available (G), no new writes - phase 3</li>
     *   <li>Stop the last node G</li>
     *   <li>Start one node from phase 1, A</li>
     *   <li>Start one node from phase 3, G</li>
     *   <li>Start one node from phase 2, E</li>
     *   <li>No data should be lost (reads from partition on A and E must be consistent with G)</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24513")
    void testSeveralHaResetsAndSomeNodeRestart() throws Exception {
        for (int i = 1; i < 8; i++) {
            startNode(i, CUSTOM_NODES_CONFIG);
        }

        String globalFilter = "$[?(@.zone == \"custom\")]";
        createHaZoneWithTable(7, globalFilter, nodeNames(1, 2, 3, 4, 5, 6, 7));

        IgniteImpl node0 = igniteImpl(0);
        Table table = node0.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);
        assertThat(errors, is(empty()));
        assertValuesPresentOnNodes(node0.clock().now(), table, 1, 2, 3, 4, 5, 6, 7);

        // Stop 4 nodes (A, B, C, D)
        stopNodes(4, 5, 6, 7);

        // Wait for the partition to become available on the remaining nodes (E, F, G)
        waitAndAssertStableAssignmentsOfPartitionEqualTo(node0, HA_TABLE_NAME, PARTITION_IDS, nodeNames(1, 2, 3));

        // Stop 2 more nodes (E, F)
        stopNodes(2, 3);

        // Wait for the partition to become available on the last node (G)
        waitAndAssertStableAssignmentsOfPartitionEqualTo(node0, HA_TABLE_NAME, PARTITION_IDS, nodeNames(1));

        // Stop the last node (G)
        stopNode(1);

        // Start one node from phase 1 (A)
        startNode(4);

        // Start one node from phase 3 (G)
        startNode(1);

        //  Start one node from phase 2 (E)
        startNode(2);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node0, HA_TABLE_NAME, PARTITION_IDS, nodeNames(1, 2, 4));

        // Verify that no data is lost and reads from partition on nodes A and E are consistent with node G
        assertValuesPresentOnNodes(node0.clock().now(), table, 1, 2, 4);
    }

    /**
     * Test scenario, where we start nodes from the previous assignments chain, with new writes.
     * The whole scenario will be possible when second phase of HA feature will be implemented.
     *
     * <ol>
     *   <li>Create a zone in HA mode with 7 nodes (A, B, C, D, E, F, G).</li>
     *   <li>Stop a majority of nodes (4 nodes A, B, C, D).</li>
     *   <li>Wait for the partition to become available on the remaining nodes (E, F, G).</li>
     *   <li>Stop a majority of nodes (E, F).</li>
     *   <li>Write data to node G.</li>
     *   <li>Stop node G.</li>
     *   <li>Start nodes E and F.</li>
     *   <li>Nodes should wait for node G to come back online.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24509")
    void testNodesWaitForLastNodeFromChainToComeBackOnlineAfterMajorityStops() throws Exception {
        for (int i = 1; i < 8; i++) {
            startNode(i, CUSTOM_NODES_CONFIG);
        }

        String globalFilter = "$[?(@.zone == \"custom\")]";
        createHaZoneWithTable(7, globalFilter, nodeNames(1, 2, 3, 4, 5, 6, 7));

        IgniteImpl node0 = igniteImpl(0);
        Table table = node0.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);
        assertThat(errors, is(empty()));
        assertValuesPresentOnNodes(node0.clock().now(), table, 1, 2, 3, 4, 5, 6, 7);

        // Stop 4 nodes (A, B, C, D)
        stopNodes(4, 5, 6, 7);

        // Wait for the partition to become available on the remaining nodes (E, F, G)
        waitAndAssertStableAssignmentsOfPartitionEqualTo(node0, HA_TABLE_NAME, PARTITION_IDS, nodeNames(1, 2, 3));

        // Stop 2 more nodes (E, F)
        stopNodes(2, 3);

        // Wait for the partition to become available on the last node (G)
        waitAndAssertStableAssignmentsOfPartitionEqualTo(node0, HA_TABLE_NAME, PARTITION_IDS, nodeNames(1));

        errors = insertValues(table, 1000);
        assertThat(errors, is(empty()));

        assertValuesPresentOnNodes(node0.clock().now(), table, 1);

        // Stop the last node (G)
        stopNode(1);

        // Start one node from phase 3 (E)
        startNode(2);

        //  Start one node from phase 2 (F)
        startNode(3);

        assertValuesNotPresentOnNodes(node0.clock().now(), table,  2, 3);
    }

    /**
     * Test scenario, where we start nodes from the previous assignments chain, without new writes.
     * The whole scenario will be possible when second phase of HA feature will be implemented.
     * <ol>
     *   <li>Create a zone in HA mode with 7 nodes (A, B, C, D, E, F, G).</li>
     *   <li>Stop a majority of nodes (4 nodes A, B, C, D).</li>
     *   <li>Wait for the partition to become available on the remaining nodes (E, F, G).</li>
     *   <li>Stop a majority of nodes (E, F).</li>
     *   <li>Stop node G.</li>
     *   <li>Start nodes E and F.</li>
     *   <li>Nodes should wait for nodes A, B, C, D, E, F, G to come back online.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24509")
    void testNodesWaitForNodesFromGracefulChainToComeBackOnlineAfterMajorityStops() throws Exception {
        for (int i = 1; i < 8; i++) {
            startNode(i, CUSTOM_NODES_CONFIG);
        }

        String globalFilter = "$[?(@.zone == \"custom\")]";
        createHaZoneWithTable(7, globalFilter, nodeNames(1, 2, 3, 4, 5, 6, 7));

        IgniteImpl node0 = igniteImpl(0);
        Table table = node0.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);
        assertThat(errors, is(empty()));
        assertValuesPresentOnNodes(node0.clock().now(), table, 1, 2, 3, 4, 5, 6, 7);

        // Stop 4 nodes (A, B, C, D)
        stopNodes(4, 5, 6, 7);

        // Wait for the partition to become available on the remaining nodes (E, F, G)
        waitAndAssertStableAssignmentsOfPartitionEqualTo(node0, HA_TABLE_NAME, PARTITION_IDS, nodeNames(1, 2, 3));

        // Stop 2 more nodes (E, F)
        stopNodes(2, 3);

        // Wait for the partition to become available on the last node (G)
        waitAndAssertStableAssignmentsOfPartitionEqualTo(node0, HA_TABLE_NAME, PARTITION_IDS, nodeNames(1));

        // Stop the last node (G)
        stopNode(1);

        // Start one node from phase 3 (E)
        startNode(2);

        //  Start one node from phase 2 (F)
        startNode(3);

        assertValuesNotPresentOnNodes(node0.clock().now(), table,  2, 3);
    }

    private void alterZoneSql(String filter, String zoneName) {
        executeSql(String.format("ALTER ZONE \"%s\" SET \"DATA_NODES_FILTER\" = '%s'", zoneName, filter));
    }

    /**
     * Test scenario.
     * <ol>
     *   <li>Create a zone in HA mode (node filter allows A, B, C)</li>
     *   <li>Set 'partitionDistributionResetTimeout' to 5 minutes</li>
     *   <li>Insert data and wait for replication to all nodes.</li>
     *   <li>Stop a majority of nodes (B, C)</li>
     *   <li>Change filter to allow (D, E, F) before 'partitionDistributionResetTimeout' expires</li>
     *   <li>Set 'partitionDistributionResetTimeout' to 0 to trigger automatic reset</li>
     *   <li>Verify the partition is on D,E,F</li>
     *   <li>No data should be lost</li>
     * </ol>
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24467")
    @Test
    void testResetAfterChangeFilters() throws InterruptedException {
        startNode(1, EU_ONLY_NODES_CONFIG);
        startNode(2, EU_ONLY_NODES_CONFIG);
        startNode(3, US_ONLY_NODES_CONFIG);
        startNode(4, US_ONLY_NODES_CONFIG);
        startNode(5, US_ONLY_NODES_CONFIG);

        String euFilter = "$[?(@.region == \"EU\")]";

        Set<String> euNodes = nodeNames(0, 1, 2);

        createHaZoneWithTable(euFilter, euNodes);

        IgniteImpl node = igniteImpl(0);

        changePartitionDistributionTimeout(node, (int) TimeUnit.MINUTES.toSeconds(5));

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, euNodes);

        assertRecoveryKeyIsEmpty(node);

        Table table = node.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);
        assertThat(errors, is(empty()));

        assertValuesPresentOnNodes(node.clock().now(), table, 0, 1, 2);

        stopNodes(1, 2);

        String globalFilter = "$[?(@.region == \"US\")]";

        alterZoneSql(globalFilter, HA_ZONE_NAME);

        changePartitionDistributionTimeout(node, 0);

        Set<String> usNodes = nodeNames(3, 4, 5);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, usNodes);

        assertValuesPresentOnNodes(node.clock().now(), table, 3, 4, 5);
    }

    /**
     * Test scenario.
     * <ol>
     *   <li>Create a zone in HA mode (profile filter allows A, B, C)</li>
     *   <li>Set 'partitionDistributionResetTimeout' to 5 minutes</li>
     *   <li>Insert data and wait for replication to all nodes.</li>
     *   <li>Stop a majority of nodes (B, C)</li>
     *   <li>Change filter to allow (D, E, F) before 'partitionDistributionResetTimeout' expires</li>
     *   <li>Set 'partitionDistributionResetTimeout' to 0 to trigger automatic reset</li>
     *   <li>Verify the partition is on D,E,F</li>
     *   <li>No data should be lost</li>
     * </ol>
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24467")
    @Test
    void testResetAfterChangeStorageProfiles() throws InterruptedException {
        startNode(1, AIPERSIST_NODES_CONFIG);
        startNode(2, AIPERSIST_NODES_CONFIG);
        startNode(3, ROCKS_NODES_CONFIG);
        startNode(4, ROCKS_NODES_CONFIG);
        startNode(5, ROCKS_NODES_CONFIG);

        Set<String> nodesWithAiProfile = nodeNames(0, 1, 2);

        createHaZoneWithTableWithStorageProfile(2, "segmented_aipersist", nodesWithAiProfile);

        IgniteImpl node = igniteImpl(0);

        changePartitionDistributionTimeout(node, (int) TimeUnit.MINUTES.toSeconds(5));

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, nodesWithAiProfile);

        assertRecoveryKeyIsEmpty(node);

        Table table = node.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);
        assertThat(errors, is(empty()));

        assertValuesPresentOnNodes(node.clock().now(), table, 0, 1, 2);

        stopNodes(1, 2);

        alterZoneStorageProfiles(node, HA_ZONE_NAME, "lru_rocks");

        changePartitionDistributionTimeout(node, 0);

        Set<String> usNodes = nodeNames(3, 4, 5);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, usNodes);

        assertValuesPresentOnNodes(node.clock().now(), table, 3, 4, 5);
    }

    private void alterZoneStorageProfiles(IgniteImpl node, String zoneName, String storageProfile) {
        AlterZoneCommandBuilder builder = AlterZoneCommand.builder().zoneName(zoneName);

        builder.storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(storageProfile).build()));

        assertThat(node.catalogManager().execute(builder.build()), willCompleteSuccessfully());
    }

    private static String nodeConfig(
            @Nullable @Language("HOCON") String nodeAtrributes,
            @Nullable @Language("HOCON") String storageProfiles
    ) {
        return "ignite {\n"
                + "  nodeAttributes.nodeAttributes: " + nodeAtrributes + ",\n"
                + (storageProfiles == null ? "" : "  storage.profiles: " + storageProfiles + ",\n")
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
