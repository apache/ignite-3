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

import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopologyInMetastorage;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeFailureHandlerConfigurationSchema;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.table.Table;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Test suite for the cases with a recovery of the group replication factor after reset by zone filter update. */
public class ItHighAvailablePartitionsRecoveryByFilterUpdateTest extends AbstractHighAvailablePartitionsRecoveryTest {
    private static final String GLOBAL_EU_NODES_CONFIG =
            nodeConfigWithFailureHandler("{region = EU, zone = global}", "{segmented_aipersist.engine = aipersist}");

    private static final String EU_ONLY_NODES_CONFIG = nodeConfigWithFailureHandler("{region = EU}", null);

    private static final String US_ONLY_NODES_CONFIG = nodeConfigWithFailureHandler("{region = US}", null);

    private static final String GLOBAL_NODES_CONFIG = nodeConfigWithFailureHandler("{zone = global}", null);

    private static final String CUSTOM_NODES_CONFIG = nodeConfigWithFailureHandler("{zone = custom}", null);

    private static final String ROCKS_NODES_CONFIG = nodeConfigWithFailureHandler(null, "{lru_rocks.engine = rocksdb}");

    private static final String AIPERSIST_NODES_CONFIG = nodeConfigWithFailureHandler(null, "{segmented_aipersist.engine = aipersist}");

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return GLOBAL_EU_NODES_CONFIG;
    }

    private static String nodeConfigWithFailureHandler(
            @Nullable String nodeAttributes,
            @Nullable String storageProfiles
    ) {
        return "ignite {\n"
                + "  nodeAttributes.nodeAttributes: " + nodeAttributes + ",\n"
                + (storageProfiles == null ? "" : "  storage.profiles: " + storageProfiles + ",\n")
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ {} ]\n"
                + "    },\n"
                + "    membership: {\n"
                + "      membershipSyncIntervalMillis: 1000,\n"
                + "      failurePingIntervalMillis: 500,\n"
                + "      scaleCube: {\n"
                + "        membershipSuspicionMultiplier: 1,\n"
                + "        failurePingRequestMembers: 1,\n"
                + "        gossipIntervalMillis: 10\n"
                + "      },\n"
                + "    }\n"
                + "  },\n"
                + "  clientConnector: { port:{} }, \n"
                + "  rest.port: {},\n"
                + "  failureHandler: {\n"
                + "    handler: {\n"
                + "      type: \"" + StopNodeFailureHandlerConfigurationSchema.TYPE + "\"\n"
                + "    },\n"
                + "    dumpThreadsOnFailure: false\n"
                + "  }\n"
                + "}";
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-25285")
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

        createHaZoneWithTable(euFilter, euNodes);

        IgniteImpl node = igniteImpl(0);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, euNodes);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1);

        // Due to the fact that only one [0] node is suitable according to filter:
        waitThatAllRebalancesHaveFinishedAndStableAssignmentsEqualsToExpected(node, HA_TABLE_NAME, PARTITION_IDS, nodeNames(0));
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-25285")
    void testThatPartitionResetZoneStorageProfileFilterAware() throws InterruptedException {
        startNode(1, AIPERSIST_NODES_CONFIG);
        startNode(2, ROCKS_NODES_CONFIG);

        Set<String> nodesWithAiProfile = nodeNames(0, 1);

        createHaZoneWithTableWithStorageProfile("segmented_aipersist", nodesWithAiProfile);

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
    void testSeveralHaResetsAndSomeNodeRestart() throws Exception {
        for (int i = 1; i < 8; i++) {
            startNode(i, CUSTOM_NODES_CONFIG);
        }

        String globalFilter = "$[?(@.zone == \"custom\")]";
        createHaZoneWithTable(globalFilter, nodeNames(1, 2, 3, 4, 5, 6, 7));

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

        startNodes(4, 1, 2);

        waitThatAllRebalancesHaveFinishedAndStableAssignmentsEqualsToExpected(node0, HA_TABLE_NAME, PARTITION_IDS,  nodeNames(1, 2, 4));

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
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-25285")
    void testNodesWaitForLastNodeFromChainToComeBackOnlineAfterMajorityStops() throws Exception {
        for (int i = 1; i < 8; i++) {
            startNode(i, CUSTOM_NODES_CONFIG);
        }

        String globalFilter = "$[?(@.zone == \"custom\")]";
        createHaZoneWithTable(globalFilter, nodeNames(1, 2, 3, 4, 5, 6, 7));

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

        assertPartitionsAreEmpty(HA_TABLE_NAME, PARTITION_IDS, 2, 3);
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
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-25285")
    void testNodesWaitForNodesFromGracefulChainToComeBackOnlineAfterMajorityStops() throws Exception {
        for (int i = 1; i < 8; i++) {
            startNode(i, CUSTOM_NODES_CONFIG);
        }

        String globalFilter = "$[?(@.zone == \"custom\")]";
        createHaZoneWithTable(globalFilter, nodeNames(1, 2, 3, 4, 5, 6, 7));

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

        assertPartitionsAreEmpty(HA_TABLE_NAME, PARTITION_IDS, 2, 3);
    }

    private void alterZoneSql(String filter, String zoneName) {
        executeSql(String.format("ALTER ZONE \"%s\"  SET (NODES FILTER '%s')", zoneName, filter));
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

        Set<LogicalNode> expectedNodes = Set.of(
                getLogicalNode(igniteImpl(0)),
                getLogicalNode(igniteImpl(3)),
                getLogicalNode(igniteImpl(4)),
                getLogicalNode(igniteImpl(5))
        );

        assertLogicalTopologyInMetastorage(expectedNodes, node.metaStorageManager());

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
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-27643")
    @Test
    void testResetAfterChangeStorageProfiles() throws InterruptedException {
        startNode(1, AIPERSIST_NODES_CONFIG);
        startNode(2, AIPERSIST_NODES_CONFIG);
        startNode(3, ROCKS_NODES_CONFIG);
        startNode(4, ROCKS_NODES_CONFIG);
        startNode(5, ROCKS_NODES_CONFIG);

        Set<String> nodesWithAiProfile = nodeNames(0, 1, 2);

        createHaZoneWithTableWithStorageProfile("segmented_aipersist", nodesWithAiProfile);

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

    @Test
    void testHaWhenAllVotingMembersAreLost() throws Exception {
        startNode(1, CUSTOM_NODES_CONFIG);
        startNode(2, CUSTOM_NODES_CONFIG);
        startNode(3, CUSTOM_NODES_CONFIG);
        startNode(4, CUSTOM_NODES_CONFIG);
        startNode(5, CUSTOM_NODES_CONFIG);

        String customFilter = "$[?(@.zone == \"custom\")]";

        // We set quorum to 2, so 3 nodes will be followers, and 2 - learners.
        createHaZoneWithTables(HA_ZONE_NAME, 1, customFilter, 2, List.of(HA_TABLE_NAME), nodeNames(1, 2, 3, 4, 5));

        IgniteImpl node0 = igniteImpl(0);
        Table table = node0.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);
        assertThat(errors, is(empty()));
        assertValuesPresentOnNodes(node0.clock().now(), table, 1, 2, 3, 4, 5);

        Set<Assignment> partitionAssignments = getPartitionClusterNodes(node0, HA_TABLE_NAME, 0);

        assertEquals(5, partitionAssignments.size());

        Set<Assignment> followers = partitionAssignments.stream().filter(Assignment::isPeer).collect(Collectors.toSet());

        assertEquals(3, followers.size());

        // Stop all followers.
        followers.forEach(n -> stopNode(n.consistentId()));

        Set<String> learners = partitionAssignments.stream()
                .filter(n -> !n.isPeer())
                .map(Assignment::consistentId)
                .collect(Collectors.toSet());

        IgniteImpl node = igniteImpl(nodeIndex(learners.iterator().next()));

        // Wait for the partition to become available on the learners.
        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, Set.of(0), learners);
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
                + "      membershipSyncIntervalMillis: 1000,\n"
                + "      failurePingIntervalMillis: 500,\n"
                + "      scaleCube: {\n"
                + "        membershipSuspicionMultiplier: 1,\n"
                + "        failurePingRequestMembers: 1,\n"
                + "        gossipIntervalMillis: 10\n"
                + "      },\n"
                + "    }\n"
                + "  },\n"
                + "  clientConnector: { port:{} }, \n"
                + "  rest.port: {}\n"
                + "}";
    }

    private static LogicalNode getLogicalNode(IgniteImpl ignite) {

        return ignite.logicalTopologyService().localLogicalTopology().nodes().stream()
                .filter(n -> n.name().equals(ignite.name()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Node not found in logical topology: " + ignite.name()));
    }
}
