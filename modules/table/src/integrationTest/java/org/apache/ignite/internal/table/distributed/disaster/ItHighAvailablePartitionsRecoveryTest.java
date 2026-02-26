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

import static java.lang.String.format;
import static org.apache.ignite.internal.ConfigTemplates.FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.alterZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromLogicalNodesInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** Test for the HA zones recovery. */
public class ItHighAvailablePartitionsRecoveryTest extends AbstractHighAvailablePartitionsRecoveryTest {
    @Override
    protected int initialNodes() {
        return 3;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Test
    void testHaRecoveryWhenMajorityLoss() throws InterruptedException {
        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1, 2);

        waitAndAssertRecoveryKeyIsNotEmpty(node);

        assertRecoveryRequestForHaZoneTable(node);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, Set.of(node.name()));
    }

    @Test
    void testHaRecoveryOfTwoZones() throws InterruptedException {
        String zone1 = "ZONE1";
        String zone2 = "ZONE2";

        String table1 = "TABLE1";
        String table21 = "TABLE21";
        String table22 = "TABLE22";

        createHaZoneWithTable(zone1, table1);
        createHaZoneWithTables(zone2, List.of(table21, table22));

        IgniteImpl node = igniteImpl(0);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1, 2);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, table1, PARTITION_IDS, Set.of(node.name()));
        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, table21, PARTITION_IDS, Set.of(node.name()));
        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, table22, PARTITION_IDS, Set.of(node.name()));
    }

    @Test
    void testInvalidPartitionResetTimeoutUpdate() {
        IgniteImpl node = igniteImpl(0);

        CompletableFuture<Void> changeFuture = node
                .clusterConfiguration()
                .getConfiguration(SystemDistributedExtensionConfiguration.KEY)
                .system().change(c0 -> c0.changeProperties()
                        .createOrUpdate(PARTITION_DISTRIBUTION_RESET_TIMEOUT,
                                c1 -> c1.changePropertyValue(String.valueOf(-1)))
                );

        assertThat(changeFuture, willThrowWithCauseOrSuppressed(ConfigurationValidationException.class));
    }

    @RepeatedTest(50)
    void testHaRecoveryWhenPartitionResetTimeoutUpdated() throws InterruptedException {
        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        changePartitionDistributionTimeout(node, INFINITE_TIMER_VALUE - 1);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1, 2);

        assertRecoveryKeyIsEmpty(node);

        changePartitionDistributionTimeout(node, 1);

        waitAndAssertRecoveryKeyIsNotEmpty(node);

        assertRecoveryRequestForHaZoneTable(node);
        assertRecoveryRequestWasOnlyOne(node);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, Set.of(node.name()));
    }

    @Test
    void testHaRecoveryWithPartitionResetTimerReschedule() throws InterruptedException {
        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        changePartitionDistributionTimeout(node, 10);

        assertRecoveryKeyIsEmpty(node);

        stopNode(1);

        assertRecoveryKeyIsEmpty(node);

        stopNode(2);

        waitAndAssertRecoveryKeyIsNotEmpty(node, 30_000);

        assertRecoveryRequestForHaZoneTable(node);
        assertRecoveryRequestWasOnlyOne(node);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, Set.of(node.name()));
    }

    @Test
    void testScaleUpAfterReset() throws InterruptedException {
        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        assertRecoveryKeyIsEmpty(node);

        log.info("Test: stopping nodes.");

        stopNodes(1, 2);

        waitAndAssertRecoveryKeyIsNotEmpty(node, 30_000);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, Set.of(node.name()));

        log.info("Test: restarting nodes.");

        startNode(1);
        startNode(2);

        Set<String> expectedAssignmentsAfterScaleUp = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, expectedAssignmentsAfterScaleUp);
    }

    @Test
    void testNoHaRecoveryWhenMajorityAvailable() throws InterruptedException {
        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        assertRecoveryKeyIsEmpty(node);

        String stopNode = node(2).name();

        stopNode(2);

        long revision = waitForSpecificZoneTopologyReductionAndReturnUpdateRevision(node, HA_ZONE_NAME, Set.of(stopNode));

        assertTrue(waitForCondition(() -> node.metaStorageManager().appliedRevision() >= revision, 10_000));

        assertTrue(
                node
                        .distributionZoneManager()
                        .dataNodesManager()
                        .zoneTimers(zoneIdByName(node.catalogManager(), HA_ZONE_NAME))
                        .partitionReset
                        .taskIsDone()
        );

        assertTrue(node.disasterRecoveryManager().ongoingOperationsById().isEmpty());

        assertRecoveryKeyIsEmpty(node);
    }

    @Test
    void testNoHaRecoveryForScZone() throws InterruptedException {
        createScZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        assertTrue(waitForCondition(() -> getRecoveryTriggerKey(node).empty(), 5_000));

        String lastStopNode = node(1).name();

        stopNodes(2, 1);

        long revision = waitForSpecificZoneTopologyReductionAndReturnUpdateRevision(node, SC_ZONE_NAME, Set.of(lastStopNode));

        assertTrue(waitForCondition(() -> node.metaStorageManager().appliedRevision() >= revision, 10_000));

        assertFalse(
                node
                        .distributionZoneManager()
                        .dataNodesManager()
                        .zoneTimers(zoneIdByName(node.catalogManager(), SC_ZONE_NAME))
                        .partitionReset
                        .taskIsScheduled()
        );

        assertRecoveryKeyIsEmpty(node);
    }

    @Test
    void testScaleUpAfterHaRecoveryWhenMajorityLoss() throws Exception {
        startNode(3);

        startNode(4);

        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1, 2, 3, 4);

        Table table = node.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);

        assertThat(errors, is(empty()));

        waitAndAssertRecoveryKeyIsNotEmpty(node);

        assertRecoveryRequestForHaZoneTable(node);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, Set.of(node.name()));

        log.info("Test: restarting nodes.");

        var node1 = startNode(1);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(
                node,
                HA_TABLE_NAME,
                PARTITION_IDS,
                Set.of(node.name(), node1.name())
        );

        var node2 = startNode(2);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(
                node,
                HA_TABLE_NAME,
                PARTITION_IDS,
                Set.of(node.name(), node1.name(), node2.name())
        );

        assertValuesPresentOnNodes(node.clock().now(), table, 0, 1, 2);
    }

    @Test
    void testHaZoneScaleDownNodesDoNotRemovedFromStable() throws InterruptedException {
        startNode(3);

        startNode(4);

        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        Set<String> allNodes = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        stopNodes(3, 4);

        startNode(3);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, allNodes);
    }

    @Test
    void testRebalanceInHaZone() throws InterruptedException {
        createHaZoneWithTable();

        startNode(3);

        IgniteImpl node = igniteImpl(0);
        Table table = node.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);
        assertThat(errors, is(empty()));

        Set<String> fourNodes = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        executeSql(format("ALTER ZONE %s SET (REPLICAS %d)", HA_ZONE_NAME, 4));

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, fourNodes);

        assertValuesPresentOnNodes(node.clock().now(), table, 0, 1, 2, 3);

        stopNode(3);

        Set<String> threeNodes = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        executeSql(format("ALTER ZONE %s SET (auto scale down %d)", HA_ZONE_NAME, 1));

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, threeNodes);

        assertValuesPresentOnNodes(node.clock().now(), table, 0, 1, 2);
    }

    @Test
    void testRestartNodesOneByOne() throws InterruptedException {
        startNode(3);
        startNode(4);

        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        Table table = node.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);
        assertThat(errors, is(empty()));

        for (int i = 0; i < 5; i++) {
            restartNode(i);
        }

        Set<String> allNodes = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        node = igniteImpl(0);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, allNodes);

        assertValuesPresentOnNodes(node.clock().now(), node.tables().table(HA_TABLE_NAME), 0, 1, 2, 3, 4);
    }

    /**
     * Test scenario.
     * <ol>
     *   <li>Create a zone in HA mode (7 nodes, A, B, C, D, E, F, G)</li>
     *   <li>Set 'partitionDistributionResetTimeout' to 5 minutes</li>
     *   <li>Insert data and wait for replication to all nodes.</li>
     *   <li>Stop a majority of nodes (4 nodes A, B, C, D)</li>
     *   <li>Manually execute partition reset before 'partitionDistributionResetTimeout' expires</li>
     *   <li>Wait for the partition to become available (E, F, G), no new writes</li>
     *   <li>Set 'partitionDistributionResetTimeout' to 1 sec to trigger automatic reset</li>
     *   <li>Verify the second (automatic) reset triggered</li>
     *   <li>No data should be lost</li>
     * </ol>
     */
    @RepeatedTest(50)
    void testManualRecovery() throws InterruptedException {
        startNode(3);
        startNode(4);
        startNode(5);
        startNode(6);

        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);
        Table table = node.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);
        assertThat(errors, is(empty()));

        changePartitionDistributionTimeout(node, (int) TimeUnit.MINUTES.toSeconds(5));

        Set<String> allNodes = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, allNodes);

        assertValuesPresentOnNodes(node.clock().now(), table, 0, 1, 2, 3, 4, 5, 6);

        stopNodes(3, 4, 5, 6);

        triggerManualReset(node);

        Set<String> threeNodes = runningNodes().map(Ignite::name).collect(Collectors.toUnmodifiableSet());

        waitThatAllRebalancesHaveFinishedAndStableAssignmentsEqualsToExpected(node, HA_TABLE_NAME, PARTITION_IDS, threeNodes);

        // This entry comes from the manual reset.
        Entry manualResetTrigger = getRecoveryTriggerKey(node);
        assertFalse(manualResetTrigger.empty());

        long manualResetRev = manualResetTrigger.revision();

        changePartitionDistributionTimeout(node, 1);

        assertTrue(waitForCondition(() -> {
                    Entry recoveryTrigger = getRecoveryTriggerKey(node);

                    return !recoveryTrigger.empty() && recoveryTrigger.revision() > manualResetRev;
                },
                5_000
        ));

        assertRecoveryRequestForHaZoneTable(node);
        assertRecoveryRequestCount(node, 2);

        waitThatAllRebalancesHaveFinishedAndStableAssignmentsEqualsToExpected(node, HA_TABLE_NAME, PARTITION_IDS, threeNodes);
    }

    @Test
    void testScaleDownTimerIsWorkingForHaZone() throws InterruptedException {
        IgniteImpl node = igniteImpl(0);

        DistributionZonesTestUtil.createZone(
                node.catalogManager(),
                HA_ZONE_NAME,
                1,
                initialNodes(),
                IMMEDIATE_TIMER_VALUE,
                IMMEDIATE_TIMER_VALUE,
                ConsistencyMode.HIGH_AVAILABILITY
        );

        int zoneId = zoneIdByName(node.catalogManager(), HA_ZONE_NAME);

        KeyValueStorage keyValueStorage = ((MetaStorageManagerImpl) node.metaStorageManager()).storage();

        String stoppedNodeName = node(2).name();

        stopNode(2);

        Set<LogicalNode> expectedNodes = runningNodes()
                .map(n -> unwrapIgniteImpl(n).clusterService().topologyService().localMember())
                .map(LogicalNode::new)
                .collect(Collectors.toUnmodifiableSet());

        assertFalse(expectedNodes.contains(stoppedNodeName));

        assertDataNodesFromLogicalNodesInStorage(
                zoneId,
                expectedNodes,
                keyValueStorage
        );
    }

    /**
     * Test scenario.
     * <ol>
     *   <li>Create a zone in HA mode with 7 nodes (A, B, C, D, E, F, G).</li>
     *   <li>Insert data and wait for replication to all nodes.</li>
     *   <li>Stop a majority of nodes (A, B, C, D).</li>
     *   <li>Wait for the partition to become available on the remaining nodes (E, F, G).</li>
     *   <li>Start node A.</li>
     *   <li>Verify that node A cleans up its state.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @RepeatedTest(50)
    void testNodeStateCleanupAfterRestartInHaMode() throws Exception {
        startNode(3);
        startNode(4);
        startNode(5);
        startNode(6);

        createHaZoneWithTable();

        IgniteImpl node0 = igniteImpl(0);
        Table table = node0.tables().table(HA_TABLE_NAME);

        List<Throwable> errors = insertValues(table, 0);
        assertThat(errors, is(empty()));
        assertValuesPresentOnNodes(node0.clock().now(), table, 0, 1, 2, 3, 4, 5, 6);

        alterZone(node0.catalogManager(), HA_ZONE_NAME, INFINITE_TIMER_VALUE, null, null);

        stopNodes(3, 4, 5, 6);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node0, HA_TABLE_NAME, PARTITION_IDS, nodeNames(0, 1, 2));

        startNode(6);

        assertPartitionsAreEmpty(HA_TABLE_NAME, PARTITION_IDS, 6);
    }
}
