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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLongKeepingOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.junit.jupiter.api.Test;

/** Test for the HA zones recovery. */
public class ItHighAvailablePartitionsRecoveryTest  extends AbstractHighAvailablePartitionsRecoveryTest {
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

    @Test
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
    void testHaRecoveryOnZoneTimersRestoreAfterNodeRestart() throws InterruptedException {
        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        changePartitionDistributionTimeout(node, 10);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(2, 1, 0);

        IgniteImpl node1 = unwrapIgniteImpl(startNode(0));

        waitAndAssertRecoveryKeyIsNotEmpty(node1, 30_000);

        assertRecoveryRequestForHaZoneTable(node1);
        assertRecoveryRequestWasOnlyOne(node1);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node1, HA_TABLE_NAME, PARTITION_IDS, Set.of(node1.name()));
    }

    @Test
    void testScaleUpAfterReset() throws InterruptedException {
        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1, 2);

        waitAndAssertRecoveryKeyIsNotEmpty(node, 30_000);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, Set.of(node.name()));

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

        long revision = waitForSpecificZoneTopologyUpdateAndReturnUpdateRevision(node, HA_ZONE_NAME, Set.of(stopNode));

        waitForCondition(() -> node.metaStorageManager().appliedRevision() >= revision, 10_000);

        assertTrue(
                node
                        .distributionZoneManager()
                        .zonesState()
                        .get(zoneIdByName(node.catalogManager(), HA_ZONE_NAME))
                        .partitionDistributionResetTask()
                        .isDone()
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

        long revision = waitForSpecificZoneTopologyUpdateAndReturnUpdateRevision(node, SC_ZONE_NAME, Set.of(lastStopNode));

        waitForCondition(() -> node.metaStorageManager().appliedRevision() >= revision, 10_000);

        assertNull(
                node
                        .distributionZoneManager()
                        .zonesState()
                        .get(zoneIdByName(node.catalogManager(), SC_ZONE_NAME))
                        .partitionDistributionResetTask()
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

        waitAndAssertRecoveryKeyIsNotEmpty(node);

        assertRecoveryRequestForHaZoneTable(node);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, Set.of(node.name()));

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

        Supplier<Long> getScaleDownRevision = () ->
                bytesToLongKeepingOrder(keyValueStorage.get(zoneScaleDownChangeTriggerKey(zoneId).bytes()).value());

        long revisionOfScaleDown = getScaleDownRevision.get();

        stopNode(2);

        assertTrue(waitForCondition(() -> getScaleDownRevision.get() > revisionOfScaleDown, 5_000));

        Set<LogicalNode> expectedNodes = runningNodes()
                .map(n -> unwrapIgniteImpl(n).clusterService().topologyService().localMember())
                .map(LogicalNode::new)
                .collect(Collectors.toUnmodifiableSet());

        DistributionZonesTestUtil.assertDataNodesFromLogicalNodesInStorage(
                zoneId,
                expectedNodes,
                keyValueStorage
        );
    }
}
