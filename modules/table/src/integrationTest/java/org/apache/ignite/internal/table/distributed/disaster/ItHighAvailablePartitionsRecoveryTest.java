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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
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

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, Set.of(0, 1), Set.of(node.name()));
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
}
