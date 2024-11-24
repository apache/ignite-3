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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.configuration.DistributionZonesHighAvailabilityConfiguration.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager.RECOVERY_TRIGGER_KEY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test for the HA zones recovery. */
public class ItHighAvailablePartitionsRecoveryTest  extends ClusterPerTestIntegrationTest {
    private static String ZONE_NAME = "HA_ZONE";

    private static String TABLE_NAME = "TEST_TABLE";

    protected final HybridClock clock = new HybridClockImpl();

    @Override
    protected int initialNodes() {
        return 2;
    }

    @BeforeEach
    void setUp() {
        executeSql(String.format(
                "CREATE ZONE %s WITH REPLICAS=%s, PARTITIONS=%s, STORAGE_PROFILES='%s', CONSISTENCY_MODE='HIGH_AVAILABILITY'",
                ZONE_NAME, initialNodes(), 2, DEFAULT_STORAGE_PROFILE
        ));

        executeSql(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, val INT) ZONE %s",
                TABLE_NAME, ZONE_NAME
        ));
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Test
    void testTopologyReduceEventPropagation() throws InterruptedException {
        IgniteImpl node = igniteImpl(0);

        assertTrue(waitForCondition(() -> getRecoveryTriggerKey(node).empty(), 5_000));

        stopNode(1);

        assertTrue(waitForCondition(() -> !getRecoveryTriggerKey(node).empty(), 5_000));

        GroupUpdateRequest request = (GroupUpdateRequest) VersionedSerialization.fromBytes(
                getRecoveryTriggerKey(node).value(), DisasterRecoveryRequestSerializer.INSTANCE);

        int zoneId = node.catalogManager().zone(ZONE_NAME, clock.nowLong()).id();
        int tableId = node.catalogManager().table(TABLE_NAME, clock.nowLong()).id();

        assertEquals(zoneId, request.zoneId());
        assertEquals(tableId, request.tableId());
        assertEquals(Set.of(0, 1), request.partitionIds());
        assertFalse(request.manualUpdate());
    }

    @Test
    void testTopologyReduceEventPropagationOnPartitionResetTimeoutChange() throws InterruptedException {
        IgniteImpl node = igniteImpl(0);

        {
            CompletableFuture<Void> changeFuture = node.clusterConfiguration().getConfiguration(SystemDistributedExtensionConfiguration.KEY)
                    .system().change(c0 -> c0.changeProperties()
                            .createOrUpdate(PARTITION_DISTRIBUTION_RESET_TIMEOUT,
                                    c1 -> c1.changePropertyValue(String.valueOf(INFINITE_TIMER_VALUE - 1)))
                    );

            assertThat(changeFuture, willCompleteSuccessfully());
        }

        assertTrue(waitForCondition(() -> getRecoveryTriggerKey(node).empty(), 5_000));

        stopNode(1);

        assertTrue(waitForCondition(() -> getRecoveryTriggerKey(node).empty(), 5_000));

        {
            CompletableFuture<Void> changeFuture = node.clusterConfiguration().getConfiguration(SystemDistributedExtensionConfiguration.KEY)
                    .system().change(c0 -> c0.changeProperties()
                            .createOrUpdate(PARTITION_DISTRIBUTION_RESET_TIMEOUT, c1 -> c1.changePropertyValue(String.valueOf(1)))
                    );

            assertThat(changeFuture, willCompleteSuccessfully());
        }

        assertTrue(waitForCondition(() -> !getRecoveryTriggerKey(node).empty(), 5_000));

        GroupUpdateRequest request = (GroupUpdateRequest) VersionedSerialization.fromBytes(
                getRecoveryTriggerKey(node).value(), DisasterRecoveryRequestSerializer.INSTANCE);

        int zoneId = node.catalogManager().zone(ZONE_NAME, clock.nowLong()).id();
        int tableId = node.catalogManager().table(TABLE_NAME, clock.nowLong()).id();

        assertEquals(zoneId, request.zoneId());
        assertEquals(tableId, request.tableId());
        assertEquals(Set.of(0, 1), request.partitionIds());
        assertFalse(request.manualUpdate());
    }


    private static Entry getRecoveryTriggerKey(IgniteImpl node) {
        CompletableFuture<Entry> getFut = node.metaStorageManager().get(RECOVERY_TRIGGER_KEY);

        assertThat(getFut, willCompleteSuccessfully());

        return getFut.join();
    }
}
