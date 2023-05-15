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

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertDataNodesForZone;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.deployWatchesAndUpdateMetaStorageRevision;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests distribution zones logical topology changes and reaction to that changes.
 */
//TODO: IGNITE-18564 Add tests with not default distribution zones, when distributionZones.change.trigger per zone will be created.
public class DistributionZoneManagerWatchListenerTest extends BaseDistributionZoneManagerTest {
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testDataNodesOfDefaultZoneUpdatedOnWatchListenerEvent() throws Exception {
        startDistributionZoneManager();

        // First invoke happens on distributionZoneManager start.
        verify(keyValueStorage, timeout(1000).times(1)).invoke(any(), any());

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(0)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

        //first event

        Set<String> nodes = Set.of("node1", "node2");

        setLogicalTopologyInMetaStorage(nodes);

        // saveDataNodesToMetaStorageOnScaleUp
        verify(keyValueStorage, timeout(1000).times(3)).invoke(any(), any());

        assertDataNodesForZone(DEFAULT_ZONE_ID, nodes, keyValueStorage);

        //second event

        nodes = Set.of("node1", "node3");

        setLogicalTopologyInMetaStorage(nodes);

        verify(keyValueStorage, timeout(1000).times(4)).invoke(any(), any());

        nodes = Set.of("node1", "node2", "node3");

        // Scale up just adds node to data nodes
        assertDataNodesForZone(DEFAULT_ZONE_ID, nodes, keyValueStorage);

        //third event

        nodes = Collections.emptySet();

        setLogicalTopologyInMetaStorage(nodes);

        verify(keyValueStorage, timeout(1000).times(4)).invoke(any(), any());

        nodes = Set.of("node1", "node2", "node3");

        // Scale up wasn't triggered
        assertDataNodesForZone(DEFAULT_ZONE_ID, nodes, keyValueStorage);
    }

    @Test
    void testStaleWatchEvent() throws Exception {
        mockVaultZonesLogicalTopologyKey(Set.of());

        startDistributionZoneManager();

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(0)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

        long revision = 100;

        keyValueStorage.put(zoneScaleUpChangeTriggerKey(DEFAULT_ZONE_ID).bytes(), longToBytes(revision), HybridTimestamp.MIN_VALUE);

        Set<String> nodes = Set.of("node1", "node2");

        setLogicalTopologyInMetaStorage(nodes);

        // two invokes on start, and invoke for update scale up won't be triggered, because revision == zoneScaleUpChangeTriggerKey
        verify(keyValueStorage, timeout(1000).times(2)).invoke(any(), any());

        assertDataNodesForZone(DEFAULT_ZONE_ID, Set.of(), keyValueStorage);
    }

    @Test
    void testStaleVaultRevisionOnZoneManagerStart() throws Exception {
        long revision = 100;

        keyValueStorage.put(zonesChangeTriggerKey(DEFAULT_ZONE_ID).bytes(), longToBytes(revision), HybridTimestamp.MIN_VALUE);

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        startDistributionZoneManager();

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any(), any());

        assertDataNodesForZone(DEFAULT_ZONE_ID, null, keyValueStorage);
    }

    @Test
    void testDataNodesUpdatedOnZoneManagerStart() throws Exception {
        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        startDistributionZoneManager();

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any(), any());

        assertDataNodesForZone(DEFAULT_ZONE_ID, nodes, keyValueStorage);
    }

    @Test
    void testLogicalTopologyIsNullOnZoneManagerStart1() {
        distributionZoneManager.start();

        // 1 invoke because only invoke to zones logical topology happens
        verify(keyValueStorage, timeout(1000).times(1)).invoke(any(), any());

        assertNull(keyValueStorage.get(zoneDataNodesKey(DEFAULT_ZONE_ID).bytes()).value());
        assertNull(keyValueStorage.get(zoneDataNodesKey(1).bytes()).value());
    }

    private void mockVaultZonesLogicalTopologyKey(Set<String> nodes) {
        byte[] newLogicalTopology = toBytes(nodes);

        assertThat(vaultMgr.put(zonesLogicalTopologyKey(), newLogicalTopology), willCompleteSuccessfully());
    }

    private void setLogicalTopologyInMetaStorage(Set<String> nodes) {
        CompletableFuture<Boolean> invokeFuture = metaStorageManager.invoke(
                Conditions.exists(zonesLogicalTopologyKey()),
                Operations.put(zonesLogicalTopologyKey(), toBytes(nodes)),
                Operations.noop()
        );

        assertThat(invokeFuture, willBe(true));
    }

    private void startDistributionZoneManager() throws Exception {
        deployWatchesAndUpdateMetaStorageRevision(metaStorageManager);

        distributionZoneManager.start();
    }
}
