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

import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesForZoneWithAttributes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertZoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertZonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesGlobalStateRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVault;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesNodesAttributesVault;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests distribution zones configuration changes and reaction to that changes.
 */
public class DistributionZoneManagerConfigurationChangesTest extends IgniteAbstractTest {
    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    private static final Set<NodeWithAttributes> nodes = Set.of(new NodeWithAttributes("name1", "name1", emptyMap()));

    private DistributionZoneManager distributionZoneManager;

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private VaultManager vaultMgr;

    private StandaloneMetaStorageManager metaStorageManager;

    private final HybridClock clock = new HybridClockImpl();

    private CatalogManager catalogManager;

    @BeforeEach
    public void setUp() {
        String nodeName = "test";

        // Mock logical topology for distribution zone.
        vaultMgr = new VaultManager(new InMemoryVaultService());

        LogicalTopologyService logicalTopologyService = mock(LogicalTopologyService.class);

        Set<LogicalNode> logicalTopology = nodes.stream()
                .map(n -> new LogicalNode(n.nodeName(), n.nodeName(), new NetworkAddress(n.nodeName(), 10_000)))
                .collect(toSet());

        when(logicalTopologyService.logicalTopologyOnLeader())
                .thenReturn(completedFuture(new LogicalTopologySnapshot(0, logicalTopology)));

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage(nodeName));

        metaStorageManager = StandaloneMetaStorageManager.create(vaultMgr, keyValueStorage);

        assertThat(vaultMgr.put(zonesLogicalTopologyVault(), toBytes(nodes)), willCompleteSuccessfully());

        Map<String, Map<String, String>> nodesAttributes = new ConcurrentHashMap<>();
        nodes.forEach(n -> nodesAttributes.put(n.nodeId(), n.nodeAttributes()));
        assertThat(vaultMgr.put(zonesNodesAttributesVault(), toBytes(nodesAttributes)), willCompleteSuccessfully());

        assertThat(vaultMgr.put(zonesGlobalStateRevision(), longToBytes(metaStorageManager.appliedRevision())), willCompleteSuccessfully());

        assertThat(vaultMgr.put(zonesLogicalTopologyVersionKey(), longToBytes(0)), willCompleteSuccessfully());

        Consumer<LongFunction<CompletableFuture<?>>> revisionUpdater = (LongFunction<CompletableFuture<?>> function) ->
                metaStorageManager.registerRevisionUpdateListener(function::apply);

        catalogManager = CatalogTestUtils.createTestCatalogManager(nodeName, clock, metaStorageManager);

        distributionZoneManager = new DistributionZoneManager(
                nodeName,
                revisionUpdater,
                mock(TablesConfiguration.class),
                metaStorageManager,
                logicalTopologyService,
                vaultMgr,
                catalogManager
        );

        vaultMgr.start();
        metaStorageManager.start();
        catalogManager.start();
        distributionZoneManager.start();

        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());

        clearInvocations(keyValueStorage);
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                distributionZoneManager == null ? null : distributionZoneManager::stop,
                catalogManager == null ? null : catalogManager::stop,
                metaStorageManager == null ? null : metaStorageManager::stop,
                vaultMgr == null ? null : vaultMgr::stop
        );
    }

    @Test
    void testDataNodesPropagationAfterZoneCreation() throws Exception {
        createZone(ZONE_NAME);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesForZoneWithAttributes(zoneId, nodes.stream().map(NodeWithAttributes::node).collect(toSet()), keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(2L, zoneId, keyValueStorage);

        assertZonesChangeTriggerKey(2, zoneId, keyValueStorage);
    }

    @Test
    void testZoneDeleteRemovesMetaStorageKey() throws Exception {
        createZone(ZONE_NAME);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesForZoneWithAttributes(zoneId, nodes.stream().map(NodeWithAttributes::node).collect(toSet()), keyValueStorage);

        dropZone(ZONE_NAME);

        assertTrue(waitForCondition(() -> keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value() == null, 5000));
    }

    @Test
    void testSeveralZoneCreationsUpdatesTriggerKey() throws Exception {
        createZone(ZONE_NAME);

        createZone(NEW_ZONE_NAME);

        int zoneId = getZoneId(ZONE_NAME);
        int zoneId2 = getZoneId(NEW_ZONE_NAME);

        assertZoneScaleUpChangeTriggerKey(2L, zoneId, keyValueStorage);
        assertZoneScaleUpChangeTriggerKey(4L, zoneId2, keyValueStorage);
        assertZonesChangeTriggerKey(2L, zoneId, keyValueStorage);
        assertZonesChangeTriggerKey(4L, zoneId2, keyValueStorage);
    }

    @Test
    void testZoneDeleteDoNotRemoveMetaStorageKey() throws Exception {
        createZone(ZONE_NAME);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesForZoneWithAttributes(zoneId, nodes.stream().map(NodeWithAttributes::node).collect(toSet()), keyValueStorage);

        keyValueStorage.put(zonesChangeTriggerKey(zoneId).bytes(), longToBytes(100), HybridTimestamp.MIN_VALUE);

        dropZone(ZONE_NAME);

        assertDataNodesForZoneWithAttributes(zoneId, nodes.stream().map(NodeWithAttributes::node).collect(toSet()), keyValueStorage);
    }

    private void createZone(String zoneName) {
        DistributionZonesTestUtil.createZone(catalogManager, zoneName, null, null, null);
    }

    private void dropZone(String zoneName) {
        DistributionZonesTestUtil.dropZone(catalogManager, zoneName);
    }

    private int getZoneId(String zoneName) {
        return DistributionZonesTestUtil.getZoneIdStrict(catalogManager, zoneName, clock.nowLong());
    }
}
