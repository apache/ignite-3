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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesForZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesForZoneWithAttributes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertZoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertZonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesGlobalStateRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVault;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesNodesAttributesVault;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.impl.TestPersistStorageConfigurationSchema;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests distribution zones configuration changes and reaction to that changes.
 */
@ExtendWith(ConfigurationExtension.class)
public class DistributionZoneManagerConfigurationChangesTest extends IgniteAbstractTest {
    private static final String NODE_NAME = "test";

    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    private static final Set<NodeWithAttributes> nodes = Set.of(new NodeWithAttributes("name1", "name1", Collections.emptyMap()));

    private DistributionZoneManager distributionZoneManager;

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private ConfigurationTreeGenerator generator;

    private ConfigurationManager clusterCfgMgr;

    private VaultManager vaultMgr;

    private StandaloneMetaStorageManager metaStorageManager;

    @InjectConfiguration
    private TablesConfiguration tablesConfiguration;

    private ClockWaiter clockWaiter;

    private CatalogManager catalogManager;

    @BeforeEach
    public void setUp() throws Exception {
        generator = new ConfigurationTreeGenerator(
                List.of(DistributionZonesConfiguration.KEY),
                List.of(),
                List.of(TestPersistStorageConfigurationSchema.class)
        );
        clusterCfgMgr = new ConfigurationManager(
                List.of(DistributionZonesConfiguration.KEY),
                new TestConfigurationStorage(DISTRIBUTED),
                generator,
                ConfigurationValidatorImpl.withDefaultValidators(generator, Set.of())
        );

        DistributionZonesConfiguration zonesConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(DistributionZonesConfiguration.KEY);

        // Mock logical topology for distribution zone.
        vaultMgr = new VaultManager(new InMemoryVaultService());

        LogicalTopologyService logicalTopologyService = mock(LogicalTopologyService.class);

        Set<LogicalNode> logicalTopology = nodes.stream()
                .map(n -> new LogicalNode(n.nodeName(), n.nodeName(), new NetworkAddress(n.nodeName(), 10_000)))
                .collect(toSet());

        when(logicalTopologyService.logicalTopologyOnLeader())
                .thenReturn(completedFuture(new LogicalTopologySnapshot(0, logicalTopology)));

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage(NODE_NAME));

        metaStorageManager = StandaloneMetaStorageManager.create(vaultMgr, keyValueStorage);

        assertThat(vaultMgr.put(zonesLogicalTopologyVault(), toBytes(nodes)), willCompleteSuccessfully());

        Map<String, Map<String, String>> nodesAttributes = new ConcurrentHashMap<>();
        nodes.forEach(n -> nodesAttributes.put(n.nodeId(), n.nodeAttributes()));
        assertThat(vaultMgr.put(zonesNodesAttributesVault(), toBytes(nodesAttributes)), willCompleteSuccessfully());

        assertThat(vaultMgr.put(zonesGlobalStateRevision(), longToBytes(metaStorageManager.appliedRevision())), willCompleteSuccessfully());

        assertThat(vaultMgr.put(zonesLogicalTopologyVersionKey(), longToBytes(0)), willCompleteSuccessfully());

        metaStorageManager.put(zonesLogicalTopologyVersionKey(), longToBytes(0));

        HybridClock clock = new HybridClockImpl();

        clockWaiter = new ClockWaiter(NODE_NAME, clock);

        catalogManager = new CatalogManagerImpl(new UpdateLogImpl(metaStorageManager), clockWaiter);

        distributionZoneManager = new DistributionZoneManager(
                NODE_NAME,
                zonesConfiguration,
                tablesConfiguration,
                metaStorageManager,
                logicalTopologyService,
                vaultMgr,
                catalogManager
        );

        vaultMgr.start();
        clusterCfgMgr.start();
        metaStorageManager.start();

        DistributionZonesTestUtil.deployWatchesAndUpdateMetaStorageRevision(metaStorageManager);

        clockWaiter.start();
        catalogManager.start();

        distributionZoneManager.start();

        clearInvocations(keyValueStorage);
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                distributionZoneManager == null ? null : distributionZoneManager::stop,
                catalogManager == null ? null : catalogManager::stop,
                clockWaiter == null ? null : clockWaiter::stop,
                metaStorageManager == null ? null : metaStorageManager::stop,
                clusterCfgMgr == null ? null : clusterCfgMgr::stop,
                vaultMgr == null ? null : vaultMgr::stop,
                generator == null ? null : generator::close
        );
    }

    @Test
    void testDataNodesPropagationAfterZoneCreation() throws Exception {
        assertDataNodesForZone(1, null, keyValueStorage);

        createZone(ZONE_NAME);

        assertDataNodesForZoneWithAttributes(1, nodes.stream().map(NodeWithAttributes::node).collect(toSet()), keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(2L, 1, keyValueStorage);

        assertZonesChangeTriggerKey(2, 1, keyValueStorage);
    }

    @Test
    void testZoneDeleteRemovesMetaStorageKey() throws Exception {
        createZone(ZONE_NAME);

        assertDataNodesForZoneWithAttributes(1, nodes.stream().map(NodeWithAttributes::node).collect(toSet()), keyValueStorage);

        dropZone(ZONE_NAME);

        assertTrue(waitForCondition(() -> keyValueStorage.get(zoneDataNodesKey(1).bytes()).value() == null, 5000));
    }

    @Test
    void testSeveralZoneCreationsUpdatesTriggerKey() throws Exception {
        assertNull(keyValueStorage.get(zoneScaleUpChangeTriggerKey(1).bytes()).value());
        assertNull(keyValueStorage.get(zoneScaleUpChangeTriggerKey(2).bytes()).value());

        createZone(ZONE_NAME);

        createZone(NEW_ZONE_NAME);

        assertZoneScaleUpChangeTriggerKey(2L, 1, keyValueStorage);
        assertZoneScaleUpChangeTriggerKey(3L, 2, keyValueStorage);
        assertZonesChangeTriggerKey(2, 1, keyValueStorage);
        assertZonesChangeTriggerKey(3, 2, keyValueStorage);
    }

    @Test
    void testDataNodesNotPropagatedAfterZoneCreation() throws Exception {
        keyValueStorage.put(zonesChangeTriggerKey(1).bytes(), longToBytes(100), HybridTimestamp.MIN_VALUE);

        createZone(ZONE_NAME);

        assertZonesChangeTriggerKey(100, 1, keyValueStorage);

        assertDataNodesForZone(1, null, keyValueStorage);
    }

    @Test
    void testZoneDeleteDoNotRemoveMetaStorageKey() throws Exception {
        createZone(ZONE_NAME);

        assertDataNodesForZoneWithAttributes(1, nodes.stream().map(NodeWithAttributes::node).collect(toSet()), keyValueStorage);

        keyValueStorage.put(zonesChangeTriggerKey(1).bytes(), longToBytes(100), HybridTimestamp.MIN_VALUE);

        dropZone(ZONE_NAME);

        assertDataNodesForZoneWithAttributes(1, nodes.stream().map(NodeWithAttributes::node).collect(toSet()), keyValueStorage);
    }

    private void createZone(String zoneName) {
        DistributionZonesTestUtil.createZone(catalogManager, zoneName, null, null, null);
    }

    private void dropZone(String zoneName) {
        assertThat(
                catalogManager.dropDistributionZone(DropZoneParams.builder().zoneName(zoneName).build()),
                willBe(nullValue())
        );
    }
}
