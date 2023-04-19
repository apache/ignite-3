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
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertDataNodesForZone;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertZoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertZonesChangeTriggerKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.impl.TestPersistStorageConfigurationSchema;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.NodeStoppingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests distribution zones configuration changes and reaction to that changes.
 */
@ExtendWith(ConfigurationExtension.class)
public class DistributionZoneManagerConfigurationChangesTest extends IgniteAbstractTest {
    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    private static final Set<String> nodes = Set.of("name1");

    private DistributionZoneManager distributionZoneManager;

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private ConfigurationManager clusterCfgMgr;

    private VaultManager vaultMgr;

    private StandaloneMetaStorageManager metaStorageManager;

    @InjectConfiguration
    private TablesConfiguration tablesConfiguration;

    @BeforeEach
    public void setUp() throws NodeStoppingException {
        clusterCfgMgr = new ConfigurationManager(
                List.of(DistributionZonesConfiguration.KEY),
                Set.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of(TestPersistStorageConfigurationSchema.class)
        );

        DistributionZonesConfiguration zonesConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(DistributionZonesConfiguration.KEY);

        // Mock logical topology for distribution zone.
        //TODO https://issues.apache.org/jira/browse/IGNITE-19104 We may get stale value, due to asynchronous read from Vault.
        // vaultMgr = new VaultManager(new InMemoryVaultService());
        // vaultMgr.put(zonesLogicalTopologyKey(), toBytes(nodes));

        vaultMgr = mock(VaultManager.class);
        when(vaultMgr.get(any())).thenReturn(completedFuture(null));
        // TODO: IGNITE-19104 Already completed future workaround the race.
        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), toBytes(nodes))));

        when(vaultMgr.get(zonesLogicalTopologyVersionKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyVersionKey(), longToBytes(0))));

        LogicalTopologyService logicalTopologyService = mock(LogicalTopologyService.class);

        when(logicalTopologyService.logicalTopologyOnLeader()).thenReturn(completedFuture(new LogicalTopologySnapshot(0, Set.of())));

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage("test"));

        metaStorageManager = StandaloneMetaStorageManager.create(vaultMgr, keyValueStorage);

        metaStorageManager.put(zonesLogicalTopologyVersionKey(), longToBytes(0));

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                tablesConfiguration,
                metaStorageManager,
                logicalTopologyService,
                vaultMgr,
                "test"
        );

        vaultMgr.start();
        clusterCfgMgr.start();
        metaStorageManager.start();
        distributionZoneManager.start();

        metaStorageManager.deployWatches();

        clearInvocations(keyValueStorage);
    }

    @AfterEach
    public void tearDown() throws Exception {
        distributionZoneManager.stop();
        metaStorageManager.stop();
        clusterCfgMgr.stop();
        vaultMgr.stop();
    }

    @Test
    void testDataNodesPropagationAfterZoneCreation() throws Exception {
        assertDataNodesForZone(1, null, keyValueStorage);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, nodes, keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(1, 1, keyValueStorage);

        assertZonesChangeTriggerKey(1, 1, keyValueStorage);
    }

    @Test
    void testZoneDeleteRemovesMetaStorageKey() throws Exception {
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, nodes, keyValueStorage);

        distributionZoneManager.dropZone(ZONE_NAME).get();

        assertTrue(waitForCondition(() -> keyValueStorage.get(zoneDataNodesKey(1).bytes()).value() == null, 5000));
    }

    @Test
    void testSeveralZoneCreationsUpdatesTriggerKey() throws Exception {
        assertNull(keyValueStorage.get(zoneScaleUpChangeTriggerKey(1).bytes()).value());
        assertNull(keyValueStorage.get(zoneScaleUpChangeTriggerKey(2).bytes()).value());

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(NEW_ZONE_NAME).build()).get();

        assertZoneScaleUpChangeTriggerKey(1, 1, keyValueStorage);
        assertZoneScaleUpChangeTriggerKey(2, 2, keyValueStorage);
        assertZonesChangeTriggerKey(1, 1, keyValueStorage);
        assertZonesChangeTriggerKey(2, 2, keyValueStorage);
    }

    @Test
    void testDataNodesNotPropagatedAfterZoneCreation() throws Exception {
        keyValueStorage.put(zonesChangeTriggerKey(1).bytes(), longToBytes(100));

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertZonesChangeTriggerKey(100, 1, keyValueStorage);

        assertDataNodesForZone(1, null, keyValueStorage);
    }

    @Test
    void testZoneDeleteDoNotRemoveMetaStorageKey() throws Exception {
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, nodes, keyValueStorage);

        keyValueStorage.put(zonesChangeTriggerKey(1).bytes(), longToBytes(100));

        distributionZoneManager.dropZone(ZONE_NAME).get();

        assertDataNodesForZone(1, nodes, keyValueStorage);
    }
}
