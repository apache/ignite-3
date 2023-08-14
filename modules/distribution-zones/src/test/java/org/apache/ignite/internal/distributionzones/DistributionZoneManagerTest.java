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

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.TestCatalogManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for distribution zone manager.
 */
@ExtendWith(ConfigurationExtension.class)
class DistributionZoneManagerTest extends IgniteAbstractTest {
    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    private ConfigurationTreeGenerator generator;

    private ConfigurationRegistry registry;

    private VaultManager vault;

    private MetaStorageManager metastore;

    private final HybridClock clock = new HybridClockImpl();

    private TestCatalogManager catalogManager;

    private DistributionZoneManager distributionZoneManager;

    @BeforeEach
    public void setUp(@InjectConfiguration DistributionZonesConfiguration zonesConfig) {
        String nodeName = "node";

        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(vault, new SimpleInMemoryKeyValueStorage(nodeName));

        catalogManager = new TestCatalogManager(nodeName, clock, vault, metastore);

        distributionZoneManager = new DistributionZoneManager(
                nodeName,
                function -> metastore.registerRevisionUpdateListener(function::apply),
                zonesConfig,
                metastore,
                mock(LogicalTopologyService.class),
                vault,
                catalogManager
        );

        Stream.of(vault, metastore, catalogManager, distributionZoneManager).forEach(IgniteComponent::start);

        assertThat(metastore.deployWatches(), willCompleteSuccessfully());
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                distributionZoneManager == null ? null : distributionZoneManager::stop,
                catalogManager == null ? null : catalogManager::stop,
                metastore == null ? null : metastore::stop,
                vault == null ? null : vault::stop
        );
    }

    @Test
    public void testGetExistingZoneIdByName() {
        createZone(ZONE_NAME);

        assertEquals(getZoneId(ZONE_NAME), distributionZoneManager.getZoneId(ZONE_NAME));

        dropZone(ZONE_NAME);

        createZone(NEW_ZONE_NAME);

        assertEquals(getZoneId(DEFAULT_ZONE_NAME), distributionZoneManager.getZoneId(DEFAULT_ZONE_NAME));

        assertEquals(getZoneId(NEW_ZONE_NAME), distributionZoneManager.getZoneId(NEW_ZONE_NAME));
    }

    @Test
    public void testGetNotExistingZoneIdByName() {
        createZone(ZONE_NAME);

        assertThrows(DistributionZoneNotFoundException.class, () -> distributionZoneManager.getZoneId(NEW_ZONE_NAME));
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
