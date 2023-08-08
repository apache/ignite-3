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

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.FilterValidator;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.impl.TestPersistStorageConfigurationSchema;
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

    @InjectConfiguration("mock.tables.fooTable {}")
    private TablesConfiguration tablesConfiguration;

    private VaultManager vault;

    private MetaStorageManager metastore;

    private ClockWaiter clockWaiter;

    private CatalogManager catalogManager;

    private DistributionZoneManager distributionZoneManager;

    @BeforeEach
    public void setUp() {
        String nodeName = "node";

        generator = new ConfigurationTreeGenerator(
                List.of(DistributionZonesConfiguration.KEY),
                List.of(),
                List.of(TestPersistStorageConfigurationSchema.class)
        );
        registry = new ConfigurationRegistry(
                List.of(DistributionZonesConfiguration.KEY),
                new TestConfigurationStorage(DISTRIBUTED),
                generator,
                ConfigurationValidatorImpl.withDefaultValidators(generator, Set.of(FilterValidator.INSTANCE))
        );

        registry.start();
        assertThat(registry.onDefaultsPersisted(), willCompleteSuccessfully());

        DistributionZonesConfiguration zonesConfiguration = registry.getConfiguration(DistributionZonesConfiguration.KEY);

        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(vault, new SimpleInMemoryKeyValueStorage(nodeName));

        var clock = new HybridClockImpl();

        clockWaiter = new ClockWaiter(nodeName, clock);

        catalogManager = new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter);

        distributionZoneManager = new DistributionZoneManager(
                nodeName,
                function -> metastore.registerRevisionUpdateListener(function::apply),
                zonesConfiguration,
                tablesConfiguration,
                metastore,
                mock(LogicalTopologyService.class),
                vault,
                catalogManager
        );

        Stream.of(vault, metastore, clockWaiter, catalogManager, distributionZoneManager).forEach(IgniteComponent::start);

        assertThat(metastore.deployWatches(), willCompleteSuccessfully());
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                distributionZoneManager == null ? null : distributionZoneManager::stop,
                catalogManager == null ? null : catalogManager::stop,
                clockWaiter == null ? null : clockWaiter::stop,
                metastore == null ? null : metastore::stop,
                vault == null ? null : vault::stop,
                registry == null ? null : registry::stop,
                generator == null ? null : generator::close
        );
    }

    @Test
    public void testGetExistingZoneIdByName() {
        createZone(ZONE_NAME);

        assertEquals(DEFAULT_ZONE_ID + 1, distributionZoneManager.getZoneId(ZONE_NAME));
        assertEquals(DEFAULT_ZONE_ID + 1,
                registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones().get(ZONE_NAME).zoneId().value(),
                "Default distribution zone has wrong id.");

        dropZone(ZONE_NAME);

        createZone(NEW_ZONE_NAME);

        assertEquals(DEFAULT_ZONE_ID, distributionZoneManager.getZoneId(DEFAULT_ZONE_NAME),
                "Default distribution zone has wrong id.");
        assertEquals(DEFAULT_ZONE_ID,
                registry.getConfiguration(DistributionZonesConfiguration.KEY).defaultDistributionZone().zoneId().value(),
                "Default distribution zone has wrong id.");

        assertEquals(DEFAULT_ZONE_ID + 2, distributionZoneManager.getZoneId(NEW_ZONE_NAME));
        assertEquals(DEFAULT_ZONE_ID + 2,
                registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones().get(NEW_ZONE_NAME).zoneId().value(),
                "Default distribution zone has wrong id.");
    }

    @Test
    public void testGetNotExistingZoneIdByName() {
        createZone(ZONE_NAME);

        assertThrows(DistributionZoneNotFoundException.class, () -> distributionZoneManager.getZoneId(NEW_ZONE_NAME),
                "Expected exception was not thrown.");
    }

    private void createZone(String zoneName) {
        DistributionZonesTestUtil.createZone(catalogManager, zoneName, null, null, null);
    }

    private void dropZone(String zoneName) {
        DistributionZonesTestUtil.dropZone(catalogManager, zoneName);
    }
}
