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

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.FilterValidator;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.impl.TestPersistStorageConfigurationSchema;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for distribution zone manager.
 */
@ExtendWith(ConfigurationExtension.class)
@SuppressWarnings("ThrowableNotThrown")
class DistributionZoneManagerTest extends IgniteAbstractTest {
    private static final String NODE_NAME = "test";

    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    private ConfigurationTreeGenerator generator;

    private ConfigurationRegistry registry;

    private DistributionZoneManager distributionZoneManager;

    @InjectConfiguration("mock.tables.fooTable {}")
    private TablesConfiguration tablesConfiguration;

    private CatalogManager catalogManager;

    @BeforeEach
    public void setUp() {
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

        distributionZoneManager = new DistributionZoneManager(
                NODE_NAME,
                zonesConfiguration,
                tablesConfiguration,
                null,
                null,
                null,
                catalogManager
        );
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                registry == null ? null : registry::stop,
                generator == null ? null : generator::close
        );
    }

    @Test
    public void testGetExistingZoneIdByName() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()
                )
                .get(5, TimeUnit.SECONDS);

        assertEquals(DEFAULT_ZONE_ID + 1, distributionZoneManager.getZoneId(ZONE_NAME));
        assertEquals(DEFAULT_ZONE_ID + 1,
                registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones().get(ZONE_NAME).zoneId().value(),
                "Default distribution zone has wrong id.");

        distributionZoneManager.dropZone(ZONE_NAME).get(5, TimeUnit.SECONDS);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(NEW_ZONE_NAME).build()
                )
                .get(5, TimeUnit.SECONDS);

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
    public void testGetNotExistingZoneIdByName() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()
                )
                .get(5, TimeUnit.SECONDS);

        assertThrows(DistributionZoneNotFoundException.class, () -> distributionZoneManager.getZoneId(NEW_ZONE_NAME),
                "Expected exception was not thrown.");
    }
}
