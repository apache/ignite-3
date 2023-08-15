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
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Set;
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

    private DistributionZoneManager distributionZoneManager;

    @InjectConfiguration("mock.tables.fooTable {}")
    private TablesConfiguration tablesConfiguration;

    private DistributionZonesConfiguration zonesConfiguration;

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

        zonesConfiguration = registry.getConfiguration(DistributionZonesConfiguration.KEY);

        distributionZoneManager = new DistributionZoneManager(
                null,
                zonesConfiguration,
                tablesConfiguration,
                null,
                null,
                null,
                "node"
        );
    }

    @AfterEach
    public void tearDown() throws Exception {
        registry.stop();
        generator.close();
    }

    @Test
    public void testGetExistingZoneIdByName() {
        createZone(ZONE_NAME);

        int zoneId = getZoneId(ZONE_NAME);

        assertEquals(zoneId, distributionZoneManager.getZoneId(ZONE_NAME));
        assertEquals(zoneId, zonesConfiguration.distributionZones().get(ZONE_NAME).zoneId().value());

        dropZone(ZONE_NAME);

        createZone(NEW_ZONE_NAME);

        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);

        assertEquals(defaultZoneId, distributionZoneManager.getZoneId(DEFAULT_ZONE_NAME));
        assertEquals(defaultZoneId, zonesConfiguration.defaultDistributionZone().zoneId().value());

        int zoneId2 = getZoneId(NEW_ZONE_NAME);

        assertEquals(zoneId2, distributionZoneManager.getZoneId(NEW_ZONE_NAME));
        assertEquals(zoneId2, zonesConfiguration.distributionZones().get(NEW_ZONE_NAME).zoneId().value());
    }

    @Test
    public void testGetNotExistingZoneIdByName() {
        createZone(ZONE_NAME);

        assertThrows(DistributionZoneNotFoundException.class, () -> distributionZoneManager.getZoneId(NEW_ZONE_NAME),
                "Expected exception was not thrown.");
    }

    private void createZone(String zoneName) {
        DistributionZonesTestUtil.createZone(distributionZoneManager, zoneName, null, null, null);
    }

    private void dropZone(String zoneName) {
        DistributionZonesTestUtil.dropZone(distributionZoneManager, zoneName);
    }

    private int getZoneId(String zoneName) {
        return DistributionZonesTestUtil.getZoneIdStrict(zonesConfiguration, zoneName);
    }
}
