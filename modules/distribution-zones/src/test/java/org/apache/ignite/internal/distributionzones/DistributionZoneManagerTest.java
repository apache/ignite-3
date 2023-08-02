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
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters.Builder;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.FilterValidator;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.impl.TestPersistStorageConfigurationSchema;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.lang.DistributionZoneAlreadyExistsException;
import org.apache.ignite.lang.DistributionZoneBindTableException;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.hamcrest.Matchers;
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
    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    private ConfigurationTreeGenerator generator;

    private ConfigurationRegistry registry;

    private DistributionZoneManager distributionZoneManager;

    @InjectConfiguration("mock.tables.fooTable {}")
    private TablesConfiguration tablesConfiguration;

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
                "node",
                zonesConfiguration,
                tablesConfiguration,
                null,
                null,
                null,
                // TODO: IGNITE-20114 вернуться позже
                mock(CatalogManager.class)
        );
    }

    @AfterEach
    public void tearDown() throws Exception {
        registry.stop();
        generator.close();
    }

    @Test
    public void testCreateZoneWithAutoAdjust() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
                )
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1, "Zone was not created.");
        assertEquals(ZONE_NAME, zone1.name().value(), "Zone name is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone1.dataNodesAutoAdjustScaleUp().value(), "dataNodesAutoAdjustScaleUp is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone1.dataNodesAutoAdjustScaleDown().value(), "dataNodesAutoAdjustScaleDown is wrong.");
        assertEquals(100, zone1.dataNodesAutoAdjust().value(), "dataNodesAutoAdjust is wrong.");
    }

    @Test
    public void testCreateZoneWithAutoAdjustScaleUp() throws Exception {
        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(100).build()
                )
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1, "Zone was not created.");
        assertEquals(ZONE_NAME, zone1.name().value(), "Zone name is wrong.");
        assertEquals(100, zone1.dataNodesAutoAdjustScaleUp().value(), "dataNodesAutoAdjustScaleUp is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone1.dataNodesAutoAdjustScaleDown().value(), "dataNodesAutoAdjustScaleDown is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone1.dataNodesAutoAdjust().value(), "dataNodesAutoAdjust is wrong.");

        distributionZoneManager.dropZone(ZONE_NAME).get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNull(zone1, "Zone was not dropped.");
    }

    @Test
    public void testCreateZoneWithAutoAdjustScaleDown() throws Exception {
        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleDown(200).build()
                )
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1, "Zone was not created.");
        assertEquals(ZONE_NAME, zone1.name().value(), "Zone name is wrong.");
        assertEquals(IMMEDIATE_TIMER_VALUE, zone1.dataNodesAutoAdjustScaleUp().value(), "dataNodesAutoAdjustScaleUp is wrong.");
        assertEquals(200, zone1.dataNodesAutoAdjustScaleDown().value(), "dataNodesAutoAdjustScaleDown is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone1.dataNodesAutoAdjust().value(), "dataNodesAutoAdjust is wrong.");

        distributionZoneManager.dropZone(ZONE_NAME).get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNull(zone1, "Zone was not dropped.");
    }

    @Test
    public void testCreateZoneIfExists() throws Exception {
        Exception e = null;

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
        ).get(5, TimeUnit.SECONDS);

        CompletableFuture<Integer> fut;

        fut = distributionZoneManager.createZone(
                new Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
        );

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof DistributionZoneAlreadyExistsException,
                "Unexpected type of exception (requires DistributionZoneAlreadyExistsException): " + e
        );
    }

    @Test
    public void testDropZoneIfNotExists() {
        Exception e = null;

        CompletableFuture<Void> fut = distributionZoneManager.dropZone(ZONE_NAME);

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof DistributionZoneNotFoundException,
                "Unexpected type of exception (requires DistributionZoneNotFoundException): " + e
        );
    }

    @Test
    public void testUpdateDefaultZone() throws Exception {
        testUpdateZone(DEFAULT_ZONE_NAME);
    }

    @Test
    public void testUpdateNotDefaultZone() throws Exception {
        testUpdateZone(ZONE_NAME);
    }

    private void testUpdateZone(String zoneName) throws Exception {
        if (DEFAULT_ZONE_NAME.equals(zoneName)) {
            distributionZoneManager.alterZone(
                            zoneName,
                            new DistributionZoneConfigurationParameters.Builder(zoneName).dataNodesAutoAdjust(100).build()
                    )
                    .get(5, TimeUnit.SECONDS);
        } else {
            distributionZoneManager.createZone(
                            new DistributionZoneConfigurationParameters.Builder(zoneName).dataNodesAutoAdjust(100).build()
                    )
                    .get(5, TimeUnit.SECONDS);
        }

        DistributionZoneConfiguration zone = getZoneFromRegistry(zoneName);

        assertNotNull(zone, "Zone was not created.");
        assertEquals(zoneName, zone.name().value(), "Zone name is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp().value(), "dataNodesAutoAdjustScaleUp is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown().value(), "dataNodesAutoAdjustScaleDown is wrong.");
        assertEquals(100, zone.dataNodesAutoAdjust().value(), "dataNodesAutoAdjust is wrong.");


        distributionZoneManager.alterZone(zoneName, new DistributionZoneConfigurationParameters.Builder(zoneName)
                        .dataNodesAutoAdjustScaleUp(200).dataNodesAutoAdjustScaleDown(300).build())
                .get(5, TimeUnit.SECONDS);

        zone = getZoneFromRegistry(zoneName);

        assertNotNull(zone, "Zone was not created.");
        assertEquals(200, zone.dataNodesAutoAdjustScaleUp().value(), "dataNodesAutoAdjustScaleUp is wrong.");
        assertEquals(300, zone.dataNodesAutoAdjustScaleDown().value(), "dataNodesAutoAdjustScaleDown is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust().value(), "dataNodesAutoAdjust is wrong.");


        distributionZoneManager.alterZone(zoneName, new DistributionZoneConfigurationParameters.Builder(zoneName)
                        .dataNodesAutoAdjustScaleUp(400).build())
                .get(5, TimeUnit.SECONDS);

        zone = getZoneFromRegistry(zoneName);

        assertNotNull(zone, "Zone was not created.");
        assertEquals(400, zone.dataNodesAutoAdjustScaleUp().value(), "dataNodesAutoAdjustScaleUp is wrong.");
        assertEquals(300, zone.dataNodesAutoAdjustScaleDown().value(), "dataNodesAutoAdjustScaleDown is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust().value(), "dataNodesAutoAdjust is wrong.");


        distributionZoneManager.alterZone(zoneName, new DistributionZoneConfigurationParameters.Builder(zoneName)
                        .dataNodesAutoAdjust(500).build())
                .get(5, TimeUnit.SECONDS);

        zone = getZoneFromRegistry(zoneName);

        assertNotNull(zone, "Zone was not created.");
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp().value(), "dataNodesAutoAdjustScaleUp is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown().value(), "dataNodesAutoAdjustScaleDown is wrong.");
        assertEquals(500, zone.dataNodesAutoAdjust().value(), "dataNodesAutoAdjust is wrong.");
    }

    private DistributionZoneConfiguration getZoneFromRegistry(String zoneName) {
        if (DEFAULT_ZONE_NAME.equals(zoneName)) {
            return registry.getConfiguration(DistributionZonesConfiguration.KEY).defaultDistributionZone();
        } else {
            return registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                    .get(zoneName);
        }
    }

    @Test
    public void testRenameZone() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
                )
                .get(5, TimeUnit.SECONDS);

        distributionZoneManager.alterZone(ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(NEW_ZONE_NAME).build())
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        DistributionZoneConfiguration zone2 = registry.getConfiguration(DistributionZonesConfiguration.KEY)
                .distributionZones()
                .get(NEW_ZONE_NAME);

        assertNull(zone1, "Zone was not renamed.");
        assertNotNull(zone2, "Zone was not renamed.");
        assertEquals(NEW_ZONE_NAME, zone2.name().value(), "Zone was not renamed.");
        assertEquals(INFINITE_TIMER_VALUE, zone2.dataNodesAutoAdjustScaleUp().value(), "dataNodesAutoAdjustScaleUp is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone2.dataNodesAutoAdjustScaleDown().value(), "dataNodesAutoAdjustScaleDown is wrong.");
        assertEquals(100, zone2.dataNodesAutoAdjust().value(), "dataNodesAutoAdjust is wrong.");
    }

    @Test
    public void testUpdateAndRenameZone() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
                )
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1, "Zone was not renamed.");
        assertEquals(ZONE_NAME, zone1.name().value(), "Zone was not renamed.");
        assertEquals(INFINITE_TIMER_VALUE, zone1.dataNodesAutoAdjustScaleUp().value(), "dataNodesAutoAdjustScaleUp is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone1.dataNodesAutoAdjustScaleDown().value(), "dataNodesAutoAdjustScaleDown is wrong.");
        assertEquals(100, zone1.dataNodesAutoAdjust().value(), "dataNodesAutoAdjust is wrong.");

        distributionZoneManager.alterZone(ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(NEW_ZONE_NAME).dataNodesAutoAdjust(400).build())
                .get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        DistributionZoneConfiguration zone2 = registry.getConfiguration(DistributionZonesConfiguration.KEY)
                .distributionZones()
                .get(NEW_ZONE_NAME);

        assertNull(zone1, "Zone was not renamed.");
        assertNotNull(zone2, "Zone was not renamed.");
        assertEquals(NEW_ZONE_NAME, zone2.name().value(), "Zone was not renamed.");
        assertEquals(INFINITE_TIMER_VALUE, zone2.dataNodesAutoAdjustScaleUp().value(), "dataNodesAutoAdjustScaleUp is wrong.");
        assertEquals(INFINITE_TIMER_VALUE, zone2.dataNodesAutoAdjustScaleDown().value(), "dataNodesAutoAdjustScaleDown is wrong.");
        assertEquals(400, zone2.dataNodesAutoAdjust().value(), "dataNodesAutoAdjust is wrong.");
    }

    @Test
    public void testAlterZoneRename1() {
        Exception e = null;

        CompletableFuture<Void> fut = distributionZoneManager
                .alterZone(ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(NEW_ZONE_NAME)
                .dataNodesAutoAdjust(100).build());

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof DistributionZoneNotFoundException,
                "Unexpected type of exception (requires DistributionZoneRenameException): " + e
        );
    }

    @Test
    public void testAlterZoneRename2() throws Exception {
        Exception e = null;

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                .dataNodesAutoAdjust(100).build()).get(5, TimeUnit.SECONDS);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(NEW_ZONE_NAME)
                .dataNodesAutoAdjust(100).build()).get(5, TimeUnit.SECONDS);

        CompletableFuture<Void> fut = distributionZoneManager.alterZone(ZONE_NAME, new Builder(NEW_ZONE_NAME)
                .dataNodesAutoAdjust(100).build());

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof DistributionZoneAlreadyExistsException,
                "Unexpected type of exception (requires DistributionZoneRenameException): " + e
        );
    }

    @Test
    public void testAlterZoneIfExists() {
        Exception e = null;

        CompletableFuture<Void> fut = distributionZoneManager.alterZone(ZONE_NAME, new Builder(ZONE_NAME)
                .dataNodesAutoAdjust(100).build());

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof DistributionZoneNotFoundException,
                "Unexpected type of exception (requires DistributionZoneNotFoundException): " + e
        );
    }

    @Test
    public void testCreateZoneWithWrongAutoAdjust() {
        Exception e = null;

        CompletableFuture<Integer> fut = distributionZoneManager.createZone(new Builder(ZONE_NAME)
                .dataNodesAutoAdjust(-10).build());

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof ConfigurationValidationException,
                "Unexpected type of exception (requires ConfigurationValidationException): " + e
        );
    }

    @Test
    public void testCreateZoneWithWrongSeparatedAutoAdjust1() {
        Exception e = null;

        CompletableFuture<Integer> fut = distributionZoneManager.createZone(new Builder(ZONE_NAME)
                .dataNodesAutoAdjustScaleUp(-100).dataNodesAutoAdjustScaleDown(1).build());

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof ConfigurationValidationException,
                "Unexpected type of exception (requires ConfigurationValidationException): " + e
        );
    }

    @Test
    public void testCreateZoneWithWrongSeparatedAutoAdjust2() {
        Exception e = null;

        CompletableFuture<Integer> fut = distributionZoneManager.createZone(new Builder(ZONE_NAME)
                .dataNodesAutoAdjustScaleUp(1).dataNodesAutoAdjustScaleDown(-100).build());

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof ConfigurationValidationException,
                "Unexpected type of exception (requires ConfigurationValidationException): " + e
        );
    }

    @Test
    public void testCreateZoneWithNullConfiguration() {
        Exception e = null;

        CompletableFuture<Integer> fut = distributionZoneManager.createZone(null);

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof IllegalArgumentException,
                "Unexpected type of exception (requires IllegalArgumentException): " + e
        );
        assertEquals(
                "Distribution zone configuration is null",
                e.getCause().getMessage(),
                "Unexpected exception message: " + e.getCause().getMessage()
        );
    }

    @Test
    public void testAlterZoneWithNullName() {
        Exception e = null;

        CompletableFuture<Void> fut = distributionZoneManager.alterZone(null, new Builder(ZONE_NAME).build());

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof IllegalArgumentException,
                "Unexpected type of exception (requires IllegalArgumentException): " + e
        );
        assertThat(
                "Unexpected exception message: " + e.getCause().getMessage(),
                e.getCause().getMessage(),
                Matchers.containsString("Distribution zone name is null")
        );
    }

    @Test
    public void testAlterZoneWithNullConfiguration() {
        Exception e = null;

        CompletableFuture<Void> fut = distributionZoneManager.alterZone(ZONE_NAME, null);

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof IllegalArgumentException,
                "Unexpected type of exception (requires IllegalArgumentException): " + e
        );
        assertEquals(
                "Distribution zone configuration is null",
                e.getCause().getMessage(),
                "Unexpected exception message: " + e.getCause().getMessage()
        );
    }

    @Test
    public void testDropZoneWithNullName() {
        Exception e = null;

        CompletableFuture<Void> fut = distributionZoneManager.dropZone(null);

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof IllegalArgumentException,
                "Unexpected type of exception (requires IllegalArgumentException): " + e
        );
        assertThat(
                "Unexpected exception message: " + e.getCause().getMessage(),
                e.getCause().getMessage(),
                Matchers.containsString("Distribution zone name is null")
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

    @Test
    public void testTryDropZoneBoundToTable() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()
                )
                .get(5, TimeUnit.SECONDS);

        bindZoneToTable(ZONE_NAME);

        CompletableFuture<Void> fut = distributionZoneManager.dropZone(ZONE_NAME);

        Exception e = null;

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof DistributionZoneBindTableException,
                "Unexpected type of exception (requires DistributionZoneBindTableException): " + e
        );
    }

    @Test
    public void testTryCreateDefaultZone() {
        Exception e = null;

        CompletableFuture<Integer> fut = distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME).build()
        );

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof IllegalArgumentException,
                "Unexpected type of exception (requires IllegalArgumentException): " + e
        );
        assertEquals(
                "It's not possible to create distribution zone with [name= " + DEFAULT_ZONE_NAME + ']',
                e.getCause().getMessage(),
                "Unexpected exception message: " + e.getCause().getMessage()
        );
    }

    @Test
    public void testTryRenameDefaultZone() {
        Exception e = null;

        CompletableFuture<Void> fut = distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(NEW_ZONE_NAME).build()
        );

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof IllegalArgumentException,
                "Unexpected type of exception (requires IllegalArgumentException): " + e
        );
        assertEquals(
                "It's not possible to rename default distribution zone",
                e.getCause().getMessage(),
                "Unexpected exception message: " + e.getCause().getMessage()
        );
    }

    @Test
    public void testTryRenameToDefaultZoneName() {
        Exception e = null;

        CompletableFuture<Void> fut = distributionZoneManager.alterZone(NEW_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME).build()
        );

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof IllegalArgumentException,
                "Unexpected type of exception (requires IllegalArgumentException): " + e
        );
        assertEquals(
                "It's not possible to rename distribution zone to [name= " + DEFAULT_ZONE_NAME + ']',
                e.getCause().getMessage(),
                "Unexpected exception message: " + e.getCause().getMessage()
        );
    }

    @Test
    public void testTryDropDefaultZone() {
        Exception e = null;

        CompletableFuture<Void> fut = distributionZoneManager.dropZone(DEFAULT_ZONE_NAME);

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertNotNull(e, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof IllegalArgumentException,
                "Unexpected type of exception (requires IllegalArgumentException): " + e
        );
        assertEquals(
                "Default distribution zone cannot be dropped.",
                e.getCause().getMessage(),
                "Unexpected exception message: " + e.getCause().getMessage()
        );
    }

    @Test
    public void testCreateZoneWithFilter() throws Exception {
        String expectedFilter = "['nodeAttributes'][?(@.['region'] == 'EU')]";

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).filter(expectedFilter).build()
        ).get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertEquals(expectedFilter, zone1.filter().value());

        distributionZoneManager.dropZone(ZONE_NAME).get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNull(zone1, "Zone was not dropped.");
    }

    @Test
    public void testAlterZoneWithFilter() throws Exception {
        String expectedFilter = "['nodeAttributes'][?(@.['region'] == 'EU')]";

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                                .filter(expectedFilter).build()
                )
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertEquals(expectedFilter, zone1.filter().value());

        String newExpectedFilter = "['nodeAttributes'][?(@.['storage'] == 'SSD')]";

        distributionZoneManager.alterZone(
                        ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                                .filter(newExpectedFilter).build()
                ).get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertEquals(newExpectedFilter, zone1.filter().value());

        distributionZoneManager.dropZone(ZONE_NAME).get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNull(zone1, "Zone was not dropped.");
    }

    @Test
    public void testCreateZoneWithNotValidFilter() {
        assertThrowsWithCause(
                () -> distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                                .filter("['nodeAttributes'[?(@.['region'] == 'EU')]").build()
                ).get(5, TimeUnit.SECONDS),
                ConfigurationValidationException.class,
                "Failed to parse filter ['nodeAttributes'[?(@.['region'] == 'EU')], the cause: Property must be separated by comma"
        );
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testAlterZoneWithNotValidFilter() throws Exception {
        String expectedFilter = "['nodeAttributes'][?(@.['region'] == 'EU')]";

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .filter(expectedFilter).build()
                )
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertEquals(expectedFilter, zone1.filter().value());

        String notValidFilter = "['nodeAttributes[?(@.['region'] == 'EU')]";

        assertThrowsWithCause(
                () -> distributionZoneManager.alterZone(
                        ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                                .filter(notValidFilter).build()
                        ).get(5, TimeUnit.SECONDS),
                ConfigurationValidationException.class,
                "Failed to parse filter ['nodeAttributes[?(@.['region'] == 'EU')], the cause: Property must be separated by comma"
        );

        distributionZoneManager.dropZone(ZONE_NAME).get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNull(zone1, "Zone was not dropped.");
    }

    private void bindZoneToTable(String zoneName) throws Exception {
        int zoneId = distributionZoneManager.getZoneId(zoneName);

        tablesConfiguration.change(ch -> ch.changeTables(tables -> tables.update("fooTable", chg -> chg.changeZoneId(zoneId))))
                .get(5, TimeUnit.SECONDS);
    }
}
