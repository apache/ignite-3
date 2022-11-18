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

package org.apache.ignite.internal;

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.exception.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for distribution zone manager.
 */
class DistributionZoneManagerTest extends IgniteAbstractTest {
    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    private final ConfigurationRegistry registry = new ConfigurationRegistry(
            List.of(DistributionZonesConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(DISTRIBUTED),
            List.of(),
            List.of()
    );

    private DistributionZoneManager distributionZoneManager;

    @BeforeEach
    public void setUp() {
        registry.start();

        registry.initializeDefaults();

        DistributionZonesConfiguration zonesConfiguration = registry.getConfiguration(DistributionZonesConfiguration.KEY);
        distributionZoneManager = new DistributionZoneManager(zonesConfiguration);
    }

    @AfterEach
    public void tearDown() throws Exception {
        registry.stop();
    }

    @Test
    public void testCreateZoneWithAutoAdjust() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneCfg.Builder().name(ZONE_NAME).dataNodesAutoAdjust(100).build()
                )
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1);
        assertEquals(ZONE_NAME, zone1.name().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjustScaleUp().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjustScaleDown().value());
        assertEquals(100, zone1.dataNodesAutoAdjust().value());
    }

    @Test
    public void testCreateZoneWithAutoAdjustScaleUp() throws Exception {
        distributionZoneManager.createZone(
                new DistributionZoneCfg.Builder().name(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(100).build()
                )
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1);
        assertEquals(ZONE_NAME, zone1.name().value());
        assertEquals(100, zone1.dataNodesAutoAdjustScaleUp().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjustScaleDown().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjust().value());

        distributionZoneManager.dropZone(ZONE_NAME).get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNull(zone1);
    }

    @Test
    public void testCreateZoneWithAutoAdjustScaleDown() throws Exception {
        distributionZoneManager.createZone(
                new DistributionZoneCfg.Builder().name(ZONE_NAME)
                        .dataNodesAutoAdjustScaleDown(200).build()
                )
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1);
        assertEquals(ZONE_NAME, zone1.name().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjustScaleUp().value());
        assertEquals(200, zone1.dataNodesAutoAdjustScaleDown().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjust().value());

        distributionZoneManager.dropZone(ZONE_NAME).get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNull(zone1);
    }

    @Test
    public void testCreateZoneIfExists() throws Exception {
        Exception e = null;

        distributionZoneManager.createZone(
                new DistributionZoneCfg.Builder().name(ZONE_NAME).dataNodesAutoAdjust(100).build()
        ).get(5, TimeUnit.SECONDS);

        try {
            distributionZoneManager.createZone(
                    new DistributionZoneCfg.Builder().name(ZONE_NAME).dataNodesAutoAdjust(100).build()
            ).get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof DistributionZoneAlreadyExistsException, e.toString());
    }

    @Test
    public void testDropZoneIfNotExists() {
        Exception e = null;

        try {
            distributionZoneManager.dropZone(ZONE_NAME).get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause().getCause() instanceof DistributionZoneNotFoundException, e.toString());
    }

    @Test
    public void testUpdateZone() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneCfg.Builder().name(ZONE_NAME).dataNodesAutoAdjust(100).build()
                )
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1);
        assertEquals(ZONE_NAME, zone1.name().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjustScaleUp().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjustScaleDown().value());
        assertEquals(100, zone1.dataNodesAutoAdjust().value());


        distributionZoneManager.alterZone(ZONE_NAME, new DistributionZoneCfg.Builder().name(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(200).dataNodesAutoAdjustScaleDown(300).build())
                .get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1);
        assertEquals(200, zone1.dataNodesAutoAdjustScaleUp().value());
        assertEquals(300, zone1.dataNodesAutoAdjustScaleDown().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjust().value());


        distributionZoneManager.alterZone(ZONE_NAME, new DistributionZoneCfg.Builder().name(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(400).build())
                .get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1);
        assertEquals(400, zone1.dataNodesAutoAdjustScaleUp().value());
        assertEquals(300, zone1.dataNodesAutoAdjustScaleDown().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjust().value());


        distributionZoneManager.alterZone(ZONE_NAME, new DistributionZoneCfg.Builder().name(ZONE_NAME)
                        .dataNodesAutoAdjust(500).build())
                .get(5, TimeUnit.SECONDS);

        zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        assertNotNull(zone1);
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjustScaleUp().value());
        assertEquals(Integer.MAX_VALUE, zone1.dataNodesAutoAdjustScaleDown().value());
        assertEquals(500, zone1.dataNodesAutoAdjust().value());
    }

    @Test
    public void testRenameZone() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneCfg.Builder().name(ZONE_NAME).dataNodesAutoAdjust(100).build()
                )
                .get(5, TimeUnit.SECONDS);

        distributionZoneManager.alterZone(ZONE_NAME,
                        new DistributionZoneCfg.Builder().name(NEW_ZONE_NAME).build())
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        DistributionZoneConfiguration zone2 = registry.getConfiguration(DistributionZonesConfiguration.KEY)
                .distributionZones()
                .get(NEW_ZONE_NAME);

        assertNull(zone1);
        assertNotNull(zone2);
        assertEquals(NEW_ZONE_NAME, zone2.name().value());
        assertEquals(Integer.MAX_VALUE, zone2.dataNodesAutoAdjustScaleUp().value());
        assertEquals(Integer.MAX_VALUE, zone2.dataNodesAutoAdjustScaleDown().value());
        assertEquals(100, zone2.dataNodesAutoAdjust().value());
    }

    @Test
    public void testUpdateAndRenameZone() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneCfg.Builder().name(ZONE_NAME).dataNodesAutoAdjust(100).build()
                )
                .get(5, TimeUnit.SECONDS);

        distributionZoneManager.alterZone(ZONE_NAME,
                        new DistributionZoneCfg.Builder().name(NEW_ZONE_NAME).dataNodesAutoAdjust(400).build())
                .get(5, TimeUnit.SECONDS);

        DistributionZoneConfiguration zone1 = registry.getConfiguration(DistributionZonesConfiguration.KEY).distributionZones()
                .get(ZONE_NAME);

        DistributionZoneConfiguration zone2 = registry.getConfiguration(DistributionZonesConfiguration.KEY)
                .distributionZones()
                .get(NEW_ZONE_NAME);

        assertNull(zone1);
        assertNotNull(zone2);
        assertEquals(NEW_ZONE_NAME, zone2.name().value());
        assertEquals(Integer.MAX_VALUE, zone2.dataNodesAutoAdjustScaleUp().value());
        assertEquals(Integer.MAX_VALUE, zone2.dataNodesAutoAdjustScaleDown().value());
        assertEquals(400, zone2.dataNodesAutoAdjust().value());
    }

    @Test
    public void testAlterZoneIfExists() {
        Exception e = null;

        try {
            distributionZoneManager.alterZone(ZONE_NAME, new DistributionZoneCfg.Builder()
                    .name(ZONE_NAME).dataNodesAutoAdjust(100).build()).get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof DistributionZoneNotFoundException, e.toString());
    }

    @Test
    public void testCreateZoneWithWrongAutoAdjust() throws Exception {
        Exception e = null;

        try {
            distributionZoneManager.createZone(new DistributionZoneCfg.Builder()
                    .name(ZONE_NAME).dataNodesAutoAdjust(-10).build()).get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof ConfigurationChangeException, e.toString());
    }

    @Test
    public void testCreateZoneWithWrongSeparatedAutoAdjust1() throws Exception {
        Exception e = null;

        try {
            distributionZoneManager.createZone(new DistributionZoneCfg.Builder().name(ZONE_NAME)
                    .dataNodesAutoAdjustScaleUp(-100).dataNodesAutoAdjustScaleDown(1).build()).get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof ConfigurationChangeException, e.toString());
    }

    @Test
    public void testCreateZoneWithWrongSeparatedAutoAdjust2() throws Exception {
        Exception e = null;

        try {
            distributionZoneManager.createZone(new DistributionZoneCfg.Builder().name(ZONE_NAME)
                    .dataNodesAutoAdjustScaleUp(1).dataNodesAutoAdjustScaleDown(-100).build()).get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof ConfigurationChangeException, e.toString());
    }

    @Test
    public void testCreateZoneWithNullName() throws Exception {
        Exception e = null;

        try {
            distributionZoneManager.createZone(new DistributionZoneCfg.Builder()
                    .dataNodesAutoAdjustScaleUp(1).dataNodesAutoAdjustScaleDown(-100).build()).get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof NullPointerException, e.toString());
    }

    @Test
    public void testCreateZoneWithNullConfiguration() throws Exception {
        Exception e = null;

        try {
            distributionZoneManager.createZone(null).get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof NullPointerException, e.toString());
    }

    @Test
    public void testAlterZoneWithNullName() throws Exception {
        Exception e = null;

        try {
            distributionZoneManager.alterZone(null, new DistributionZoneCfg.Builder().build())
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof NullPointerException, e.toString());
    }

    @Test
    public void testAlterZoneWithNullConfiguration() throws Exception {
        Exception e = null;

        try {
            distributionZoneManager.alterZone(ZONE_NAME, null)
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof NullPointerException, e.toString());
    }

    @Test
    public void testDropZoneWithNullName() throws Exception {
        Exception e = null;

        try {
            distributionZoneManager.dropZone(null)
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause().getCause() instanceof NullPointerException, e.toString());
    }
}
