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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DistributionZoneConfigurationParameters}.
 */
class DistributionZoneConfigurationParametersTest {
    private static final String ZONE_NAME = "zone1";

    @Test
    public void testDefaultValues() {
        DistributionZoneConfigurationParameters zoneCfg = new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build();

        assertEquals(ZONE_NAME, zoneCfg.name());
        assertEquals(null, zoneCfg.dataNodesAutoAdjust());
        assertEquals(null, zoneCfg.dataNodesAutoAdjustScaleUp());
        assertEquals(null, zoneCfg.dataNodesAutoAdjustScaleDown());
    }

    @Test
    public void testAutoAdjust() {
        DistributionZoneConfigurationParameters zoneCfg = new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                .dataNodesAutoAdjust(100)
                .build();

        assertEquals(ZONE_NAME, zoneCfg.name());
        assertEquals(100, zoneCfg.dataNodesAutoAdjust());
        assertEquals(null, zoneCfg.dataNodesAutoAdjustScaleUp());
        assertEquals(null, zoneCfg.dataNodesAutoAdjustScaleDown());
    }

    @Test
    public void testAutoAdjustScaleUp() {
        DistributionZoneConfigurationParameters zoneCfg = new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                .dataNodesAutoAdjustScaleUp(100)
                .build();

        assertEquals(ZONE_NAME, zoneCfg.name());
        assertEquals(null, zoneCfg.dataNodesAutoAdjust());
        assertEquals(100, zoneCfg.dataNodesAutoAdjustScaleUp());
        assertEquals(null, zoneCfg.dataNodesAutoAdjustScaleDown());
    }

    @Test
    public void testAutoAdjustScaleDown() {
        DistributionZoneConfigurationParameters zoneCfg = new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                .dataNodesAutoAdjustScaleDown(100)
                .build();

        assertEquals(ZONE_NAME, zoneCfg.name());
        assertEquals(null, zoneCfg.dataNodesAutoAdjust());
        assertEquals(null, zoneCfg.dataNodesAutoAdjustScaleUp());
        assertEquals(100, zoneCfg.dataNodesAutoAdjustScaleDown());
    }

    @Test
    public void testIncompatibleValues1() {
        assertThrows(IllegalArgumentException.class,
                () -> new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjust(1)
                        .dataNodesAutoAdjustScaleUp(1)
                        .build());
    }

    @Test
    public void testIncompatibleValues2() {
        assertThrows(IllegalArgumentException.class,
                () -> new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjust(1)
                        .dataNodesAutoAdjustScaleDown(1)
                        .build());
    }

    @Test
    public void testNullName() {
        assertThrows(IllegalArgumentException.class,
                () -> new DistributionZoneConfigurationParameters.Builder(null));
    }

    @Test
    public void testEmptyName() {
        assertThrows(IllegalArgumentException.class,
                () -> new DistributionZoneConfigurationParameters.Builder(""));
    }
}
