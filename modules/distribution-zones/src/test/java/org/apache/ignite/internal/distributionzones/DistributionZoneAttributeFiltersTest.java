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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterNodeAttributes;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests scenarios for filtering attributes of a node.
 */
public class DistributionZoneAttributeFiltersTest {
    @Test
    void testNodeAttributesFilterScenario1() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.dataRegionSize == 10)]";

        assertTrue(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario2() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.region == 'US')]";

        assertTrue(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario3() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.dataRegionSize == 10 && @.region == 'US')]";

        assertTrue(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario4() {
        Map<String, String> newAttributesMap = Map.of("storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.dataRegionSize == 10 && @.region == 'US')]";

        assertFalse(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario5() {
        Map<String, String> newAttributesMap = Map.of();

        String filter = "$[?(@.dataRegionSize == 10 && @.region == 'US')]";

        assertFalse(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario6() {
        Map<String, String> newAttributesMap = Map.of("storage", "SSD");

        String filter = "$[?(@.storage == 'SSD' && @.region == 'US')]";

        assertFalse(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario7() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        assertTrue(filterNodeAttributes(newAttributesMap, DEFAULT_FILTER));
    }

    @Test
    void testNodeAttributesFilterScenario8() {
        Map<String, String> newAttributesMap = Map.of();

        assertTrue(filterNodeAttributes(newAttributesMap, DEFAULT_FILTER));
    }

    @Test
    void testNodeAttributesFilterScenario9() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$.[?(@.newValue == 100)]";

        assertFalse(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario10() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$.[?(@.newValue != 100)]";

        assertTrue(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario11() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$.[?(@.newValue && @.newValue != 100)]";

        assertFalse(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario12() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.dataRegionSize != 10)]";

        assertFalse(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario13() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.region != 'EU')]";

        assertTrue(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario14() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.region != 'EU' && @.dataRegionSize > 5)]";

        assertTrue(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario15() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.region != 'EU' && @.dataRegionSize < 5)]";

        assertFalse(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario16() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.dataRegionSize > 5 && @.dataRegionSize < 5)]";

        assertFalse(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario17() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.region == 'US' && @.region == 'EU')]";

        assertFalse(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario18() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10");

        String filter = "$[?(@.region == 'EU' || @.dataRegionSize > 5)]";

        assertTrue(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario19() {
        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD");

        String filter = "region";

        assertTrue(filterNodeAttributes(newAttributesMap, filter));
    }

    @Test
    void testNodeAttributesFilterScenario20() {
        Map<String, String> newAttributesMap = Map.of("storage", "SSD");

        String filter = "region";

        assertFalse(filterNodeAttributes(newAttributesMap, filter));
    }
}
