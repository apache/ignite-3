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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterStorageProfiles;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseStorageProfiles;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests storage profiles filtering. */
public class DistributionZoneStorageProfilesFilterTest {
    @Test
    void testStorageProfilesScenario1() {
        List<String> nodeStorageProfiles = List.of("qwe", "asd", "zxc");
        NodeWithAttributes node = new NodeWithAttributes("n", "id", Map.of(), nodeStorageProfiles);

        String zoneStorageProfiles = "qwe,asd";

        assertTrue(filterStorageProfiles(node, fromParams(parseStorageProfiles(zoneStorageProfiles)).profiles()));
    }

    @Test
    void testStorageProfilesScenario2() {
        List<String> nodeStorageProfiles = List.of("asd", "zxc");
        NodeWithAttributes node = new NodeWithAttributes("n", "id", Map.of(), nodeStorageProfiles);

        String zoneStorageProfiles = "qwe,asd";

        assertFalse(filterStorageProfiles(node, fromParams(parseStorageProfiles(zoneStorageProfiles)).profiles()));
    }

    @Test
    void testStorageProfilesScenario3() {
        List<String> nodeStorageProfiles = List.of("asd", "zxc");
        NodeWithAttributes node = new NodeWithAttributes("n", "id", Map.of(), nodeStorageProfiles);

        String zoneStorageProfiles = "zxc,asd";

        assertTrue(filterStorageProfiles(node, fromParams(parseStorageProfiles(zoneStorageProfiles)).profiles()));
    }

    @Test
    void testStorageProfilesScenario4() {
        List<String> nodeStorageProfiles = List.of("asd", "zxc");
        NodeWithAttributes node = new NodeWithAttributes("n", "id", Map.of(), nodeStorageProfiles);

        String zoneStorageProfiles = "zxc,   asd";

        assertTrue(filterStorageProfiles(node, fromParams(parseStorageProfiles(zoneStorageProfiles)).profiles()));
    }
}
