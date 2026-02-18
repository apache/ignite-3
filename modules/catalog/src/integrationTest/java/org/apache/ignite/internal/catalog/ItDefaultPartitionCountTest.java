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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.system.CpuInformationProvider;
import org.apache.ignite.internal.system.JvmCpuInformationProvider;
import org.junit.jupiter.api.Test;

class ItDefaultPartitionCountTest extends ClusterPerTestIntegrationTest {
    private static final CpuInformationProvider cpuInfoProvider = new JvmCpuInformationProvider();

    @Override
    protected boolean shouldCreateDefaultZone() {
        return false;
    }

    @Test
    public void defaultPartitionCountTest() {
        // Triggers lazy default zone creation.
        sql("CREATE TABLE IF NOT EXISTS test_table (id INT, val VARCHAR, PRIMARY KEY (id))");

        assertEquals(partitionCount(CatalogUtils.DEFAULT_REPLICA_COUNT), zoneDescriptor(CatalogUtils.DEFAULT_ZONE_NAME).partitions());

        sql("CREATE ZONE TEST_ZONE_R1 WITH REPLICAS=1, STORAGE_PROFILES='" + CatalogService.DEFAULT_STORAGE_PROFILE + "'");

        assertEquals(partitionCount(1), zoneDescriptor("TEST_ZONE_R1").partitions());

        sql("CREATE ZONE TEST_ZONE_R3 WITH REPLICAS=3, STORAGE_PROFILES='" + CatalogService.DEFAULT_STORAGE_PROFILE + "'");

        assertEquals(partitionCount(3), zoneDescriptor("TEST_ZONE_R3").partitions());

        // 23 is just some simple number.
        sql("CREATE ZONE TEST_ZONE_P23 WITH PARTITIONS=23, REPLICAS=3, STORAGE_PROFILES='" + CatalogService.DEFAULT_STORAGE_PROFILE + "'");

        assertEquals(23, zoneDescriptor("TEST_ZONE_P23").partitions());
    }

    private int partitionCount(int replicas) {
        return initialNodes()
                * cpuInfoProvider.availableProcessors()
                * DataNodesAwarePartitionCountCalculator.SCALE_FACTOR
                / replicas;
    }

    private CatalogZoneDescriptor zoneDescriptor(String zoneName) {
        IgniteImpl node = unwrapIgniteImpl(node(0));

        CatalogZoneDescriptor descriptor = node.catalogManager().latestCatalog().zone(zoneName);

        assertNotNull(descriptor);

        return descriptor;
    }

    private void sql(String sql) {
        node(0).sql().executeScript(sql);
    }
}
