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

package org.apache.ignite.internal.distributionzones.rebalance;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.distributionzones.ZoneMetricSource;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests metric source name and zone metric names.
 * If you want to change the name, or add a new metric, please don't forget to update the corresponding documentation.
 */
@ExtendWith(MockitoExtension.class)
public class ZoneMetricSourceTest extends BaseIgniteAbstractTest {
    private static final String TEST_ZONE_NAME = "test_zone";

    private static final String TEST_NODE_NAME = "test_node";

    @Mock
    MetaStorageManager metaStorageManager;

    @Test
    void testMetricSourceName() {
        var metricSource = createTestZoneMetricSource();

        assertThat(ZoneMetricSource.SOURCE_NAME, is("zones"));
        assertThat(metricSource.name(), is("zones." + TEST_ZONE_NAME));
    }

    @Test
    void testMetricNames() {
        var metricSource = createTestZoneMetricSource();

        MetricSet set = metricSource.enable();

        assertThat(set, is(notNullValue()));

        Set<String> expectedMetrics = Set.of(
                "LocalUnrebalancedPartitionsCount",
                "TotalUnrebalancedPartitionsCount");

        var actualMetrics = new HashSet<String>();
        set.forEach(m -> actualMetrics.add(m.name()));

        assertThat(actualMetrics, is(expectedMetrics));
    }

    /**
     * Creates a test {@link ZoneMetricSource} with a predefined zone descriptor.
     *
     * @return A new instance of {@link ZoneMetricSource} with a test zone descriptor.
     */
    private ZoneMetricSource createTestZoneMetricSource() {
        var zoneDescriptor = new CatalogZoneDescriptor(
                123,
                TEST_ZONE_NAME,
                1024,
                1,
                1,
                1,
                1,
                CatalogUtils.DEFAULT_FILTER,
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor("default"))),
                ConsistencyMode.STRONG_CONSISTENCY
                );

        return new ZoneMetricSource(metaStorageManager, TEST_NODE_NAME, zoneDescriptor);
    }
}
