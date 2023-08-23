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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STORAGE_ENGINE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParamsAndPreviousValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** For {@link CatalogUtils} testing. */
public class CatalogUtilsTest {
    private static final String ZONE_NAME = "test_zone";

    @Test
    void testFromParamsCreateZoneWithoutAutoAdjustFields() {
        CreateZoneParams params = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(2)
                .replicas(3)
                .filter("test_filter")
                .dataStorage(DataStorageParams.builder().engine("test_engine").dataRegion("test_region").build())
                .build();

        CatalogZoneDescriptor descriptor = fromParams(1, params);

        assertEquals(1, descriptor.id());
        assertEquals(ZONE_NAME, descriptor.name());
        assertEquals(2, descriptor.partitions());
        assertEquals(3, descriptor.replicas());
        assertEquals("test_filter", descriptor.filter());
        assertEquals("test_engine", descriptor.dataStorage().engine());
        assertEquals("test_region", descriptor.dataStorage().dataRegion());
    }

    @Test
    void testFromParamsCreateZoneWithDefaults() {
        CatalogZoneDescriptor descriptor = fromParams(2, CreateZoneParams.builder().zoneName(ZONE_NAME).build());

        assertEquals(2, descriptor.id());
        assertEquals(ZONE_NAME, descriptor.name());
        assertEquals(DEFAULT_PARTITION_COUNT, descriptor.partitions());
        assertEquals(DEFAULT_REPLICA_COUNT, descriptor.replicas());
        assertEquals(INFINITE_TIMER_VALUE, descriptor.dataNodesAutoAdjust());
        assertEquals(IMMEDIATE_TIMER_VALUE, descriptor.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, descriptor.dataNodesAutoAdjustScaleDown());
        assertEquals(DEFAULT_FILTER, descriptor.filter());
        assertEquals(DEFAULT_STORAGE_ENGINE, descriptor.dataStorage().engine());
        assertEquals(DEFAULT_DATA_REGION, descriptor.dataStorage().dataRegion());
    }

    @Test
    void testFromParamsCreateZoneWithAutoAdjustFields() {
        checkAutoAdjustParams(
                fromParams(1, createZoneParams(1, 2, 3)),
                1, 2, 3
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(1, null, null)),
                1, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(1, 2, null)),
                1, 2, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(1, null, 3)),
                1, INFINITE_TIMER_VALUE, 3
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(null, 2, null)),
                INFINITE_TIMER_VALUE, 2, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(null, null, 3)),
                INFINITE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, 3
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(null, 2, 3)),
                INFINITE_TIMER_VALUE, 2, 3
        );
    }

    @Test
    void testFromParamsAndPreviousValueWithoutAutoAdjustFields() {
        AlterZoneParams params = AlterZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(2)
                .replicas(3)
                .filter("test_filter")
                .dataStorage(DataStorageParams.builder().engine("test_engine").dataRegion("test_region").build())
                .build();

        CatalogZoneDescriptor descriptor = fromParamsAndPreviousValue(params, createPreviousZoneWithDefaults());

        assertEquals(1, descriptor.id());
        assertEquals(ZONE_NAME, descriptor.name());
        assertEquals(2, descriptor.partitions());
        assertEquals(3, descriptor.replicas());
        assertEquals("test_filter", descriptor.filter());
        assertEquals("test_engine", descriptor.dataStorage().engine());
        assertEquals("test_region", descriptor.dataStorage().dataRegion());
    }

    @Test
    void testFromParamsAndPreviousValueWithPreviousValues() {
        CreateZoneParams createZoneParams = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(11)
                .replicas(22)
                .dataNodesAutoAdjust(33)
                .dataNodesAutoAdjustScaleUp(44)
                .dataNodesAutoAdjustScaleDown(55)
                .filter("previous_filter")
                .dataStorage(DataStorageParams.builder().engine("previous_engine").dataRegion("previous_region").build())
                .build();

        CatalogZoneDescriptor previous = fromParams(10, createZoneParams);

        CatalogZoneDescriptor descriptor = fromParamsAndPreviousValue(AlterZoneParams.builder().zoneName(ZONE_NAME).build(), previous);

        assertEquals(10, descriptor.id());
        assertEquals(ZONE_NAME, descriptor.name());
        assertEquals(11, descriptor.partitions());
        assertEquals(22, descriptor.replicas());
        assertEquals(33, descriptor.dataNodesAutoAdjust());
        assertEquals(44, descriptor.dataNodesAutoAdjustScaleUp());
        assertEquals(55, descriptor.dataNodesAutoAdjustScaleDown());
        assertEquals("previous_filter", descriptor.filter());
        assertEquals("previous_engine", descriptor.dataStorage().engine());
        assertEquals("previous_region", descriptor.dataStorage().dataRegion());
    }

    @Test
    void testFromParamsAndPreviousValueWithAutoAdjustFields() {
        checkAutoAdjustParams(
                fromParamsAndPreviousValue(alterZoneParams(1, 2, 3), createPreviousZoneWithDefaults()),
                1, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParamsAndPreviousValue(alterZoneParams(1, null, null), createPreviousZoneWithDefaults()),
                1, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParamsAndPreviousValue(alterZoneParams(null, 2, null), createPreviousZoneWithDefaults()),
                INFINITE_TIMER_VALUE, 2, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParamsAndPreviousValue(alterZoneParams(null, null, 3), createPreviousZoneWithDefaults()),
                INFINITE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, 3
        );

        checkAutoAdjustParams(
                fromParamsAndPreviousValue(alterZoneParams(null, 2, 3), createPreviousZoneWithDefaults()),
                INFINITE_TIMER_VALUE, 2, 3
        );
    }

    private static CreateZoneParams createZoneParams(@Nullable Integer autoAdjust, @Nullable Integer scaleUp, @Nullable Integer scaleDown) {
        return CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .dataNodesAutoAdjust(autoAdjust)
                .dataNodesAutoAdjustScaleUp(scaleUp)
                .dataNodesAutoAdjustScaleDown(scaleDown)
                .build();
    }

    private static AlterZoneParams alterZoneParams(@Nullable Integer autoAdjust, @Nullable Integer scaleUp, @Nullable Integer scaleDown) {
        return AlterZoneParams.builder()
                .zoneName(ZONE_NAME)
                .dataNodesAutoAdjust(autoAdjust)
                .dataNodesAutoAdjustScaleUp(scaleUp)
                .dataNodesAutoAdjustScaleDown(scaleDown)
                .build();
    }

    private static void checkAutoAdjustParams(CatalogZoneDescriptor descriptor, int expAutoAdjust, int expScaleUp, int expScaleDown) {
        assertEquals(expAutoAdjust, descriptor.dataNodesAutoAdjust());
        assertEquals(expScaleUp, descriptor.dataNodesAutoAdjustScaleUp());
        assertEquals(expScaleDown, descriptor.dataNodesAutoAdjustScaleDown());
    }

    private static CatalogZoneDescriptor createPreviousZoneWithDefaults() {
        return fromParams(1, CreateZoneParams.builder().zoneName(ZONE_NAME).build());
    }
}
