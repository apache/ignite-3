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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createZoneBuilder;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_PARTITION_COUNT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link CreateZoneCommand}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class CreateZoneCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void zoneNameMustNotBeNullOrBlank(String zone) {
        assertThrows(
                CatalogValidationException.class,
                () -> CreateZoneCommand.builder().zoneName(zone).build(),
                "Name of the zone can't be null or blank"
        );
    }

    @Test
    void zonePartitions() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).partitions(-1).build(),
                "Invalid number of partitions"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).partitions(0).build(),
                "Invalid number of partitions"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).partitions(MAX_PARTITION_COUNT + 1).build(),
                "Invalid number of partitions"
        );

        // Let's check the success cases.
        createZoneBuilder(ZONE_NAME + 0).partitions(1).build();
        createZoneBuilder(ZONE_NAME + 1).partitions(MAX_PARTITION_COUNT).build();
        createZoneBuilder(ZONE_NAME + 2).partitions(10).build();
        createZoneBuilder(ZONE_NAME + 3).partitions(DEFAULT_PARTITION_COUNT).build();
    }

    @Test
    void zoneReplicas() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).replicas(-1).build(),
                "Invalid number of replicas"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).replicas(0).build(),
                "Invalid number of replicas"
        );

        // Let's check the success cases.
        createZoneBuilder(ZONE_NAME + 0).replicas(1).build();
        createZoneBuilder(ZONE_NAME + 1).replicas(Integer.MAX_VALUE).build();
        createZoneBuilder(ZONE_NAME + 2).replicas(DEFAULT_REPLICA_COUNT).build();
    }

    @Test
    void zoneAutoAdjust() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(-1).build(),
                "Invalid data nodes auto adjust"
        );

        // Let's check the success cases.
        createZoneBuilder(ZONE_NAME + 0).dataNodesAutoAdjust(INFINITE_TIMER_VALUE).build();
        createZoneBuilder(ZONE_NAME + 1).dataNodesAutoAdjust(IMMEDIATE_TIMER_VALUE).build();
        createZoneBuilder(ZONE_NAME + 2).dataNodesAutoAdjust(10).build();
    }

    @Test
    void zoneAutoAdjustScaleUp() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(-1).build(),
                "Invalid data nodes auto adjust scale up"
        );

        // Let's check the success cases.
        createZoneBuilder(ZONE_NAME + 0).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build();
        createZoneBuilder(ZONE_NAME + 1).dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build();
        createZoneBuilder(ZONE_NAME + 2).dataNodesAutoAdjustScaleUp(10).build();
    }

    @Test
    void zoneAutoAdjustScaleDown() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(-1).build(),
                "Invalid data nodes auto adjust scale down"
        );

        // Let's check the success cases.
        createZoneBuilder(ZONE_NAME + 0).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build();
        createZoneBuilder(ZONE_NAME + 1).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build();
        createZoneBuilder(ZONE_NAME + 2).dataNodesAutoAdjustScaleDown(10).build();
    }

    @Test
    void zoneAutoAdjustCompatibility() {
        // Auto adjust + scale up.
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneParams(66, IMMEDIATE_TIMER_VALUE, null),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneParams(66, INFINITE_TIMER_VALUE, null),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneParams(66, 77, null),
                "Not compatible parameters"
        );

        // Auto adjust + scale down.
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneParams(66, null, IMMEDIATE_TIMER_VALUE),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneParams(66, null, INFINITE_TIMER_VALUE),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneParams(66, null, 88),
                "Not compatible parameters"
        );

        // Auto adjust + scale up + scale down.
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneParams(66, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneParams(66, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneParams(66, 77, 88),
                "Not compatible parameters"
        );

        // Let's check the success cases.

        // Auto adjust only.
        createZoneParams(ZONE_NAME + 0, IMMEDIATE_TIMER_VALUE, null, null);
        createZoneParams(ZONE_NAME + 1, INFINITE_TIMER_VALUE, null, null);
        createZoneParams(ZONE_NAME + 2, 66, null, null);
        createZoneParams(ZONE_NAME + 3, null, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);
        createZoneParams(ZONE_NAME + 4, null, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE);
        createZoneParams(ZONE_NAME + 5, null, 77, 88);
    }

    @Test
    void zoneFilter() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).filter("not a JsonPath").build(),
                "Invalid filter"
        );

        // Missing ']' after 'nodeAttributes'.
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).filter("['nodeAttributes'[?(@.['region'] == 'EU')]").build(),
                "Invalid filter"
        );

        // Let's check the success cases.
        createZoneBuilder(ZONE_NAME + 0).filter("['nodeAttributes'][?(@.['region'] == 'EU')]").build();
        createZoneBuilder(ZONE_NAME + 1).filter(DEFAULT_FILTER).build();
    }

    @Test
    void zoneStorageProfiles() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).storageProfilesParams(List.of()).build(),
                "Storage profile cannot be empty"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).storageProfilesParams(null).build(),
                "Storage profile cannot be null"
        );

        // Let's check the success case.
        createZoneBuilder(ZONE_NAME + 0).storageProfilesParams(
                List.of(StorageProfileParams.builder().storageProfile("lru_rocks").build())).build();
    }

    @Test
    void exceptionIsThrownIfZoneAlreadyExist() {
        CreateZoneCommandBuilder builder = CreateZoneCommand.builder();

        Catalog catalog = catalogWithZone("some_zone");

        CatalogCommand command = builder
                .zoneName("some_zone")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Distribution zone with name 'some_zone' already exists"
        );
    }

    private static CatalogCommand createZoneParams(@Nullable Integer autoAdjust, @Nullable Integer scaleUp, @Nullable Integer scaleDown) {
        return createZoneParams(ZONE_NAME, autoAdjust, scaleUp, scaleDown);
    }

    private static CatalogCommand createZoneParams(
            String zoneName,
            @Nullable Integer autoAdjust,
            @Nullable Integer scaleUp,
            @Nullable Integer scaleDown
    ) {
        return createZoneBuilder(zoneName)
                .dataNodesAutoAdjust(autoAdjust)
                .dataNodesAutoAdjustScaleUp(scaleUp)
                .dataNodesAutoAdjustScaleDown(scaleDown)
                .build();
    }
}
