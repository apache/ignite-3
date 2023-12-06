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

import static org.apache.ignite.internal.catalog.CatalogTestUtils.alterZoneBuilder;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_PARTITION_COUNT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify validation of {@link AlterZoneCommand}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class AlterZoneCommandValidationTest extends AbstractCommandValidationTest {
    private static final String ZONE_NAME = "test_zone";

    @Test
    void testValidateZoneNameOnAlterZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> AlterZoneCommand.builder().build(),
                "Name of the zone can't be null or blank"
        );
    }

    @Test
    void testValidateZonePartitionsOnAlterZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).partitions(-1).build(),
                "Invalid number of partitions"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).partitions(0).build(),
                "Invalid number of partitions"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).partitions(MAX_PARTITION_COUNT + 1).build(),
                "Invalid number of partitions"
        );

        // Let's check the success cases.
        alterZoneBuilder(ZONE_NAME).partitions(1).build();
        alterZoneBuilder(ZONE_NAME).partitions(MAX_PARTITION_COUNT).build();
        alterZoneBuilder(ZONE_NAME).partitions(10).build();
        alterZoneBuilder(ZONE_NAME).partitions(DEFAULT_PARTITION_COUNT).build();
    }

    @Test
    void testValidateZoneReplicasOnAlterZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).replicas(-1).build(),
                "Invalid number of replicas"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).replicas(0).build(),
                "Invalid number of replicas"
        );

        // Let's check the success cases.
        alterZoneBuilder(ZONE_NAME).replicas(1).build();
        alterZoneBuilder(ZONE_NAME).replicas(Integer.MAX_VALUE).build();
        alterZoneBuilder(ZONE_NAME).replicas(DEFAULT_REPLICA_COUNT).build();
    }

    @Test
    void testValidateDataNodesAutoAdjustOnAlterZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(-1).build(),
                "Invalid data nodes auto adjust"
        );

        // Let's check the success cases.
        alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(INFINITE_TIMER_VALUE).build();
        alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(IMMEDIATE_TIMER_VALUE).build();
        alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(10).build();
    }

    @Test
    void testValidateDataNodesAutoAdjustScaleUpOnAlterZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(-1).build(),
                "Invalid data nodes auto adjust scale up"
        );

        // Let's check the success cases.
        alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build();
        alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build();
        alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(10).build();
    }

    @Test
    void testValidateDataNodesAutoAdjustScaleDownOnAlterZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(-1).build(),
                "Invalid data nodes auto adjust scale down"
        );

        // Let's check the success cases.
        alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build();
        alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build();
        alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(10).build();
    }

    @Test
    void testValidateDataNodesAutoAdjustCompatibilityParametersOnAlterZone() {
        // Auto adjust + scale up.
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneParams(66, IMMEDIATE_TIMER_VALUE, null),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneParams(66, INFINITE_TIMER_VALUE, null),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneParams(66, 77, null),
                "Not compatible parameters"
        );

        // Auto adjust + scale down.
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneParams(66, null, IMMEDIATE_TIMER_VALUE),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneParams(66, null, INFINITE_TIMER_VALUE),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneParams(66, null, 88),
                "Not compatible parameters"
        );

        // Auto adjust + scale up + scale down.
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneParams(66, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneParams(66, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE),
                "Not compatible parameters"
        );

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneParams(66, 77, 88),
                "Not compatible parameters"
        );

        // Let's check the success cases.
        alterZoneParams(IMMEDIATE_TIMER_VALUE, null, null);
        alterZoneParams(INFINITE_TIMER_VALUE, null, null);
        alterZoneParams(66, null, null);
        alterZoneParams(null, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);
        alterZoneParams(null, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE);
        alterZoneParams(null, 77, 88);
    }

    @Test
    void testValidateFilterOnAlterZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).filter("not a JsonPath").build(),
                "Invalid filter"
        );

        // Missing ']' after 'nodeAttributes'.
        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).filter("['nodeAttributes'[?(@.['region'] == 'EU')]").build(),
                "Invalid filter"
        );

        // Let's check the success cases.
        alterZoneBuilder(ZONE_NAME).filter("['nodeAttributes'][?(@.['region'] == 'EU')]").build();
        alterZoneBuilder(ZONE_NAME).filter(DEFAULT_FILTER).build();
    }

    @Test
    void testValidateZoneNamesOnRenameZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> RenameZoneCommand.builder().build(),
                "Name of the zone can't be null or blank");

        assertThrows(
                CatalogValidationException.class,
                () -> RenameZoneCommand.builder().zoneName(ZONE_NAME).build(),
                "New zone name can't be null or blank");

        // Let's check the success cases.
        RenameZoneCommand.builder().zoneName(ZONE_NAME).newZoneName(ZONE_NAME + 0).build();
    }

    private static CatalogCommand alterZoneParams(@Nullable Integer autoAdjust, @Nullable Integer scaleUp, @Nullable Integer scaleDown) {
        return alterZoneBuilder(ZONE_NAME)
                .dataNodesAutoAdjust(autoAdjust)
                .dataNodesAutoAdjustScaleUp(scaleUp)
                .dataNodesAutoAdjustScaleDown(scaleDown)
                .build();
    }
}
