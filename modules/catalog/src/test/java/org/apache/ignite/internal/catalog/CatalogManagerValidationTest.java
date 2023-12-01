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

import static org.apache.ignite.internal.catalog.CatalogTestUtils.alterZoneBuilder;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createZoneBuilder;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_PARTITION_COUNT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Catalog manager validation test.
 */
public class CatalogManagerValidationTest extends BaseCatalogManagerTest {
    private static final String ZONE_NAME = "test_zone";

    @Test
    void testValidateZoneNameOnCreateZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> manager.execute(CreateZoneCommand.builder().build()),
                "Name of the zone can't be null or blank"
        );

        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZoneNameOnAlterZone() {
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThrows(
                CatalogValidationException.class,
                () -> DropZoneCommand.builder().build(),
                "Name of the zone can't be null or blank"
        );

        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZonePartitionsOnCreateZone() {
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
                () -> createZoneBuilder(ZONE_NAME).partitions(65_001).build(),
                "Invalid number of partitions"
        );

        // Let's check the success cases.
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME + 0).partitions(1).build()), willBe(nullValue()));
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME + 1).partitions(MAX_PARTITION_COUNT).build()), willBe(nullValue()));
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME + 2).partitions(10).build()), willBe(nullValue()));
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME + 3).partitions(DEFAULT_PARTITION_COUNT).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZonePartitionsOnAlterZone() {
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

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
                () -> alterZoneBuilder(ZONE_NAME).partitions(65_001).build(),
                "Invalid number of partitions"
        );

        // Let's check the success cases.
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).partitions(1).build()), willBe(nullValue()));
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).partitions(MAX_PARTITION_COUNT).build()), willBe(nullValue()));
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).partitions(10).build()), willBe(nullValue()));
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).partitions(DEFAULT_PARTITION_COUNT).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZoneReplicasOnCreateZone() {
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
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME + 0).replicas(1).build()), willBe(nullValue()));
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME + 1).replicas(Integer.MAX_VALUE).build()), willBe(nullValue()));
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME + 2).replicas(DEFAULT_REPLICA_COUNT).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZoneReplicasOnAlterZone() {
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

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
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).replicas(1).build()), willBe(nullValue()));
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).replicas(Integer.MAX_VALUE).build()), willBe(nullValue()));
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).replicas(DEFAULT_REPLICA_COUNT).build()), willBe(nullValue()));
    }

    @Test
    void testValidateDataNodesAutoAdjustOnCreateZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(-1).build(),
                "Invalid data nodes auto adjust"
        );

        // Let's check the success cases.
        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 0).dataNodesAutoAdjust(INFINITE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 1).dataNodesAutoAdjust(IMMEDIATE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 2).dataNodesAutoAdjust(10).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustOnAlterZone() {
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(-1).build(),
                "Invalid data nodes auto adjust"
        );

        // Let's check the success cases.
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(INFINITE_TIMER_VALUE).build()), willBe(nullValue()));

        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(IMMEDIATE_TIMER_VALUE).build()), willBe(nullValue()));

        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(10).build()), willBe(nullValue()));
    }

    @Test
    void testValidateDataNodesAutoAdjustScaleUpOnCreateZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(-1).build(),
                "Invalid data nodes auto adjust scale up"
        );

        // Let's check the success cases.
        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 0).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 1).dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 2).dataNodesAutoAdjustScaleUp(10).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustScaleUpOnAlterZone() {
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(-1).build(),
                "Invalid data nodes auto adjust scale up"
        );

        // Let's check the success cases.
        assertThat(
                manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(10).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustScaleDownOnCreateZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> createZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(-1).build(),
                "Invalid data nodes auto adjust scale down"
        );

        // Let's check the success cases.
        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 0).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 1).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 2).dataNodesAutoAdjustScaleDown(10).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustScaleDownOnAlterZone() {
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThrows(
                CatalogValidationException.class,
                () -> alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(-1).build(),
                "Invalid data nodes auto adjust scale down"
        );

        // Let's check the success cases.
        assertThat(
                manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(10).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustCompatibilityParametersOnCreateZone() {
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
        assertThat(
                manager.execute(createZoneParams(ZONE_NAME + 0, IMMEDIATE_TIMER_VALUE, null, null)),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneParams(ZONE_NAME + 1, INFINITE_TIMER_VALUE, null, null)),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneParams(ZONE_NAME + 2, 66, null, null)),
                willBe(nullValue())
        );

        // Scale up + scale down.
        assertThat(
                manager.execute(createZoneParams(ZONE_NAME + 3, null, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE)),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneParams(ZONE_NAME + 4, null, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE)),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneParams(ZONE_NAME + 5, null, 77, 88)),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustCompatibilityParametersOnAlterZone() {
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

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

        // Auto adjust only.
        assertThat(
                manager.execute(alterZoneParams(IMMEDIATE_TIMER_VALUE, null, null)),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(alterZoneParams(INFINITE_TIMER_VALUE, null, null)),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(alterZoneParams(66, null, null)),
                willBe(nullValue())
        );

        // Scale up + scale down.
        assertThat(
                manager.execute(alterZoneParams(null, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE)),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(alterZoneParams(null, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE)),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(alterZoneParams(null, 77, 88)),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateFilterOnCreateZone() {
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
        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 0).filter("['nodeAttributes'][?(@.['region'] == 'EU')]").build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(createZoneBuilder(ZONE_NAME + 1).filter(DEFAULT_FILTER).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateFilterOnAlterZone() {
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

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
        assertThat(
                manager.execute(alterZoneBuilder(ZONE_NAME).filter("['nodeAttributes'][?(@.['region'] == 'EU')]").build()),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(alterZoneBuilder(ZONE_NAME).filter(DEFAULT_FILTER).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateZoneNameOnDropZone() {
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThrows(
                CatalogValidationException.class,
                () -> DropZoneCommand.builder().build(),
                "Name of the zone can't be null or blank"
        );

        // Let's check the success cases.
        assertThat(manager.execute(DropZoneCommand.builder().zoneName(ZONE_NAME).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZoneNamesOnRenameZone() {
        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThrows(
                CatalogValidationException.class,
                () -> RenameZoneCommand.builder().build(),
                "Name of the zone can't be null or blank");

        assertThrows(
                CatalogValidationException.class,
                () -> RenameZoneCommand.builder().zoneName(ZONE_NAME).build(),
                "New zone name can't be null or blank");

        // Let's check the success cases.
        assertThat(
                manager.execute(RenameZoneCommand.builder().zoneName(ZONE_NAME).newZoneName(ZONE_NAME + 0).build()),
                willBe(nullValue())
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

    private static CatalogCommand alterZoneParams(@Nullable Integer autoAdjust, @Nullable Integer scaleUp, @Nullable Integer scaleDown) {
        return alterZoneBuilder(ZONE_NAME)
                .dataNodesAutoAdjust(autoAdjust)
                .dataNodesAutoAdjustScaleUp(scaleUp)
                .dataNodesAutoAdjustScaleDown(scaleDown)
                .build();
    }
}
