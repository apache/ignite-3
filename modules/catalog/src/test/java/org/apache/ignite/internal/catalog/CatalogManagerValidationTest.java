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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_FIRST;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Catalog manager validation test.
 */
public class CatalogManagerValidationTest extends BaseCatalogManagerTest {
    private static final String ZONE_NAME = "test_zone";

    @Test
    void testValidateZoneNameOnCreateZone() {
        assertThat(
                manager.createZone(CreateZoneParams.builder().build()),
                willThrowFast(CatalogValidationException.class, "Missing zone name")
        );

        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZoneNameOnAlterZone() {
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThat(
                manager.alterZone(AlterZoneParams.builder().build()),
                willThrowFast(CatalogValidationException.class, "Missing zone name")
        );

        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZonePartitionsOnCreateZone() {
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME).partitions(-1).build()),
                willThrowFast(CatalogValidationException.class, "Invalid number of partitions")
        );

        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME).partitions(0).build()),
                willThrowFast(CatalogValidationException.class, "Invalid number of partitions")
        );

        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME).partitions(65_001).build()),
                willThrowFast(CatalogValidationException.class, "Invalid number of partitions")
        );

        // Let's check the success cases.
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME + 0).partitions(1).build()), willBe(nullValue()));
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME + 1).partitions(MAX_PARTITION_COUNT).build()), willBe(nullValue()));
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME + 2).partitions(10).build()), willBe(nullValue()));
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME + 3).partitions(DEFAULT_PARTITION_COUNT).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZonePartitionsOnAlterZone() {
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).partitions(-1).build()),
                willThrowFast(CatalogValidationException.class, "Invalid number of partitions")
        );

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).partitions(0).build()),
                willThrowFast(CatalogValidationException.class, "Invalid number of partitions")
        );

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).partitions(65_001).build()),
                willThrowFast(CatalogValidationException.class, "Invalid number of partitions")
        );

        // Let's check the success cases.
        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).partitions(1).build()), willBe(nullValue()));
        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).partitions(MAX_PARTITION_COUNT).build()), willBe(nullValue()));
        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).partitions(10).build()), willBe(nullValue()));
        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).partitions(DEFAULT_PARTITION_COUNT).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZoneReplicasOnCreateZone() {
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME).replicas(-1).build()),
                willThrowFast(CatalogValidationException.class, "Invalid number of replicas")
        );

        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME).replicas(0).build()),
                willThrowFast(CatalogValidationException.class, "Invalid number of replicas")
        );

        // Let's check the success cases.
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME + 0).replicas(1).build()), willBe(nullValue()));
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME + 1).replicas(Integer.MAX_VALUE).build()), willBe(nullValue()));
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME + 2).replicas(DEFAULT_REPLICA_COUNT).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZoneReplicasOnAlterZone() {
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).replicas(-1).build()),
                willThrowFast(CatalogValidationException.class, "Invalid number of replicas")
        );

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).replicas(0).build()),
                willThrowFast(CatalogValidationException.class, "Invalid number of replicas")
        );

        // Let's check the success cases.
        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).replicas(1).build()), willBe(nullValue()));
        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).replicas(Integer.MAX_VALUE).build()), willBe(nullValue()));
        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).replicas(DEFAULT_REPLICA_COUNT).build()), willBe(nullValue()));
    }

    @Test
    void testValidateDataNodesAutoAdjustOnCreateZone() {
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(-1).build()),
                willThrowFast(CatalogValidationException.class, "Invalid data nodes auto adjust")
        );

        // Let's check the success cases.
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 0).dataNodesAutoAdjust(INFINITE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 1).dataNodesAutoAdjust(IMMEDIATE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 2).dataNodesAutoAdjust(10).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustOnAlterZone() {
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(-1).build()),
                willThrowFast(CatalogValidationException.class, "Invalid data nodes auto adjust")
        );

        // Let's check the success cases.
        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(INFINITE_TIMER_VALUE).build()), willBe(nullValue()));

        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(IMMEDIATE_TIMER_VALUE).build()), willBe(nullValue()));

        assertThat(manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(10).build()), willBe(nullValue()));
    }

    @Test
    void testValidateDataNodesAutoAdjustScaleUpOnCreateZone() {
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(-1).build()),
                willThrowFast(CatalogValidationException.class, "Invalid data nodes auto adjust scale up")
        );

        // Let's check the success cases.
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 0).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 1).dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 2).dataNodesAutoAdjustScaleUp(10).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustScaleUpOnAlterZone() {
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(-1).build()),
                willThrowFast(CatalogValidationException.class, "Invalid data nodes auto adjust scale up")
        );

        // Let's check the success cases.
        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(10).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustScaleDownOnCreateZone() {
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(-1).build()),
                willThrowFast(CatalogValidationException.class, "Invalid data nodes auto adjust scale down")
        );

        // Let's check the success cases.
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 0).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 1).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 2).dataNodesAutoAdjustScaleDown(10).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustScaleDownOnAlterZone() {
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(-1).build()),
                willThrowFast(CatalogValidationException.class, "Invalid data nodes auto adjust scale down")
        );

        // Let's check the success cases.
        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build()),
                willBe(nullValue())
        );

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(10).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustCompatibilityParametersOnCreateZone() {
        // Auto adjust + scale up.
        assertThat(
                manager.createZone(createZoneParams(66, IMMEDIATE_TIMER_VALUE, null)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.createZone(createZoneParams(66, INFINITE_TIMER_VALUE, null)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.createZone(createZoneParams(66, 77, null)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        // Auto adjust + scale down.
        assertThat(
                manager.createZone(createZoneParams(66, null, IMMEDIATE_TIMER_VALUE)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.createZone(createZoneParams(66, null, INFINITE_TIMER_VALUE)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.createZone(createZoneParams(66, null, 88)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        // Auto adjust + scale up + scale down.
        assertThat(
                manager.createZone(createZoneParams(66, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.createZone(createZoneParams(66, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.createZone(createZoneParams(66, 77, 88)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        // Let's check the success cases.

        // Auto adjust only.
        assertThat(
                manager.createZone(createZoneParams(ZONE_NAME + 0, IMMEDIATE_TIMER_VALUE, null, null)),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneParams(ZONE_NAME + 1, INFINITE_TIMER_VALUE, null, null)),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneParams(ZONE_NAME + 2, 66, null, null)),
                willBe(nullValue())
        );

        // Scale up + scale down.
        assertThat(
                manager.createZone(createZoneParams(ZONE_NAME + 3, null, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE)),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneParams(ZONE_NAME + 4, null, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE)),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneParams(ZONE_NAME + 5, null, 77, 88)),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateDataNodesAutoAdjustCompatibilityParametersOnAlterZone() {
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        // Auto adjust + scale up.
        assertThat(
                manager.alterZone(alterZoneParams(66, IMMEDIATE_TIMER_VALUE, null)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.alterZone(alterZoneParams(66, INFINITE_TIMER_VALUE, null)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.alterZone(alterZoneParams(66, 77, null)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        // Auto adjust + scale down.
        assertThat(
                manager.alterZone(alterZoneParams(66, null, IMMEDIATE_TIMER_VALUE)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.alterZone(alterZoneParams(66, null, INFINITE_TIMER_VALUE)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.alterZone(alterZoneParams(66, null, 88)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        // Auto adjust + scale up + scale down.
        assertThat(
                manager.alterZone(alterZoneParams(66, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.alterZone(alterZoneParams(66, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        assertThat(
                manager.alterZone(alterZoneParams(66, 77, 88)),
                willThrowFast(CatalogValidationException.class, "Not compatible parameters")
        );

        // Let's check the success cases.

        // Auto adjust only.
        assertThat(
                manager.alterZone(alterZoneParams(IMMEDIATE_TIMER_VALUE, null, null)),
                willBe(nullValue())
        );

        assertThat(
                manager.alterZone(alterZoneParams(INFINITE_TIMER_VALUE, null, null)),
                willBe(nullValue())
        );

        assertThat(
                manager.alterZone(alterZoneParams(66, null, null)),
                willBe(nullValue())
        );

        // Scale up + scale down.
        assertThat(
                manager.alterZone(alterZoneParams(null, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE)),
                willBe(nullValue())
        );

        assertThat(
                manager.alterZone(alterZoneParams(null, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE)),
                willBe(nullValue())
        );

        assertThat(
                manager.alterZone(alterZoneParams(null, 77, 88)),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateFilterOnCreateZone() {
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME).filter("not a JsonPath").build()),
                willThrowFast(CatalogValidationException.class, "Invalid filter")
        );

        // Missing ']' after 'nodeAttributes'.
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME).filter("['nodeAttributes'[?(@.['region'] == 'EU')]").build()),
                willThrowFast(CatalogValidationException.class, "Invalid filter")
        );

        // Let's check the success cases.
        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 0).filter("['nodeAttributes'][?(@.['region'] == 'EU')]").build()),
                willBe(nullValue())
        );

        assertThat(
                manager.createZone(createZoneBuilder(ZONE_NAME + 1).filter(DEFAULT_FILTER).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateFilterOnAlterZone() {
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).filter("not a JsonPath").build()),
                willThrowFast(CatalogValidationException.class, "Invalid filter")
        );

        // Missing ']' after 'nodeAttributes'.
        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).filter("['nodeAttributes'[?(@.['region'] == 'EU')]").build()),
                willThrowFast(CatalogValidationException.class, "Invalid filter")
        );

        // Let's check the success cases.
        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).filter("['nodeAttributes'][?(@.['region'] == 'EU')]").build()),
                willBe(nullValue())
        );

        assertThat(
                manager.alterZone(alterZoneBuilder(ZONE_NAME).filter(DEFAULT_FILTER).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateZoneNameOnDropZone() {
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThat(
                manager.dropZone(DropZoneParams.builder().build()),
                willThrowFast(CatalogValidationException.class, "Missing zone name")
        );

        // Let's check the success cases.
        assertThat(manager.dropZone(DropZoneParams.builder().zoneName(ZONE_NAME).build()), willBe(nullValue()));
    }

    @Test
    void testValidateZoneNamesOnRenameZone() {
        assertThat(manager.createZone(createZoneBuilder(ZONE_NAME).build()), willBe(nullValue()));

        assertThat(
                manager.renameZone(RenameZoneParams.builder().build()),
                willThrowFast(CatalogValidationException.class, "Missing zone name")
        );

        assertThat(
                manager.renameZone(RenameZoneParams.builder().zoneName(ZONE_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Missing new zone name")
        );

        // Let's check the success cases.
        assertThat(
                manager.renameZone(RenameZoneParams.builder().zoneName(ZONE_NAME).newZoneName(ZONE_NAME + 0).build()),
                willBe(nullValue())
        );
    }

    @Test
    void testValidateTableNameOnIndexCreation() {
        assertThat(
                manager.createIndex(CreateHashIndexParams.builder().schemaName(DEFAULT_SCHEMA_NAME).indexName(INDEX_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Missing table name")
        );

        assertThat(
                manager.createIndex(CreateSortedIndexParams.builder().schemaName(DEFAULT_SCHEMA_NAME).indexName(INDEX_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Missing table name")
        );
    }

    @Test
    void testValidateIndexNameOnIndexCreation() {
        assertThat(
                manager.createIndex(CreateHashIndexParams.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(TABLE_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Missing index name")
        );

        assertThat(
                manager.createIndex(CreateSortedIndexParams.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(TABLE_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Missing index name")
        );
    }

    @Test
    void testValidateIndexColumnsNotSpecifiedOnIndexCreation() {
        assertThat(
                manager.createIndex(createHashIndexParams(INDEX_NAME, null)),
                willThrowFast(CatalogValidationException.class, "Columns not specified")
        );

        assertThat(
                manager.createIndex(createSortedIndexParams(INDEX_NAME, null, null)),
                willThrowFast(CatalogValidationException.class, "Columns not specified")
        );

        assertThat(
                manager.createIndex(createHashIndexParams(INDEX_NAME, List.of())),
                willThrowFast(CatalogValidationException.class, "Columns not specified")
        );

        assertThat(
                manager.createIndex(createSortedIndexParams(INDEX_NAME, List.of(), null)),
                willThrowFast(CatalogValidationException.class, "Columns not specified")
        );
    }

    @Test
    void testValidateIndexColumnsDuplicatesOnIndexCreation() {
        assertThat(
                manager.createIndex(createHashIndexParams(INDEX_NAME, Arrays.asList("key", "key"))),
                willThrowFast(CatalogValidationException.class, "Duplicate columns are present")
        );

        assertThat(
                manager.createIndex(createSortedIndexParams(INDEX_NAME, Arrays.asList("key", "key"), null)),
                willThrowFast(CatalogValidationException.class, "Duplicate columns are present")
        );
    }

    @Test
    void testValidateIndexColumnsCollationsNotSpecifiedOnIndexCreation() {
        assertThat(
                manager.createIndex(createSortedIndexParams(INDEX_NAME, List.of("key"), null)),
                willThrowFast(CatalogValidationException.class, "Columns collations not specified")
        );

        assertThat(
                manager.createIndex(createSortedIndexParams(INDEX_NAME, List.of("key"), List.of())),
                willThrowFast(CatalogValidationException.class, "Columns collations not specified")
        );
    }

    @Test
    void testValidateIndexColumnsCollationsNotScameSizeWithColumnsOnIndexCreation() {
        assertThat(
                manager.createIndex(createSortedIndexParams(INDEX_NAME, List.of("key", "val"), List.of(ASC_NULLS_FIRST))),
                willThrowFast(CatalogValidationException.class, "Columns collations doesn't match number of columns")
        );

        assertThat(
                manager.createIndex(createSortedIndexParams(INDEX_NAME, List.of("key"), List.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST))),
                willThrowFast(CatalogValidationException.class, "Columns collations doesn't match number of columns")
        );
    }

    @Test
    void testValidateIndexNameOnIndexDrop() {
        assertThat(
                manager.dropIndex(DropIndexParams.builder().schemaName(DEFAULT_SCHEMA_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Missing index name")
        );
    }

    @Test
    void testValidateTableNameOnDropColumn() {
        assertThat(
                manager.dropColumn(AlterTableDropColumnParams.builder().build()),
                willThrowFast(CatalogValidationException.class, "Name of the table can't be null or blank")
        );
    }

    @Test
    void testValidateColumnsOnDropColumn() {
        assertThat(
                manager.dropColumn(AlterTableDropColumnParams.builder().tableName(TABLE_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Columns not specified")
        );

        assertThat(
                manager.dropColumn(AlterTableDropColumnParams.builder().tableName(TABLE_NAME).columns(Set.of()).build()),
                willThrowFast(CatalogValidationException.class, "Columns not specified")
        );
    }

    @Test
    void testValidateTableNameOnAddColumn() {
        assertThat(
                manager.addColumn(AlterTableAddColumnParams.builder().build()),
                willThrowFast(CatalogValidationException.class, "Name of the table can't be null or blank")
        );
    }

    @Test
    void testValidateColumnsOnAddColumn() {
        assertThat(
                manager.addColumn(AlterTableAddColumnParams.builder().tableName(TABLE_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Columns not specified")
        );

        assertThat(
                manager.addColumn(AlterTableAddColumnParams.builder().tableName(TABLE_NAME).columns(List.of()).build()),
                willThrowFast(CatalogValidationException.class, "Columns not specified")
        );

        assertThat(
                manager.addColumn(addColumnParams(ColumnParams.builder().build())),
                willThrowFast(CatalogValidationException.class, "Name of the column can't be null or blank")
        );

        assertThat(
                manager.addColumn(addColumnParams(ColumnParams.builder().name("key").build())),
                willThrowFast(CatalogValidationException.class, "Missing column type: key")
        );

        assertThat(
                manager.addColumn(addColumnParams(columnParams("key", INT32), columnParams("key", INT64))),
                willThrowFast(CatalogValidationException.class, "Duplicate columns are present: [key]")
        );
    }

    @Test
    void testValidateTableNameOnAlterColumn() {
        assertThat(
                manager.alterColumn(AlterColumnParams.builder().build()),
                willThrowFast(CatalogValidationException.class, "Name of the table can't be null or blank")
        );
    }

    @Test
    void testValidateColumnNameOnAlterColumn() {
        assertThat(
                manager.alterColumn(AlterColumnParams.builder().tableName(TABLE_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Missing column name")
        );
    }

    private static CreateZoneParams.Builder createZoneBuilder(String zoneName) {
        return CreateZoneParams.builder().zoneName(zoneName);
    }

    private static AlterZoneParams.Builder alterZoneBuilder(String zoneName) {
        return AlterZoneParams.builder().zoneName(zoneName);
    }

    private static CreateZoneParams createZoneParams(@Nullable Integer autoAdjust, @Nullable Integer scaleUp, @Nullable Integer scaleDown) {
        return createZoneParams(ZONE_NAME, autoAdjust, scaleUp, scaleDown);
    }

    private static CreateZoneParams createZoneParams(
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

    private static AlterZoneParams alterZoneParams(@Nullable Integer autoAdjust, @Nullable Integer scaleUp, @Nullable Integer scaleDown) {
        return alterZoneBuilder(ZONE_NAME)
                .dataNodesAutoAdjust(autoAdjust)
                .dataNodesAutoAdjustScaleUp(scaleUp)
                .dataNodesAutoAdjustScaleDown(scaleDown)
                .build();
    }
}
