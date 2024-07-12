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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createCatalogManagerWithTestUpdateLog;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.clusterWideEnsuredActivationTimestamp;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceIndex;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceTable;
import static org.apache.ignite.internal.hlc.TestClockService.TEST_MAX_CLOCK_SKEW_MILLIS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link CatalogUtils} testing. */
public class CatalogUtilsTest extends BaseIgniteAbstractTest {
    private static final String TABLE_NAME = "test_table";

    private static final String COLUMN_NAME = "key";

    private final HybridClock clock = new HybridClockImpl();

    private CatalogManager catalogManager;

    @BeforeEach
    void setUp() {
        catalogManager = createCatalogManagerWithTestUpdateLog("test", clock);

        assertThat(catalogManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        assertThat(catalogManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void testReplaceTable() {
        createTable("foo");
        createTable("bar");

        CatalogSchemaDescriptor schema = catalogManager.activeSchema(SqlCommon.DEFAULT_SCHEMA_NAME, clock.nowLong());

        assertThat(schema, is(notNullValue()));

        CatalogTableDescriptor fooTable = catalogManager.table("foo", clock.nowLong());

        assertThat(fooTable, is(notNullValue()));

        CatalogTableDescriptor bazTable = fooTable.newDescriptor(
                "baz",
                fooTable.tableVersion(),
                fooTable.columns(),
                fooTable.updateToken(),
                fooTable.storageProfile()
        );

        CatalogSchemaDescriptor updatedSchema = replaceTable(schema, bazTable);

        List<String> tableNames = Arrays.stream(updatedSchema.tables()).map(CatalogTableDescriptor::name).collect(toList());

        assertThat(tableNames, contains("baz", "bar"));
    }

    @Test
    void testReplaceTableMissingTable() {
        CatalogSchemaDescriptor schema = catalogManager.activeSchema(SqlCommon.DEFAULT_SCHEMA_NAME, clock.nowLong());

        assertThat(schema, is(notNullValue()));

        var table = mock(CatalogTableDescriptor.class);

        when(table.id()).thenReturn(Integer.MAX_VALUE);

        Exception e = assertThrows(CatalogValidationException.class, () -> replaceTable(schema, table));

        assertThat(e.getMessage(), is(String.format("Table with ID %d has not been found in schema with ID %d", table.id(), 0)));
    }

    @Test
    void testReplaceIndex() {
        String tableName = "table";

        createTable(tableName);
        createIndex(tableName, "foo");
        createIndex(tableName, "bar");

        CatalogSchemaDescriptor schema = catalogManager.activeSchema(SqlCommon.DEFAULT_SCHEMA_NAME, clock.nowLong());

        assertThat(schema, is(notNullValue()));

        var fooIndex = (CatalogHashIndexDescriptor) catalogManager.aliveIndex("foo", clock.nowLong());

        assertThat(fooIndex, is(notNullValue()));

        CatalogIndexDescriptor bazIndex = new CatalogHashIndexDescriptor(
                fooIndex.id(),
                "baz",
                fooIndex.tableId(),
                fooIndex.unique(),
                fooIndex.status(),
                fooIndex.columns()
        );

        CatalogSchemaDescriptor updatedSchema = replaceIndex(schema, bazIndex);

        List<String> indexNames = Arrays.stream(updatedSchema.indexes()).map(CatalogIndexDescriptor::name).collect(toList());

        assertThat(indexNames, contains(tableName + "_PK", "baz", "bar"));
    }

    @Test
    void testReplaceIndexMissingIndex() {
        CatalogSchemaDescriptor schema = catalogManager.activeSchema(SqlCommon.DEFAULT_SCHEMA_NAME, clock.nowLong());

        assertThat(schema, is(notNullValue()));

        var index = mock(CatalogIndexDescriptor.class);

        when(index.id()).thenReturn(Integer.MAX_VALUE);

        Exception e = assertThrows(CatalogValidationException.class, () -> replaceIndex(schema, index));

        assertThat(e.getMessage(), is(String.format("Index with ID %d has not been found in schema with ID %d", index.id(), 1)));
    }

    @Test
    void testClusterWideEnsuredActivationTimestamp() {
        createTable(TABLE_NAME);

        Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        HybridTimestamp expClusterWideActivationTs = HybridTimestamp.hybridTimestamp(catalog.time())
                .addPhysicalTime(TEST_MAX_CLOCK_SKEW_MILLIS)
                .roundUpToPhysicalTick();

        assertEquals(expClusterWideActivationTs, clusterWideEnsuredActivationTimestamp(catalog, TEST_MAX_CLOCK_SKEW_MILLIS));
    }

    private void createTable(String tableName) {
        CatalogCommand catalogCommand = CreateTableCommand.builder()
                .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(ColumnParams.builder().name(COLUMN_NAME).type(INT32).build()))
                // Any type of a primary key index can be used.
                .primaryKey(TableHashPrimaryKey.builder()
                        .columns(List.of(COLUMN_NAME))
                        .build()
                )
                .colocationColumns(List.of(COLUMN_NAME))
                .build();

        assertThat(catalogManager.execute(catalogCommand), willCompleteSuccessfully());
    }

    private int createIndex(String tableName, String indexName) {
        CatalogCommand catalogCommand = CreateHashIndexCommand.builder()
                .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .tableName(tableName)
                .indexName(indexName)
                .columns(List.of(COLUMN_NAME))
                .unique(false)
                .build();

        assertThat(catalogManager.execute(catalogCommand), willCompleteSuccessfully());

        return catalogManager.aliveIndex(indexName, clock.nowLong()).id();
    }
}
