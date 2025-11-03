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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.UNSPECIFIED_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.UNSPECIFIED_PRECISION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.UNSPECIFIED_SCALE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.clusterWideEnsuredActivationTimestamp;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceIndex;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceTable;
import static org.apache.ignite.internal.hlc.TestClockService.TEST_MAX_CLOCK_SKEW_MILLIS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.BYTE_ARRAY;
import static org.apache.ignite.sql.ColumnType.DATETIME;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.DURATION;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.PERIOD;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.apache.ignite.sql.ColumnType.TIME;
import static org.apache.ignite.sql.ColumnType.TIMESTAMP;
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
import java.util.stream.Stream;
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
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** For {@link CatalogUtils} testing. */
public class CatalogUtilsTest extends BaseIgniteAbstractTest {
    private static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

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

        Catalog catalog = catalogManager.activeCatalog(clock.nowLong());
        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);

        assertThat(schema, is(notNullValue()));

        CatalogTableDescriptor fooTable = schema.table("foo");

        assertThat(fooTable, is(notNullValue()));

        CatalogTableDescriptor bazTable = fooTable.copyBuilder()
                .name("baz")
                .newColumns(fooTable.columns())
                .build();

        CatalogSchemaDescriptor updatedSchema = replaceTable(schema, bazTable);

        List<String> tableNames = Arrays.stream(updatedSchema.tables()).map(CatalogTableDescriptor::name).collect(toList());

        assertThat(tableNames, contains("baz", "bar"));
    }

    @Test
    void testReplaceTableMissingTable() {
        Catalog catalog = catalogManager.activeCatalog(clock.nowLong());
        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);

        assertThat(schema, is(notNullValue()));

        var table = mock(CatalogTableDescriptor.class);

        when(table.id()).thenReturn(Integer.MAX_VALUE);

        Exception e = assertThrows(CatalogValidationException.class, () -> replaceTable(schema, table));

        assertThat(e.getMessage(), is(String.format("Table with ID %d has not been found in schema with ID %d.", table.id(), 0)));
    }

    @Test
    void testReplaceIndex() {
        String tableName = "table";

        createTable(tableName);
        createIndex(tableName, "foo");
        createIndex(tableName, "bar");

        Catalog catalog = catalogManager.activeCatalog(clock.nowLong());
        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);

        assertThat(schema, is(notNullValue()));

        var fooIndex = (CatalogHashIndexDescriptor) schema.aliveIndex("foo");

        assertThat(fooIndex, is(notNullValue()));

        CatalogIndexDescriptor bazIndex = new CatalogHashIndexDescriptor(
                fooIndex.id(),
                "baz",
                fooIndex.tableId(),
                fooIndex.unique(),
                fooIndex.status(),
                fooIndex.columnIds(),
                fooIndex.isCreatedWithTable()
        );

        CatalogSchemaDescriptor updatedSchema = replaceIndex(schema, bazIndex);

        List<String> indexNames = Arrays.stream(updatedSchema.indexes()).map(CatalogIndexDescriptor::name).collect(toList());

        assertThat(indexNames, contains(tableName + "_PK", "baz", "bar"));
    }

    @Test
    void testReplaceIndexMissingIndex() {
        Catalog catalog = catalogManager.activeCatalog(clock.nowLong());
        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);

        assertThat(schema, is(notNullValue()));

        var index = mock(CatalogIndexDescriptor.class);

        when(index.id()).thenReturn(Integer.MAX_VALUE);

        Exception e = assertThrows(CatalogValidationException.class, () -> replaceIndex(schema, index));

        assertThat(e.getMessage(), is(String.format("Index with ID %d has not been found in schema with ID %d.", index.id(), 0)));
    }

    @Test
    void testClusterWideEnsuredActivationTimestamp() {
        createTable(TABLE_NAME);

        Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        HybridTimestamp expClusterWideActivationTs = HybridTimestamp.hybridTimestamp(catalog.time())
                .addPhysicalTime(TEST_MAX_CLOCK_SKEW_MILLIS)
                .roundUpToPhysicalTick();

        assertEquals(expClusterWideActivationTs, clusterWideEnsuredActivationTimestamp(catalog.time(), TEST_MAX_CLOCK_SKEW_MILLIS));
    }

    @Test
    void testUnspecifiedConstants() {
        assertEquals(-1, UNSPECIFIED_PRECISION, "unspecified precision");
        assertEquals(-1, UNSPECIFIED_LENGTH, "unspecified length");
        assertEquals(-1, UNSPECIFIED_LENGTH, "unspecified scale");
    }

    @ParameterizedTest
    @MethodSource("columnTypesPrecision")
    void testGetPrecision(ColumnType columnType, int min, int max) {
        assertEquals(CatalogUtils.getMinPrecision(columnType), min, "min");
        assertEquals(CatalogUtils.getMaxPrecision(columnType), max, "max");
    }

    private static Stream<Arguments> columnTypesPrecision() {
        Stream<Arguments> types = Stream.of(
                Arguments.of(DECIMAL, 1, 32767),
                Arguments.of(TIME, 0, 9),
                Arguments.of(TIMESTAMP, 0, 9),
                Arguments.of(DATETIME, 0, 9),
                Arguments.of(DURATION, 1, 10),
                Arguments.of(PERIOD, 1, 10)
        );

        Stream<Arguments> otherTypes = Arrays.stream(ColumnType.values())
                .filter(t -> !t.precisionAllowed())
                .map(t -> Arguments.of(t, UNSPECIFIED_PRECISION, UNSPECIFIED_PRECISION));

        return Stream.concat(types, otherTypes);
    }

    @ParameterizedTest
    @MethodSource("columnTypesScale")
    void testGetScale(ColumnType columnType, int min, int max) {
        assertEquals(CatalogUtils.getMinScale(columnType), min, "min");
        assertEquals(CatalogUtils.getMaxScale(columnType), max, "max");
    }

    private static Stream<Arguments> columnTypesScale() {
        Stream<Arguments> types = Stream.of(
                Arguments.of(DECIMAL, 0, 32767)
        );

        Stream<Arguments> otherTypes = Arrays.stream(ColumnType.values())
                .filter(t -> !t.scaleAllowed())
                .map(t -> Arguments.of(t, UNSPECIFIED_SCALE, UNSPECIFIED_SCALE));

        return Stream.concat(types, otherTypes);
    }

    @ParameterizedTest
    @MethodSource("columnTypesLength")
    void testGetLength(ColumnType columnType, int min, int max) {
        assertEquals(CatalogUtils.getMinLength(columnType), min, "min");
        assertEquals(CatalogUtils.getMaxLength(columnType), max, "max");
    }

    private static Stream<Arguments> columnTypesLength() {
        Stream<Arguments> types = Stream.of(
                Arguments.of(STRING, 1, Integer.MAX_VALUE),
                Arguments.of(BYTE_ARRAY, 1, Integer.MAX_VALUE)
        );

        Stream<Arguments> otherTypes = Arrays.stream(ColumnType.values())
                .filter(t -> !t.lengthAllowed())
                .map(t -> Arguments.of(t, UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH));

        return Stream.concat(types, otherTypes);
    }

    private void createTable(String tableName) {
        CatalogCommand catalogCommand = CreateTableCommand.builder()
                .schemaName(SCHEMA_NAME)
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
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .indexName(indexName)
                .columns(List.of(COLUMN_NAME))
                .unique(false)
                .build();

        assertThat(catalogManager.execute(catalogCommand), willCompleteSuccessfully());

        Catalog catalog = catalogManager.activeCatalog(clock.nowLong());
        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);
        return schema.aliveIndex(indexName).id();
    }
}
