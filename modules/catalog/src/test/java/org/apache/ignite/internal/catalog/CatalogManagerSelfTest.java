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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STORAGE_ENGINE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_VARLEN_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DataStorageParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.distributionzones.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.ArgumentCaptor;

/**
 * Catalog manager self test.
 */
public class CatalogManagerSelfTest extends BaseCatalogManagerTest {
    private static final String SCHEMA_NAME = DEFAULT_SCHEMA_NAME;
    private static final String ZONE_NAME = DEFAULT_ZONE_NAME;
    private static final String TABLE_NAME = "myTable";
    private static final String TABLE_NAME_2 = "myTable2";
    private static final String NEW_COLUMN_NAME = "NEWCOL";
    private static final String NEW_COLUMN_NAME_2 = "NEWCOL2";
    private static final String INDEX_NAME = "myIndex";

    @Test
    public void testEmptyCatalog() {
        CatalogSchemaDescriptor schema = manager.schema(DEFAULT_SCHEMA_NAME, 0);

        assertNotNull(schema);
        assertSame(schema, manager.activeSchema(DEFAULT_SCHEMA_NAME, clock.nowLong()));
        assertSame(schema, manager.schema(0));
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertNull(manager.schema(1));
        assertThrows(IllegalStateException.class, () -> manager.activeSchema(-1L));

        // Validate default schema.
        assertEquals(DEFAULT_SCHEMA_NAME, schema.name());
        assertEquals(0, schema.id());
        assertEquals(0, schema.tables().length);
        assertEquals(0, schema.indexes().length);

        // Default distribution zone must exists.
        CatalogZoneDescriptor zone = manager.zone(DEFAULT_ZONE_NAME, clock.nowLong());

        assertEquals(DEFAULT_ZONE_NAME, zone.name());
        assertEquals(DEFAULT_PARTITION_COUNT, zone.partitions());
        assertEquals(DEFAULT_REPLICA_COUNT, zone.replicas());
        assertEquals(DEFAULT_FILTER, zone.filter());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(IMMEDIATE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals(DEFAULT_STORAGE_ENGINE, zone.dataStorage().engine());
        assertEquals(DEFAULT_DATA_REGION, zone.dataStorage().dataRegion());
    }

    @Test
    public void testCreateTable() {
        CreateTableParams params = CreateTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .zone(ZONE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name("key1").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("key2").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("val").type(ColumnType.INT32).nullable(true).build()
                ))
                .primaryKeyColumns(List.of("key1", "key2"))
                .colocationColumns(List.of("key2"))
                .build();

        assertThat(manager.createTable(params), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(0);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(0L));
        assertSame(schema, manager.activeSchema(123L));

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, 123L));
        assertNull(manager.index(createPkIndexName(TABLE_NAME), 123L));

        // Validate actual catalog
        schema = manager.schema(SCHEMA_NAME, 1);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        CatalogHashIndexDescriptor pkIndex = (CatalogHashIndexDescriptor) schema.index(createPkIndexName(TABLE_NAME));

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertSame(table, manager.table(TABLE_NAME, clock.nowLong()));
        assertSame(table, manager.table(table.id(), clock.nowLong()));

        assertSame(pkIndex, manager.index(createPkIndexName(TABLE_NAME), clock.nowLong()));
        assertSame(pkIndex, manager.index(pkIndex.id(), clock.nowLong()));

        // Validate newly created table
        assertEquals(TABLE_NAME, table.name());
        assertEquals(manager.zone(ZONE_NAME, clock.nowLong()).id(), table.zoneId());

        // Validate newly created pk index
        assertEquals(3L, pkIndex.id());
        assertEquals(createPkIndexName(TABLE_NAME), pkIndex.name());
        assertEquals(table.id(), pkIndex.tableId());
        assertEquals(table.primaryKeyColumns(), pkIndex.columns());
        assertTrue(pkIndex.unique());

        // Validate another table creation.
        assertThat(manager.createTable(simpleTable(TABLE_NAME_2)), willBe(nullValue()));

        // Validate actual catalog has both tables.
        schema = manager.schema(2);
        table = schema.table(TABLE_NAME);
        pkIndex = (CatalogHashIndexDescriptor) schema.index(createPkIndexName(TABLE_NAME));
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);
        CatalogHashIndexDescriptor pkIndex2 = (CatalogHashIndexDescriptor) schema.index(createPkIndexName(TABLE_NAME_2));

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertSame(table, manager.table(TABLE_NAME, clock.nowLong()));
        assertSame(table, manager.table(table.id(), clock.nowLong()));

        assertSame(pkIndex, manager.index(createPkIndexName(TABLE_NAME), clock.nowLong()));
        assertSame(pkIndex, manager.index(pkIndex.id(), clock.nowLong()));

        assertSame(table2, manager.table(TABLE_NAME_2, clock.nowLong()));
        assertSame(table2, manager.table(table2.id(), clock.nowLong()));

        assertSame(pkIndex2, manager.index(createPkIndexName(TABLE_NAME_2), clock.nowLong()));
        assertSame(pkIndex2, manager.index(pkIndex2.id(), clock.nowLong()));

        assertNotSame(table, table2);
        assertNotSame(pkIndex, pkIndex2);

        // Try to create another table with same name.
        assertThat(manager.createTable(simpleTable(TABLE_NAME_2)), willThrowFast(TableAlreadyExistsException.class));

        // Validate schema wasn't changed.
        assertSame(schema, manager.activeSchema(clock.nowLong()));
    }

    @Test
    public void testDropTable() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(manager.createTable(simpleTable(TABLE_NAME_2)), willBe(nullValue()));

        long beforeDropTimestamp = clock.nowLong();

        DropTableParams dropTableParams = DropTableParams.builder().schemaName(SCHEMA_NAME).tableName(TABLE_NAME).build();

        assertThat(manager.dropTable(dropTableParams), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(2);
        CatalogTableDescriptor table1 = schema.table(TABLE_NAME);
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);
        CatalogIndexDescriptor pkIndex1 = schema.index(createPkIndexName(TABLE_NAME));
        CatalogIndexDescriptor pkIndex2 = schema.index(createPkIndexName(TABLE_NAME_2));

        assertNotEquals(table1.id(), table2.id());
        assertNotEquals(pkIndex1.id(), pkIndex2.id());

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(beforeDropTimestamp));

        assertSame(table1, manager.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(table1, manager.table(table1.id(), beforeDropTimestamp));

        assertSame(pkIndex1, manager.index(createPkIndexName(TABLE_NAME), beforeDropTimestamp));
        assertSame(pkIndex1, manager.index(pkIndex1.id(), beforeDropTimestamp));

        assertSame(table2, manager.table(TABLE_NAME_2, beforeDropTimestamp));
        assertSame(table2, manager.table(table2.id(), beforeDropTimestamp));

        assertSame(pkIndex2, manager.index(createPkIndexName(TABLE_NAME_2), beforeDropTimestamp));
        assertSame(pkIndex2, manager.index(pkIndex2.id(), beforeDropTimestamp));

        // Validate actual catalog
        schema = manager.schema(3);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, clock.nowLong()));
        assertNull(manager.table(table1.id(), clock.nowLong()));

        assertNull(schema.index(createPkIndexName(TABLE_NAME)));
        assertNull(manager.index(createPkIndexName(TABLE_NAME), clock.nowLong()));
        assertNull(manager.index(pkIndex1.id(), clock.nowLong()));

        assertSame(table2, manager.table(TABLE_NAME_2, clock.nowLong()));
        assertSame(table2, manager.table(table2.id(), clock.nowLong()));

        assertSame(pkIndex2, manager.index(createPkIndexName(TABLE_NAME_2), clock.nowLong()));
        assertSame(pkIndex2, manager.index(pkIndex2.id(), clock.nowLong()));

        // Try to drop table once again.
        assertThat(manager.dropTable(dropTableParams), willThrowFast(TableNotFoundException.class));

        // Validate schema wasn't changed.
        assertSame(schema, manager.activeSchema(clock.nowLong()));
    }

    @Test
    public void testAddColumn() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        AlterTableAddColumnParams params = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(ColumnParams.builder()
                        .name(NEW_COLUMN_NAME)
                        .type(ColumnType.STRING)
                        .nullable(true)
                        .defaultValue(DefaultValue.constant("Ignite!"))
                        .build()
                ))
                .build();

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(manager.addColumn(params), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.activeSchema(beforeAddedTimestamp);
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));

        // Validate actual catalog
        schema = manager.activeSchema(clock.nowLong());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        // Validate column descriptor.
        CatalogTableColumnDescriptor column = schema.table(TABLE_NAME).column(NEW_COLUMN_NAME);

        assertEquals(NEW_COLUMN_NAME, column.name());
        assertEquals(ColumnType.STRING, column.type());
        assertTrue(column.nullable());

        assertEquals(DefaultValue.Type.CONSTANT, column.defaultValue().type());
        assertEquals("Ignite!", ((DefaultValue.ConstantValue) column.defaultValue()).value());

        assertEquals(DEFAULT_VARLEN_LENGTH, column.length());
        assertEquals(0, column.precision());
        assertEquals(0, column.scale());
    }

    @Test
    public void testDropColumn() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        // Validate dropping column
        AlterTableDropColumnParams params = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"))
                .build();

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(manager.dropColumn(params), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.activeSchema(beforeAddedTimestamp);
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNotNull(schema.table(TABLE_NAME).column("VAL"));

        // Validate actual catalog
        schema = manager.activeSchema(clock.nowLong());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNull(schema.table(TABLE_NAME).column("VAL"));
    }

    @Test
    public void testCreateDropColumnIfTableNotExists() {
        assertNull(manager.table(TABLE_NAME, clock.nowLong()));

        // Try to add a new column.
        AlterTableAddColumnParams addColumnParams = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(ColumnParams.builder().name(NEW_COLUMN_NAME).type(ColumnType.INT32).nullable(true).build()))
                .build();

        assertThat(manager.addColumn(addColumnParams), willThrow(TableNotFoundException.class));

        // Try to drop column.
        AlterTableDropColumnParams dropColumnParams = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"))
                .build();

        assertThat(manager.dropColumn(dropColumnParams), willThrow(TableNotFoundException.class));
    }

    @Test
    public void testDropIndexedColumn() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(manager.createIndex(simpleIndex(INDEX_NAME, TABLE_NAME)), willBe(nullValue()));

        // Try to drop indexed column
        AlterTableDropColumnParams params = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"))
                .build();

        assertThat(manager.dropColumn(params), willThrow(SqlException.class,
                "Can't drop indexed column: [columnName=VAL, indexName=myIndex]"));

        // Try to drop PK column
        params = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("ID"))
                .build();

        assertThat(manager.dropColumn(params), willThrow(SqlException.class, "Can't drop primary key column: [name=ID]"));

        // Validate actual catalog
        CatalogSchemaDescriptor schema = manager.activeSchema(clock.nowLong());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));
        assertSame(manager.schema(2), schema);

        assertNotNull(schema.table(TABLE_NAME).column("ID"));
        assertNotNull(schema.table(TABLE_NAME).column("VAL"));
    }

    @Test
    public void testAddDropMultipleColumns() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        // Add duplicate column.
        AlterTableAddColumnParams addColumnParams = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name(NEW_COLUMN_NAME).type(ColumnType.INT32).nullable(true).build(),
                        ColumnParams.builder().name("VAL").type(ColumnType.INT32).nullable(true).build()
                ))
                .build();

        assertThat(manager.addColumn(addColumnParams), willThrow(ColumnAlreadyExistsException.class));

        // Validate no column added.
        CatalogSchemaDescriptor schema = manager.activeSchema(clock.nowLong());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));

        // Add multiple columns.
        addColumnParams = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name(NEW_COLUMN_NAME).type(ColumnType.INT32).nullable(true).build(),
                        ColumnParams.builder().name(NEW_COLUMN_NAME_2).type(ColumnType.INT32).nullable(true).build()
                ))
                .build();

        assertThat(manager.addColumn(addColumnParams), willBe(nullValue()));

        // Validate both columns added.
        schema = manager.activeSchema(clock.nowLong());

        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));

        // Drop multiple columns.
        AlterTableDropColumnParams dropColumnParams = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of(NEW_COLUMN_NAME, NEW_COLUMN_NAME_2))
                .build();

        assertThat(manager.dropColumn(dropColumnParams), willBe(nullValue()));

        // Validate both columns dropped.
        schema = manager.activeSchema(clock.nowLong());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));

        // Check dropping of non-existing column
        dropColumnParams = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of(NEW_COLUMN_NAME, "VAL"))
                .build();

        assertThat(manager.dropColumn(dropColumnParams), willThrow(ColumnNotFoundException.class));

        // Validate no column dropped.
        schema = manager.activeSchema(clock.nowLong());

        assertNotNull(schema.table(TABLE_NAME).column("VAL"));
    }

    /**
     * Checks for possible changes to the default value of a column descriptor.
     *
     * <p>Set/drop default value allowed for any column.
     */
    @Test
    public void testAlterColumnDefault() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // NULL-> NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(null)),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // NULL -> 1 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(1)),
                willBe(nullValue()));
        assertNotNull(manager.schema(++schemaVer));

        // 1 -> 1 : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(1)),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // 1 -> 2 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(2)),
                willBe(nullValue()));
        assertNotNull(manager.schema(++schemaVer));

        // 2 -> NULL : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(null)),
                willBe(nullValue()));
        assertNotNull(manager.schema(++schemaVer));
    }

    /**
     * Checks for possible changes of the nullable flag of a column descriptor.
     *
     * <ul>
     *  <li>{@code DROP NOT NULL} is allowed on any non-PK column.
     *  <li>{@code SET NOT NULL} is forbidden.
     * </ul>
     */
    @Test
    public void testAlterColumnNotNull() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // NULLABLE -> NULLABLE : No-op.
        // NOT NULL -> NOT NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, false, null), willBe(nullValue()));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null), willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // NOT NULL -> NULlABLE : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, false, null), willBe(nullValue()));
        assertNotNull(manager.schema(++schemaVer));

        // DROP NOT NULL for PK : PK column can't be `null`.
        assertThat(changeColumn(TABLE_NAME, "ID", null, false, null),
                willThrowFast(SqlException.class, "Cannot change NOT NULL for the primary key column 'ID'."));

        // NULlABLE -> NOT NULL : Forbidden because this change lead to incompatible schemas.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, true, null),
                willThrowFast(SqlException.class, "Cannot set NOT NULL for column 'VAL'."));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null),
                willThrowFast(SqlException.class, "Cannot set NOT NULL for column 'VAL_NOT_NULL'."));

        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the precision of a column descriptor.
     *
     * <ul>
     *  <li>Increasing precision is allowed for non-PK {@link ColumnType#DECIMAL} column.</li>
     *  <li>Decreasing precision is forbidden.</li>
     * </ul>
     */
    @Test
    public void testAlterColumnTypePrecision() {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL_DECIMAL").type(ColumnType.DECIMAL).precision(10).build();

        assertThat(manager.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // 10 ->11 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), 11, null, null), null, null),
                willBe(nullValue())
        );
        assertNotNull(manager.schema(++schemaVer));

        // No change.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), 11, null, null), null, null),
                willBe(nullValue())
        );
        assertNull(manager.schema(schemaVer + 1));

        // 11 -> 10 : Forbidden because this change lead to incompatible schemas.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), 10, null, null), null, null),
                willThrowFast(SqlException.class, "Cannot decrease precision to 10 for column '" + col.name() + "'.")
        );
        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Changing precision is not supported for all types other than DECIMAL.
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "DECIMAL"}, mode = Mode.EXCLUDE)
    public void testAlterColumnTypeAnyPrecisionChangeIsRejected(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL").type(type).build();
        ColumnParams colWithPrecision = ColumnParams.builder().name("COL_PRECISION").type(type).precision(3).build();

        assertThat(manager.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col, colWithPrecision))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(type, 3, null, null), null, null),
                willThrowFast(SqlException.class, "Cannot change precision for column '" + col.name() + "'"));

        assertThat(changeColumn(TABLE_NAME, colWithPrecision.name(), new TestColumnTypeParams(type, 3, null, null), null, null),
                willBe(nullValue()));

        assertThat(changeColumn(TABLE_NAME, colWithPrecision.name(), new TestColumnTypeParams(type, 2, null, null), null, null),
                willThrowFast(SqlException.class, "Cannot change precision for column '" + colWithPrecision.name() + "'"));

        assertThat(changeColumn(TABLE_NAME, colWithPrecision.name(), new TestColumnTypeParams(type, 4, null, null), null, null),
                willThrowFast(SqlException.class, "Cannot change precision for column '" + colWithPrecision.name() + "'"));

        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the length of a column descriptor.
     *
     * <ul>
     *  <li>Increasing length is allowed for non-PK {@link ColumnType#STRING} and {@link ColumnType#BYTE_ARRAY} column.</li>
     *  <li>Decreasing length is forbidden.</li>
     * </ul>
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"STRING", "BYTE_ARRAY"}, mode = Mode.INCLUDE)
    public void testAlterColumnTypeLength(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL_" + type).length(10).type(type).build();

        assertThat(manager.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // 10 -> 11 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 11, null), null, null),
                willBe(nullValue())
        );

        CatalogSchemaDescriptor schema = manager.schema(++schemaVer);
        assertNotNull(schema);

        // 11 -> 10 : Error.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 10, null), null, null),
                willThrowFast(SqlException.class, "Cannot decrease length to 10 for column '" + col.name() + "'.")
        );
        assertNull(manager.schema(schemaVer + 1));

        // 11 -> 11 : No-op.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 11, null), null, null),
                willBe(nullValue())
        );
        assertNull(manager.schema(schemaVer + 1));

        // No change.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // 11 -> 10 : failed.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 10, null), null, null),
                willThrowFast(SqlException.class)
        );
        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Changing length is forbidden for all types other than STRING and BYTE_ARRAY.
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "STRING", "BYTE_ARRAY"}, mode = Mode.EXCLUDE)
    public void testAlterColumnTypeAnyLengthChangeIsRejected(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL").type(type).build();
        ColumnParams colWithLength = ColumnParams.builder().name("COL_PRECISION").type(type).length(10).build();

        assertThat(manager.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col, colWithLength))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(type, null, 10, null), null, null),
                willThrowFast(SqlException.class, "Cannot change length for column '" + col.name() + "'"));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 10, null), null, null),
                willBe(nullValue()));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 9, null), null, null),
                willThrowFast(SqlException.class, "Cannot change length for column '" + colWithLength.name() + "'"));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 11, null), null, null),
                willThrowFast(SqlException.class, "Cannot change length for column '" + colWithLength.name() + "'"));

        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Changing scale is incompatible change, thus it's forbidden for all types.
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testAlterColumnTypeScaleIsRejected(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL_" + type).type(type).scale(3).build();
        assertThat(manager.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // ANY-> UNDEFINED SCALE : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // 3 -> 3 : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 3), null, null),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // 3 -> 4 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 4), null, null),
                willThrowFast(SqlException.class, "Cannot change scale for column '" + col.name() + "'."));
        assertNull(manager.schema(schemaVer + 1));

        // 3 -> 2 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 2), null, null),
                willThrowFast(SqlException.class, "Cannot change scale for column '" + col.name() + "'."));
        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the type of a column descriptor.
     *
     * <p>The following transitions are allowed for non-PK columns:
     * <ul>
     *     <li>INT8 -> INT16 -> INT32 -> INT64</li>
     *     <li>FLOAT -> DOUBLE</li>
     * </ul>
     * All other transitions are forbidden because they lead to incompatible schemas.
     */
    @ParameterizedTest(name = "set data type {0}")
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testAlterColumnType(ColumnType target) {
        EnumSet<ColumnType> types = EnumSet.allOf(ColumnType.class);
        types.remove(ColumnType.NULL);

        List<ColumnParams> testColumns = types.stream()
                .map(t -> ColumnParams.builder().name("COL_" + t).type(t).build())
                .collect(Collectors.toList());

        List<ColumnParams> tableColumns = new ArrayList<>(List.of(ColumnParams.builder().name("ID").type(ColumnType.INT32).build()));
        tableColumns.addAll(testColumns);

        CreateTableParams createTableParams = simpleTable(TABLE_NAME, tableColumns);

        assertThat(manager.createTable(createTableParams), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        for (ColumnParams col : testColumns) {
            TypeSafeMatcher<CompletableFuture<?>> matcher;
            boolean sameType = col.type() == target;

            if (sameType || CatalogUtils.isSupportedColumnTypeChange(col.type(), target)) {
                matcher = willBe(nullValue());
                schemaVer += sameType ? 0 : 1;
            } else {
                matcher = willThrowFast(SqlException.class,
                        "Cannot change data type for column '" + col.name() + "' [from=" + col.type() + ", to=" + target + "].");
            }

            TestColumnTypeParams tyoeParams = new TestColumnTypeParams(target);

            assertThat(col.type() + " -> " + target, changeColumn(TABLE_NAME, col.name(), tyoeParams, null, null), matcher);
            assertNotNull(manager.schema(schemaVer));
            assertNull(manager.schema(schemaVer + 1));
        }
    }

    @Test
    public void testAlterColumnTypeRejectedForPrimaryKey() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        assertThat(changeColumn(TABLE_NAME, "ID", new TestColumnTypeParams(ColumnType.INT64), null, null),
                willThrowFast(SqlException.class, "Cannot change data type for primary key column 'ID'."));
    }

    /**
     * Ensures that the compound change command {@code SET DATA TYPE BIGINT NULL DEFAULT NULL} will change the type, drop NOT NULL and the
     * default value at the same time.
     */
    @Test
    public void testAlterColumnMultipleChanges() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        Supplier<DefaultValue> dflt = () -> DefaultValue.constant(null);
        boolean notNull = false;
        TestColumnTypeParams typeParams = new TestColumnTypeParams(ColumnType.INT64);

        // Ensures that 3 different actions applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));

        CatalogSchemaDescriptor schema = manager.schema(++schemaVer);
        assertNotNull(schema);

        CatalogTableColumnDescriptor desc = schema.table(TABLE_NAME).column("VAL_NOT_NULL");
        assertEquals(DefaultValue.constant(null), desc.defaultValue());
        assertTrue(desc.nullable());
        assertEquals(ColumnType.INT64, desc.type());

        // Ensures that only one of three actions applied.
        dflt = () -> DefaultValue.constant(2);
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));

        schema = manager.schema(++schemaVer);
        assertNotNull(schema);
        assertEquals(DefaultValue.constant(2), schema.table(TABLE_NAME).column("VAL_NOT_NULL").defaultValue());

        // Ensures that no action will be applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));
    }

    @Test
    public void testAlterColumnForNonExistingTableRejected() {
        assertNotNull(manager.schema(0));
        assertNull(manager.schema(1));

        assertThat(changeColumn(TABLE_NAME, "ID", null, null, null), willThrowFast(TableNotFoundException.class));

        assertNotNull(manager.schema(0));
        assertNull(manager.schema(1));
    }

    @Test
    public void testDropTableWithIndex() {
        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL"))
                .build();

        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(manager.createIndex(params), willBe(nullValue()));

        long beforeDropTimestamp = clock.nowLong();

        DropTableParams dropTableParams = DropTableParams.builder().schemaName(SCHEMA_NAME).tableName(TABLE_NAME).build();

        assertThat(manager.dropTable(dropTableParams), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(2);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        CatalogIndexDescriptor index = schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(beforeDropTimestamp));

        assertSame(table, manager.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(table, manager.table(table.id(), beforeDropTimestamp));

        assertSame(index, manager.index(INDEX_NAME, beforeDropTimestamp));
        assertSame(index, manager.index(index.id(), beforeDropTimestamp));

        // Validate actual catalog
        schema = manager.schema(3);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, clock.nowLong()));
        assertNull(manager.table(table.id(), clock.nowLong()));

        assertNull(schema.index(INDEX_NAME));
        assertNull(manager.index(INDEX_NAME, clock.nowLong()));
        assertNull(manager.index(index.id(), clock.nowLong()));
    }

    @Test
    public void testCreateHashIndex() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL", "ID"))
                .build();

        assertThat(manager.createIndex(params), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(1);

        assertNotNull(schema);
        assertNull(schema.index(INDEX_NAME));
        assertNull(manager.index(INDEX_NAME, 123L));

        // Validate actual catalog
        schema = manager.schema(2);

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, manager.index(INDEX_NAME, clock.nowLong()));
        assertSame(index, manager.index(index.id(), clock.nowLong()));

        // Validate newly created hash index
        assertEquals(4L, index.id());
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals(List.of("VAL", "ID"), index.columns());
        assertFalse(index.unique());
        assertFalse(index.writeOnly());
    }

    @Test
    public void testCreateSortedIndex() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CreateSortedIndexParams params = CreateSortedIndexParams.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .unique()
                .columns(List.of("VAL", "ID"))
                .collations(List.of(CatalogColumnCollation.DESC_NULLS_FIRST, CatalogColumnCollation.ASC_NULLS_LAST))
                .build();

        assertThat(manager.createIndex(params), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(1);

        assertNotNull(schema);
        assertNull(schema.index(INDEX_NAME));
        assertNull(manager.index(INDEX_NAME, 123L));
        assertNull(manager.index(4, 123L));

        // Validate actual catalog
        schema = manager.schema(2);

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, manager.index(INDEX_NAME, clock.nowLong()));
        assertSame(index, manager.index(index.id(), clock.nowLong()));

        // Validate newly created sorted index
        assertEquals(4L, index.id());
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals("VAL", index.columns().get(0).name());
        assertEquals("ID", index.columns().get(1).name());
        assertEquals(CatalogColumnCollation.DESC_NULLS_FIRST, index.columns().get(0).collation());
        assertEquals(CatalogColumnCollation.ASC_NULLS_LAST, index.columns().get(1).collation());
        assertTrue(index.unique());
        assertFalse(index.writeOnly());
    }

    @Test
    public void testCreateIndexWithSameName() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL"))
                .build();

        assertThat(manager.createIndex(params), willBe(nullValue()));
        assertThat(manager.createIndex(params), willThrow(IndexAlreadyExistsException.class));
    }

    @Test
    public void testCreateIndexOnDuplicateColumns() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL", "VAL"))
                .build();

        assertThat(manager.createIndex(params),
                willThrow(IgniteInternalException.class, "Can't create index on duplicate columns: VAL, VAL"));
    }

    @Test
    public void operationWillBeRetriedFiniteAmountOfTimes() {
        UpdateLog updateLogMock = mock(UpdateLog.class);

        ArgumentCaptor<OnUpdateHandler> updateHandlerCapture = ArgumentCaptor.forClass(OnUpdateHandler.class);

        doNothing().when(updateLogMock).registerUpdateHandler(updateHandlerCapture.capture());

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLogMock, clockWaiter);
        manager.start();

        when(updateLogMock.append(any())).thenAnswer(invocation -> {
            // here we emulate concurrent updates. First of all, we return a future completed with "false"
            // as if someone has concurrently appended an update. Besides, in order to unblock manager and allow to
            // make another attempt, we must notify manager with the same version as in current attempt.
            VersionedUpdate updateFromInvocation = invocation.getArgument(0, VersionedUpdate.class);

            VersionedUpdate update = new VersionedUpdate(
                    updateFromInvocation.version(),
                    updateFromInvocation.delayDurationMs(),
                    List.of(new ObjectIdGenUpdateEntry(1))
            );

            updateHandlerCapture.getValue().handle(update, clock.now(), 0);

            return completedFuture(false);
        });

        CompletableFuture<Void> createTableFut = manager.createTable(simpleTable("T"));

        assertThat(createTableFut, willThrow(IgniteInternalException.class, "Max retry limit exceeded"));

        // retry limit is hardcoded at org.apache.ignite.internal.catalog.CatalogServiceImpl.MAX_RETRY_COUNT
        verify(updateLogMock, times(10)).append(any());
    }

    @Test
    public void catalogActivationTime() throws Exception {
        final long delayDuration = TimeUnit.DAYS.toMillis(365);

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLog, clockWaiter, delayDuration);

        manager.start();

        try {
            CreateTableParams params = CreateTableParams.builder()
                    .schemaName(SCHEMA_NAME)
                    .tableName(TABLE_NAME)
                    .columns(List.of(
                            ColumnParams.builder().name("key").type(ColumnType.INT32).build(),
                            ColumnParams.builder().name("val").type(ColumnType.INT32).nullable(true).build()
                    ))
                    .primaryKeyColumns(List.of("key"))
                    .build();

            manager.createTable(params);

            verify(updateLog).append(any());
            // TODO IGNITE-19400: recheck createTable future completion guarantees

            // This waits till the new Catalog version lands in the internal structures.
            verify(clockWaiter, timeout(10_000)).waitFor(any());

            assertSame(manager.schema(0), manager.activeSchema(clock.nowLong()));
            assertNull(manager.table(TABLE_NAME, clock.nowLong()));

            clock.update(clock.now().addPhysicalTime(delayDuration));

            assertSame(manager.schema(1), manager.activeSchema(clock.nowLong()));
            assertNotNull(manager.table(TABLE_NAME, clock.nowLong()));
        } finally {
            manager.stop();
        }
    }

    @Test
    public void catalogServiceManagesUpdateLogLifecycle() throws Exception {
        UpdateLog updateLogMock = mock(UpdateLog.class);

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLogMock, clockWaiter);

        manager.start();

        verify(updateLogMock).start();

        manager.stop();

        verify(updateLogMock).stop();
    }

    @Test
    public void testTableEvents() {
        CreateTableParams createTableParams = CreateTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .zone(ZONE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name("key1").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("key2").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("val").type(ColumnType.INT32).nullable(true).build()
                ))
                .primaryKeyColumns(List.of("key1", "key2"))
                .colocationColumns(List.of("key2"))
                .build();

        DropTableParams dropTableparams = DropTableParams.builder().tableName(TABLE_NAME).build();

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        manager.listen(CatalogEvent.TABLE_CREATE, eventListener);
        manager.listen(CatalogEvent.TABLE_DROP, eventListener);

        assertThat(manager.createTable(createTableParams), willBe(nullValue()));
        verify(eventListener).notify(any(CreateTableEventParameters.class), isNull());

        assertThat(manager.dropTable(dropTableparams), willBe(nullValue()));
        verify(eventListener).notify(any(DropTableEventParameters.class), isNull());

        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testCreateIndexEvents() {
        CreateTableParams createTableParams = CreateTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .zone(ZONE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name("key1").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("key2").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("val").type(ColumnType.INT32).nullable(true).build()
                ))
                .primaryKeyColumns(List.of("key1", "key2"))
                .colocationColumns(List.of("key2"))
                .build();

        DropTableParams dropTableparams = DropTableParams.builder().tableName(TABLE_NAME).build();

        CreateHashIndexParams createIndexParams = CreateHashIndexParams.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("key2"))
                .build();

        DropIndexParams dropIndexParams = DropIndexParams.builder().indexName(INDEX_NAME).build();

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        manager.listen(CatalogEvent.INDEX_CREATE, eventListener);
        manager.listen(CatalogEvent.INDEX_DROP, eventListener);

        // Try to create index without table.
        assertThat(manager.createIndex(createIndexParams), willThrow(TableNotFoundException.class));
        verifyNoInteractions(eventListener);

        // Create table with PK index.
        assertThat(manager.createTable(createTableParams), willCompleteSuccessfully());
        verify(eventListener).notify(any(CreateIndexEventParameters.class), isNull());

        clearInvocations(eventListener);

        // Create index.
        assertThat(manager.createIndex(createIndexParams), willCompleteSuccessfully());
        verify(eventListener).notify(any(CreateIndexEventParameters.class), isNull());

        clearInvocations(eventListener);

        // Drop index.
        assertThat(manager.dropIndex(dropIndexParams), willBe(nullValue()));
        verify(eventListener).notify(any(DropIndexEventParameters.class), isNull());

        clearInvocations(eventListener);

        // Drop table with pk index.
        assertThat(manager.dropTable(dropTableparams), willBe(nullValue()));

        // Try drop index once again.
        assertThat(manager.dropIndex(dropIndexParams), willThrow(IndexNotFoundException.class));

        verify(eventListener).notify(any(DropIndexEventParameters.class), isNull());
    }

    @Test
    public void testCreateZone() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams params = CreateZoneParams.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .dataNodesAutoAdjust(73)
                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                .filter("expression")
                .dataStorage(DataStorageParams.builder().engine("test_engine").dataRegion("test_region").build())
                .build();

        assertThat(manager.createZone(params), willCompleteSuccessfully());

        // Validate catalog version from the past.
        assertNull(manager.zone(zoneName, 0));
        assertNull(manager.zone(2, 0));
        assertNull(manager.zone(zoneName, 123L));
        assertNull(manager.zone(2, 123L));

        // Validate actual catalog
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());

        assertNotNull(zone);
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        // Validate newly created zone
        assertEquals(zoneName, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());
        assertEquals(73, zone.dataNodesAutoAdjust());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("expression", zone.filter());
        assertEquals("test_engine", zone.dataStorage().engine());
        assertEquals("test_region", zone.dataStorage().dataRegion());
    }

    @Test
    public void testDropZone() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams createZoneParams = CreateZoneParams.builder()
                .zoneName(zoneName)
                .build();

        assertThat(manager.createZone(createZoneParams), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        DropZoneParams params = DropZoneParams.builder()
                .zoneName(zoneName)
                .build();

        CompletableFuture<Void> fut = manager.dropZone(params);

        assertThat(fut, willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogZoneDescriptor zone = manager.zone(zoneName, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), beforeDropTimestamp));

        // Validate actual catalog
        assertNull(manager.zone(zoneName, clock.nowLong()));
        assertNull(manager.zone(zone.id(), clock.nowLong()));

        // Try to drop non-existing zone.
        assertThat(manager.dropZone(params), willThrow(DistributionZoneNotFoundException.class));
    }

    @Test
    public void testRenameZone() throws InterruptedException {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams createParams = CreateZoneParams.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .build();

        assertThat(manager.createZone(createParams), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        Thread.sleep(5);

        String newZoneName = "RenamedZone";

        RenameZoneParams renameZoneParams = RenameZoneParams.builder()
                .zoneName(zoneName)
                .newZoneName(newZoneName)
                .build();

        assertThat(manager.renameZone(renameZoneParams), willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogZoneDescriptor zone = manager.zone(zoneName, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), beforeDropTimestamp));

        // Validate actual catalog
        zone = manager.zone(newZoneName, clock.nowLong());

        assertNotNull(zone);
        assertNull(manager.zone(zoneName, clock.nowLong()));
        assertEquals(newZoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));
    }

    @Test
    public void testDefaultZone() {
        CatalogZoneDescriptor defaultZone = manager.zone(DEFAULT_ZONE_NAME, clock.nowLong());

        // Try to create zone with default zone name.
        CreateZoneParams createParams = CreateZoneParams.builder()
                .zoneName(DEFAULT_ZONE_NAME)
                .partitions(42)
                .replicas(15)
                .build();
        assertThat(manager.createZone(createParams), willThrow(IgniteInternalException.class));

        // Validate default zone wasn't changed.
        assertSame(defaultZone, manager.zone(DEFAULT_ZONE_NAME, clock.nowLong()));

        // Try to rename default zone.
        String newDefaultZoneName = "RenamedDefaultZone";

        RenameZoneParams renameZoneParams = RenameZoneParams.builder()
                .zoneName(DEFAULT_ZONE_NAME)
                .newZoneName(newDefaultZoneName)
                .build();
        assertThat(manager.renameZone(renameZoneParams), willThrow(IgniteInternalException.class));

        // Validate default zone wasn't changed.
        assertNull(manager.zone(newDefaultZoneName, clock.nowLong()));
        assertSame(defaultZone, manager.zone(DEFAULT_ZONE_NAME, clock.nowLong()));

        // Try to drop default zone.
        DropZoneParams dropZoneParams = DropZoneParams.builder()
                .zoneName(DEFAULT_ZONE_NAME)
                .build();
        assertThat(manager.dropZone(dropZoneParams), willThrow(IgniteInternalException.class));

        // Validate default zone wasn't changed.
        assertSame(defaultZone, manager.zone(DEFAULT_ZONE_NAME, clock.nowLong()));
    }

    @Test
    public void testAlterZone() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams createParams = CreateZoneParams.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .dataNodesAutoAdjust(73)
                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                .filter("expression")
                .build();

        AlterZoneParams alterZoneParams = AlterZoneParams.builder()
                .zoneName(zoneName)
                .partitions(10)
                .replicas(2)
                .dataNodesAutoAdjust(INFINITE_TIMER_VALUE)
                .dataNodesAutoAdjustScaleUp(3)
                .dataNodesAutoAdjustScaleDown(4)
                .filter("newExpression")
                .dataStorage(DataStorageParams.builder().engine("test_engine").dataRegion("test_region").build())
                .build();

        assertThat(manager.createZone(createParams), willCompleteSuccessfully());
        assertThat(manager.alterZone(alterZoneParams), willCompleteSuccessfully());

        // Validate actual catalog
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());
        assertNotNull(zone);
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        assertEquals(zoneName, zone.name());
        assertEquals(10, zone.partitions());
        assertEquals(2, zone.replicas());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(3, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(4, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("newExpression", zone.filter());
        assertEquals("test_engine", zone.dataStorage().engine());
        assertEquals("test_region", zone.dataStorage().dataRegion());
    }

    @Test
    public void testCreateZoneWithSameName() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams params = CreateZoneParams.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .build();

        assertThat(manager.createZone(params), willCompleteSuccessfully());

        // Try to create zone with same name.
        params = CreateZoneParams.builder()
                .zoneName(zoneName)
                .partitions(8)
                .replicas(1)
                .build();

        assertThat(manager.createZone(params), willThrowFast(DistributionZoneAlreadyExistsException.class));

        // Validate zone was NOT changed
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());

        assertNotNull(zone);
        assertSame(zone, manager.zone(zoneName, clock.nowLong()));
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        assertEquals(zoneName, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());
    }

    @Test
    public void testCreateZoneEvents() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams createZoneParams = CreateZoneParams.builder()
                .zoneName(zoneName)
                .build();

        DropZoneParams dropZoneParams = DropZoneParams.builder()
                .zoneName(zoneName)
                .build();

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        manager.listen(CatalogEvent.ZONE_CREATE, eventListener);
        manager.listen(CatalogEvent.ZONE_DROP, eventListener);

        CompletableFuture<Void> fut = manager.createZone(createZoneParams);

        assertThat(fut, willCompleteSuccessfully());

        verify(eventListener).notify(any(CreateZoneEventParameters.class), isNull());

        fut = manager.dropZone(dropZoneParams);

        assertThat(fut, willCompleteSuccessfully());

        verify(eventListener).notify(any(DropZoneEventParameters.class), isNull());
        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testColumnEvents() {
        AlterTableAddColumnParams addColumnParams = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(ColumnParams.builder()
                        .name(NEW_COLUMN_NAME)
                        .type(ColumnType.INT32)
                        .defaultValue(DefaultValue.constant(42))
                        .nullable(true)
                        .build()
                ))
                .build();

        AlterTableDropColumnParams dropColumnParams = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of(NEW_COLUMN_NAME))
                .build();

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        manager.listen(CatalogEvent.TABLE_ALTER, eventListener);

        // Try to add column without table.
        assertThat(manager.addColumn(addColumnParams), willThrow(TableNotFoundException.class));
        verifyNoInteractions(eventListener);

        // Create table.
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        // Add column.
        assertThat(manager.addColumn(addColumnParams), willBe(nullValue()));
        verify(eventListener).notify(any(AddColumnEventParameters.class), isNull());

        // Drop column.
        assertThat(manager.dropColumn(dropColumnParams), willBe(nullValue()));
        verify(eventListener).notify(any(DropColumnEventParameters.class), isNull());

        // Try drop column once again.
        assertThat(manager.dropColumn(dropColumnParams), willThrow(ColumnNotFoundException.class));

        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void userFutureCompletesAfterClusterWideActivationHappens() throws Exception {
        final long delayDuration = TimeUnit.DAYS.toMillis(365);

        HybridTimestamp startTs = clock.now();

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLog, clockWaiter, delayDuration);

        manager.start();

        try {
            CreateTableParams params = CreateTableParams.builder()
                    .schemaName(SCHEMA_NAME)
                    .tableName(TABLE_NAME)
                    .columns(List.of(
                            ColumnParams.builder().name("key").type(ColumnType.INT32).build(),
                            ColumnParams.builder().name("val").type(ColumnType.INT32).nullable(true).build()
                    ))
                    .primaryKeyColumns(List.of("key"))
                    .build();

            CompletableFuture<Void> future = manager.createTable(params);

            assertThat(future.isDone(), is(false));

            ArgumentCaptor<HybridTimestamp> tsCaptor = ArgumentCaptor.forClass(HybridTimestamp.class);

            verify(clockWaiter, timeout(10_000)).waitFor(tsCaptor.capture());
            HybridTimestamp userWaitTs = tsCaptor.getValue();
            assertThat(
                    userWaitTs.getPhysical() - startTs.getPhysical(),
                    greaterThanOrEqualTo(delayDuration + HybridTimestamp.maxClockSkew())
            );
        } finally {
            manager.stop();
        }
    }

    @Test
    void testGetCatalogEntityInCatalogEvent() {
        CompletableFuture<Void> result = new CompletableFuture<>();

        manager.listen(CatalogEvent.TABLE_CREATE, (parameters, exception) -> {
            try {
                assertNotNull(manager.schema(parameters.catalogVersion()));

                result.complete(null);

                return completedFuture(true);
            } catch (Throwable t) {
                result.completeExceptionally(t);

                return failedFuture(t);
            }
        });

        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(result, willCompleteSuccessfully());
    }

    @Test
    void testGetTableByIdAndCatalogVersion() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CatalogTableDescriptor table = manager.table(TABLE_NAME, clock.nowLong());

        assertNull(manager.table(table.id(), 0));
        assertSame(table, manager.table(table.id(), 1));
    }

    @Test
    void testGetTableIdOnDropIndexEvent() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        assertThat(
                manager.createIndex(
                        CreateHashIndexParams.builder()
                                .schemaName(SCHEMA_NAME)
                                .tableName(TABLE_NAME)
                                .indexName(INDEX_NAME)
                                .columns(List.of("VAL"))
                                .build()
                ),
                willBe(nullValue())
        );

        int tableId = manager.table(TABLE_NAME, clock.nowLong()).id();
        int pkIndexId = manager.index(createPkIndexName(TABLE_NAME), clock.nowLong()).id();
        int indexId = manager.index(INDEX_NAME, clock.nowLong()).id();

        assertNotEquals(tableId, indexId);

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);

        ArgumentCaptor<DropIndexEventParameters> captor = ArgumentCaptor.forClass(DropIndexEventParameters.class);

        doReturn(completedFuture(false)).when(eventListener).notify(captor.capture(), any());

        manager.listen(CatalogEvent.INDEX_DROP, eventListener);

        // Let's remove the index.
        assertThat(
                manager.dropIndex(DropIndexParams.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).build()),
                willBe(nullValue())
        );

        DropIndexEventParameters eventParameters = captor.getValue();

        assertEquals(indexId, eventParameters.indexId());
        assertEquals(tableId, eventParameters.tableId());

        // Let's delete the table.
        assertThat(
                manager.dropTable(DropTableParams.builder().schemaName(SCHEMA_NAME).tableName(TABLE_NAME).build()),
                willBe(nullValue())
        );

        // Let's make sure that the PK index has been deleted.
        eventParameters = captor.getValue();

        assertEquals(pkIndexId, eventParameters.indexId());
        assertEquals(tableId, eventParameters.tableId());
    }

    @Test
    void testCreateTableErrors() {
        // Table must have at least one column.
        assertThat(
                manager.createTable(
                        CreateTableParams.builder()
                                .schemaName(SCHEMA_NAME)
                                .zone(ZONE_NAME)
                                .tableName(TABLE_NAME)
                                .columns(List.of())
                                .primaryKeyColumns(List.of())
                                .build()
                ),
                willThrowFast(IgniteInternalException.class, "Table must include at least one column.")
        );

        // Table must have PK columns.
        assertThat(
                manager.createTable(
                        CreateTableParams.builder()
                                .schemaName(SCHEMA_NAME)
                                .zone(ZONE_NAME)
                                .tableName(TABLE_NAME)
                                .columns(List.of(
                                        ColumnParams.builder().name("val").type(ColumnType.INT32).build()
                                ))
                                .primaryKeyColumns(List.of())
                                .build()
                ),
                willThrowFast(IgniteInternalException.class, "Table without primary key is not supported.")
        );

        // PK column must be a valid column
        assertThat(
                manager.createTable(
                        CreateTableParams.builder()
                                .schemaName(SCHEMA_NAME)
                                .zone(ZONE_NAME)
                                .tableName(TABLE_NAME)
                                .columns(List.of(
                                        ColumnParams.builder().name("val").type(ColumnType.INT32).build()
                                ))
                                .primaryKeyColumns(List.of("key"))
                                .build()
                ),
                willThrowFast(IgniteInternalException.class, "Invalid primary key columns: val")
        );

        // Column names must be unique.
        assertThat(
                manager.createTable(
                        CreateTableParams.builder()
                                .schemaName(SCHEMA_NAME)
                                .tableName(TABLE_NAME)
                                .zone(ZONE_NAME)
                                .columns(List.of(
                                        ColumnParams.builder().name("key").type(ColumnType.INT32).build(),
                                        ColumnParams.builder().name("val").type(ColumnType.INT32).build(),
                                        ColumnParams.builder().name("val").type(ColumnType.INT32).nullable(true).build()
                                ))
                                .primaryKeyColumns(List.of("key"))
                                .build()
                ),
                willThrowFast(IgniteInternalException.class, "Can't create table with duplicate columns: key, val, val"));

        // PK column names must be unique.
        assertThat(
                manager.createTable(
                        CreateTableParams.builder()
                                .schemaName(SCHEMA_NAME)
                                .tableName(TABLE_NAME)
                                .zone(ZONE_NAME)
                                .columns(List.of(
                                        ColumnParams.builder().name("key1").type(ColumnType.INT32).build(),
                                        ColumnParams.builder().name("key2").type(ColumnType.INT32).build()
                                ))
                                .primaryKeyColumns(List.of("key1", "key2", "key1"))
                                .build()
                ),
                willThrowFast(IgniteInternalException.class, "Primary key columns contains duplicates: key1, key2, key1"));

        // Colocated columns names must be unique.
        assertThat(
                manager.createTable(
                        CreateTableParams.builder()
                                .schemaName(SCHEMA_NAME)
                                .tableName(TABLE_NAME)
                                .zone(ZONE_NAME)
                                .columns(List.of(
                                        ColumnParams.builder().name("key1").type(ColumnType.INT32).build(),
                                        ColumnParams.builder().name("key2").type(ColumnType.INT32).build(),
                                        ColumnParams.builder().name("val").type(ColumnType.INT32).build()
                                ))
                                .primaryKeyColumns(List.of("key1", "key2"))
                                .colocationColumns(List.of("key1", "key2", "key1"))
                                .build()
                ),
                willThrowFast(IgniteInternalException.class, "Colocation columns contains duplicates: key1, key2, key1"));

        // Colocated columns must be valid primary key columns.
        assertThat(
                manager.createTable(
                        CreateTableParams.builder()
                                .schemaName(SCHEMA_NAME)
                                .tableName(TABLE_NAME)
                                .zone(ZONE_NAME)
                                .columns(List.of(
                                        ColumnParams.builder().name("key").type(ColumnType.INT32).build(),
                                        ColumnParams.builder().name("val").type(ColumnType.INT32).build()
                                ))
                                .primaryKeyColumns(List.of("key"))
                                .colocationColumns(List.of("val"))
                                .build()
                ),
                willThrowFast(IgniteInternalException.class, "Colocation columns must be subset of primary key: outstandingColumns=[val]"));
    }

    @Test
    void testLatestCatalogVersion() {
        assertEquals(0, manager.latestCatalogVersion());

        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertEquals(1, manager.latestCatalogVersion());

        assertThat(manager.createIndex(simpleIndex(INDEX_NAME, TABLE_NAME)), willBe(nullValue()));
        assertEquals(2, manager.latestCatalogVersion());
    }

    @Test
    void testTables() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME + 0)), willBe(nullValue()));
        assertThat(manager.createTable(simpleTable(TABLE_NAME + 1)), willBe(nullValue()));

        assertThat(manager.tables(0), empty());
        assertThat(manager.tables(1), hasItems(table(1, TABLE_NAME + 0)));
        assertThat(manager.tables(2), hasItems(table(2, TABLE_NAME + 0), table(2, TABLE_NAME + 1)));
    }

    @Test
    void testIndexes() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(manager.createIndex(simpleIndex(INDEX_NAME, TABLE_NAME)), willBe(nullValue()));

        assertThat(manager.indexes(0), empty());
        assertThat(manager.indexes(1), hasItems(index(1, createPkIndexName(TABLE_NAME))));
        assertThat(manager.indexes(2), hasItems(index(2, createPkIndexName(TABLE_NAME)), index(2, INDEX_NAME)));
    }

    @Test
    public void createTableProducesTableVersion1() {
        createSomeTable(TABLE_NAME);

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(1));
    }

    @Test
    public void addColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        CompletableFuture<Void> future = manager.addColumn(
                AlterTableAddColumnParams.builder()
                        .schemaName(SCHEMA_NAME)
                        .tableName(TABLE_NAME)
                        .columns(List.of(ColumnParams.builder().name("val2").type(ColumnType.INT32).build()))
                        .build()
        );
        assertThat(future, willCompleteSuccessfully());

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));
    }

    @Test
    public void dropColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        CompletableFuture<Void> future = manager.dropColumn(
                AlterTableDropColumnParams.builder()
                        .schemaName(SCHEMA_NAME)
                        .tableName(TABLE_NAME)
                        .columns(Set.of("val1"))
                        .build()
        );
        assertThat(future, willCompleteSuccessfully());

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));
    }

    @Test
    public void alterColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        CompletableFuture<Void> future = manager.alterColumn(
                AlterColumnParams.builder()
                        .schemaName(SCHEMA_NAME)
                        .tableName(TABLE_NAME)
                        .columnName("val1")
                        .type(ColumnType.INT64)
                        .build()
        );
        assertThat(future, willCompleteSuccessfully());

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));
    }

    @Test
    void testCreateZoneWithDefaults() {
        assertThat(manager.createZone(CreateZoneParams.builder().zoneName(ZONE_NAME + 1).build()), willBe(nullValue()));

        CatalogZoneDescriptor zone = manager.zone(ZONE_NAME, clock.nowLong());

        assertEquals(DEFAULT_PARTITION_COUNT, zone.partitions());
        assertEquals(DEFAULT_REPLICA_COUNT, zone.replicas());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(IMMEDIATE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals(DEFAULT_FILTER, zone.filter());
        assertEquals(DEFAULT_STORAGE_ENGINE, zone.dataStorage().engine());
        assertEquals(DEFAULT_DATA_REGION, zone.dataStorage().dataRegion());
    }

    private void createSomeTable(String tableName) {
        CreateTableParams params = CreateTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .zone(ZONE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name("key1").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("val1").type(ColumnType.INT32).build()
                ))
                .primaryKeyColumns(List.of("key1"))
                .build();

        assertThat(manager.createTable(params), willCompleteSuccessfully());
    }

    private CompletableFuture<Void> changeColumn(
            String tab,
            String col,
            @Nullable TestColumnTypeParams typeParams,
            @Nullable Boolean notNull,
            @Nullable Supplier<DefaultValue> dflt
    ) {
        Builder builder = AlterColumnParams.builder()
                .tableName(tab)
                .columnName(col)
                .notNull(notNull);

        if (dflt != null) {
            builder.defaultValueResolver(ignore -> dflt.get());
        }

        if (typeParams != null) {
            builder.type(typeParams.type);

            if (typeParams.precision != null) {
                builder.precision(typeParams.precision);
            }

            if (typeParams.length != null) {
                builder.length(typeParams.length);
            }

            if (typeParams.scale != null) {
                builder.scale(typeParams.scale);
            }
        }

        return manager.alterColumn(builder.build());
    }

    private static CreateTableParams simpleTable(String name) {
        List<ColumnParams> cols = List.of(
                ColumnParams.builder().name("ID").type(ColumnType.INT32).build(),
                ColumnParams.builder().name("VAL").type(ColumnType.INT32).nullable(true).defaultValue(DefaultValue.constant(null)).build(),
                ColumnParams.builder().name("VAL_NOT_NULL").type(ColumnType.INT32).defaultValue(DefaultValue.constant(1)).build(),
                ColumnParams.builder().name("DEC").type(ColumnType.DECIMAL).nullable(true).build(),
                ColumnParams.builder().name("STR").type(ColumnType.STRING).nullable(true).build(),
                ColumnParams.builder().name("DEC_SCALE").type(ColumnType.DECIMAL).scale(3).build()
        );

        return simpleTable(name, cols);
    }

    private static CreateTableParams simpleTable(String name, List<ColumnParams> cols) {
        return CreateTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(name)
                .zone(ZONE_NAME)
                .columns(cols)
                .primaryKeyColumns(List.of(cols.get(0).name()))
                .build();
    }

    private static CreateSortedIndexParams simpleIndex(String indexName, String tableName) {
        return CreateSortedIndexParams.builder()
                .indexName(indexName)
                .tableName(tableName)
                .unique()
                .columns(List.of("VAL"))
                .collations(List.of(CatalogColumnCollation.ASC_NULLS_LAST))
                .build();
    }

    private static class TestColumnTypeParams {
        private final ColumnType type;
        private final Integer precision;
        private final Integer length;
        private final Integer scale;

        private TestColumnTypeParams(ColumnType type) {
            this(type, null, null, null);
        }

        private TestColumnTypeParams(ColumnType type, @Nullable Integer precision, @Nullable Integer length, @Nullable Integer scale) {
            this.type = type;
            this.precision = precision;
            this.length = length;
            this.scale = scale;
        }
    }

    private static String createPkIndexName(String tableName) {
        return tableName + "_PK";
    }

    private @Nullable CatalogTableDescriptor table(int catalogVersion, String tableName) {
        return manager.schema(catalogVersion).table(tableName);
    }

    private @Nullable CatalogIndexDescriptor index(int catalogVersion, String indexName) {
        return manager.schema(catalogVersion).index(indexName);
    }
}
