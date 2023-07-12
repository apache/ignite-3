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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
import static org.mockito.Mockito.spy;
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
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.DistributionZoneAlreadyExistsException;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.ArgumentCaptor;

/**
 * Catalog service self test.
 */
public class CatalogServiceSelfTest {
    private static final String SCHEMA_NAME = DEFAULT_SCHEMA_NAME;
    private static final String ZONE_NAME = DEFAULT_ZONE_NAME;
    private static final String TABLE_NAME = "myTable";
    private static final String TABLE_NAME_2 = "myTable2";
    private static final String NEW_COLUMN_NAME = "NEWCOL";
    private static final String NEW_COLUMN_NAME_2 = "NEWCOL2";
    private static final String INDEX_NAME = "myIndex";

    private MetaStorageManager metastore;

    private VaultManager vault;

    private UpdateLog updateLog;

    private CatalogServiceImpl service;

    private HybridClock clock;

    private ClockWaiter clockWaiter;

    @BeforeEach
    void setUp() {
        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(vault, new SimpleInMemoryKeyValueStorage("test"));

        clock = new HybridClockImpl();
        clockWaiter = spy(new ClockWaiter("test", clock));
        updateLog = spy(new UpdateLogImpl(metastore));
        service = new CatalogServiceImpl(updateLog, clockWaiter);

        vault.start();
        metastore.start();
        clockWaiter.start();
        service.start();

        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());
    }

    @AfterEach
    public void tearDown() throws Exception {
        service.stop();
        clockWaiter.stop();
        metastore.stop();
        vault.stop();
    }

    @Test
    public void testEmptyCatalog() {
        CatalogSchemaDescriptor schema = service.schema(DEFAULT_SCHEMA_NAME, 0);

        assertNotNull(schema);
        assertSame(schema, service.activeSchema(DEFAULT_SCHEMA_NAME, clock.nowLong()));
        assertSame(schema, service.schema(0));
        assertSame(schema, service.activeSchema(clock.nowLong()));

        assertNull(service.schema(1));
        assertThrows(IllegalStateException.class, () -> service.activeSchema(-1L));

        // Validate default schema.
        assertEquals(DEFAULT_SCHEMA_NAME, schema.name());
        assertEquals(0, schema.id());
        assertEquals(0, schema.tables().length);
        assertEquals(0, schema.indexes().length);

        // Default distribution zone must exists.
        CatalogZoneDescriptor zone = service.zone(DEFAULT_ZONE_NAME, clock.nowLong());
        assertEquals(DEFAULT_ZONE_NAME, zone.name());
        assertEquals(CreateZoneParams.DEFAULT_PARTITION_COUNT, zone.partitions());
        assertEquals(CreateZoneParams.DEFAULT_REPLICA_COUNT, zone.replicas());
        assertEquals(CreateZoneParams.DEFAULT_FILTER, zone.filter());
        assertEquals(CreateZoneParams.INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(CreateZoneParams.INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(CreateZoneParams.INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
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

        assertThat(service.createTable(params), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = service.schema(0);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, service.activeSchema(0L));
        assertSame(schema, service.activeSchema(123L));

        assertNull(schema.table(TABLE_NAME));
        assertNull(service.table(TABLE_NAME, 123L));
        assertNull(service.index(createPkIndexName(TABLE_NAME), 123L));

        // Validate actual catalog
        schema = service.schema(SCHEMA_NAME, 1);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        CatalogHashIndexDescriptor pkIndex = (CatalogHashIndexDescriptor) schema.index(createPkIndexName(TABLE_NAME));

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, service.activeSchema(clock.nowLong()));

        assertSame(table, service.table(TABLE_NAME, clock.nowLong()));
        assertSame(table, service.table(table.id(), clock.nowLong()));

        assertSame(pkIndex, service.index(createPkIndexName(TABLE_NAME), clock.nowLong()));
        assertSame(pkIndex, service.index(pkIndex.id(), clock.nowLong()));

        // Validate newly created table
        assertEquals(TABLE_NAME, table.name());
        assertEquals(service.zone(ZONE_NAME, clock.nowLong()).id(), table.zoneId());

        // Validate newly created pk index
        assertEquals(createPkIndexName(TABLE_NAME), pkIndex.name());
        assertEquals(table.id(), pkIndex.tableId());
        assertEquals(table.primaryKeyColumns(), pkIndex.columns());
        assertTrue(pkIndex.unique());

        // Validate another table creation.
        assertThat(service.createTable(simpleTable(TABLE_NAME_2)), willBe(nullValue()));

        // Validate actual catalog has both tables.
        schema = service.schema(2);
        table = schema.table(TABLE_NAME);
        pkIndex = (CatalogHashIndexDescriptor) schema.index(createPkIndexName(TABLE_NAME));
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);
        CatalogHashIndexDescriptor pkIndex2 = (CatalogHashIndexDescriptor) schema.index(createPkIndexName(TABLE_NAME_2));

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, service.activeSchema(clock.nowLong()));

        assertSame(table, service.table(TABLE_NAME, clock.nowLong()));
        assertSame(table, service.table(table.id(), clock.nowLong()));

        assertSame(pkIndex, service.index(createPkIndexName(TABLE_NAME), clock.nowLong()));
        assertSame(pkIndex, service.index(pkIndex.id(), clock.nowLong()));

        assertSame(table2, service.table(TABLE_NAME_2, clock.nowLong()));
        assertSame(table2, service.table(table2.id(), clock.nowLong()));

        assertSame(pkIndex2, service.index(createPkIndexName(TABLE_NAME_2), clock.nowLong()));
        assertSame(pkIndex2, service.index(pkIndex2.id(), clock.nowLong()));

        assertNotSame(table, table2);
        assertNotSame(pkIndex, pkIndex2);

        // Try to create another table with same name.
        assertThat(service.createTable(simpleTable(TABLE_NAME_2)), willThrowFast(TableAlreadyExistsException.class));

        // Validate schema wasn't changed.
        assertSame(schema, service.activeSchema(clock.nowLong()));
    }

    @Test
    public void testCreateTableWithDuplicateColumns() {
        CreateTableParams params = CreateTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .zone(ZONE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name("key").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("val").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("val").type(ColumnType.INT32).nullable(true).build()
                ))
                .primaryKeyColumns(List.of("key"))
                .build();

        assertThat(service.createTable(params), willThrowFast(IgniteInternalException.class,
                "Can't create table with duplicate columns: key, val, val"));
    }

    @Test
    public void testDropTable() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(service.createTable(simpleTable(TABLE_NAME_2)), willBe(nullValue()));

        long beforeDropTimestamp = clock.nowLong();

        DropTableParams dropTableParams = DropTableParams.builder().schemaName(SCHEMA_NAME).tableName(TABLE_NAME).build();

        assertThat(service.dropTable(dropTableParams), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = service.schema(2);
        CatalogTableDescriptor table1 = schema.table(TABLE_NAME);
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);
        CatalogIndexDescriptor pkIndex1 = schema.index(createPkIndexName(TABLE_NAME));
        CatalogIndexDescriptor pkIndex2 = schema.index(createPkIndexName(TABLE_NAME_2));

        assertNotEquals(table1.id(), table2.id());
        assertNotEquals(pkIndex1.id(), pkIndex2.id());

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, service.activeSchema(beforeDropTimestamp));

        assertSame(table1, service.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(table1, service.table(table1.id(), beforeDropTimestamp));

        assertSame(pkIndex1, service.index(createPkIndexName(TABLE_NAME), beforeDropTimestamp));
        assertSame(pkIndex1, service.index(pkIndex1.id(), beforeDropTimestamp));

        assertSame(table2, service.table(TABLE_NAME_2, beforeDropTimestamp));
        assertSame(table2, service.table(table2.id(), beforeDropTimestamp));

        assertSame(pkIndex2, service.index(createPkIndexName(TABLE_NAME_2), beforeDropTimestamp));
        assertSame(pkIndex2, service.index(pkIndex2.id(), beforeDropTimestamp));

        // Validate actual catalog
        schema = service.schema(3);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, service.activeSchema(clock.nowLong()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(service.table(TABLE_NAME, clock.nowLong()));
        assertNull(service.table(table1.id(), clock.nowLong()));

        assertNull(schema.index(createPkIndexName(TABLE_NAME)));
        assertNull(service.index(createPkIndexName(TABLE_NAME), clock.nowLong()));
        assertNull(service.index(pkIndex1.id(), clock.nowLong()));

        assertSame(table2, service.table(TABLE_NAME_2, clock.nowLong()));
        assertSame(table2, service.table(table2.id(), clock.nowLong()));

        assertSame(pkIndex2, service.index(createPkIndexName(TABLE_NAME_2), clock.nowLong()));
        assertSame(pkIndex2, service.index(pkIndex2.id(), clock.nowLong()));

        // Try to drop table once again.
        assertThat(service.dropTable(dropTableParams), willThrowFast(TableNotFoundException.class));

        // Validate schema wasn't changed.
        assertSame(schema, service.activeSchema(clock.nowLong()));
    }

    @Test
    public void testAddColumn() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

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

        assertThat(service.addColumn(params), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = service.activeSchema(beforeAddedTimestamp);
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));

        // Validate actual catalog
        schema = service.activeSchema(clock.nowLong());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        // Validate column descriptor.
        CatalogTableColumnDescriptor column = schema.table(TABLE_NAME).column(NEW_COLUMN_NAME);

        assertEquals(NEW_COLUMN_NAME, column.name());
        assertEquals(ColumnType.STRING, column.type());
        assertTrue(column.nullable());

        assertEquals(DefaultValue.Type.CONSTANT, column.defaultValue().type());
        assertEquals("Ignite!", ((DefaultValue.ConstantValue) column.defaultValue()).value());

        assertEquals(0, column.length());
        assertEquals(0, column.precision());
        assertEquals(0, column.scale());
    }

    @Test
    public void testDropColumn() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        // Validate dropping column
        AlterTableDropColumnParams params = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"))
                .build();

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(service.dropColumn(params), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = service.activeSchema(beforeAddedTimestamp);
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNotNull(schema.table(TABLE_NAME).column("VAL"));

        // Validate actual catalog
        schema = service.activeSchema(clock.nowLong());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNull(schema.table(TABLE_NAME).column("VAL"));
    }

    @Test
    public void testCreateDropColumnIfTableNotExists() {
        assertNull(service.table(TABLE_NAME, clock.nowLong()));

        // Try to add a new column.
        AlterTableAddColumnParams addColumnParams = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(ColumnParams.builder().name(NEW_COLUMN_NAME).type(ColumnType.INT32).nullable(true).build()))
                .build();

        assertThat(service.addColumn(addColumnParams), willThrow(TableNotFoundException.class));

        // Try to drop column.
        AlterTableDropColumnParams dropColumnParams = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"))
                .build();

        assertThat(service.dropColumn(dropColumnParams), willThrow(TableNotFoundException.class));
    }

    @Test
    public void testDropIndexedColumn() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(service.createIndex(simpleIndex(INDEX_NAME, TABLE_NAME)), willBe(nullValue()));

        // Try to drop indexed column
        AlterTableDropColumnParams params = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"))
                .build();

        assertThat(service.dropColumn(params), willThrow(SqlException.class));

        // Try to drop PK column
        params = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("ID"))
                .build();

        assertThat(service.dropColumn(params), willThrow(SqlException.class));

        // Validate actual catalog
        CatalogSchemaDescriptor schema = service.activeSchema(clock.nowLong());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));
        assertSame(service.schema(2), schema);

        assertNotNull(schema.table(TABLE_NAME).column("ID"));
        assertNotNull(schema.table(TABLE_NAME).column("VAL"));
    }

    @Test
    public void testAddDropMultipleColumns() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        // Add duplicate column.
        AlterTableAddColumnParams addColumnParams = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name(NEW_COLUMN_NAME).type(ColumnType.INT32).nullable(true).build(),
                        ColumnParams.builder().name("VAL").type(ColumnType.INT32).nullable(true).build()
                ))
                .build();

        assertThat(service.addColumn(addColumnParams), willThrow(ColumnAlreadyExistsException.class));

        // Validate no column added.
        CatalogSchemaDescriptor schema = service.activeSchema(clock.nowLong());

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

        assertThat(service.addColumn(addColumnParams), willBe(nullValue()));

        // Validate both columns added.
        schema = service.activeSchema(clock.nowLong());

        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));

        // Drop multiple columns.
        AlterTableDropColumnParams dropColumnParams = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of(NEW_COLUMN_NAME, NEW_COLUMN_NAME_2))
                .build();

        assertThat(service.dropColumn(dropColumnParams), willBe(nullValue()));

        // Validate both columns dropped.
        schema = service.activeSchema(clock.nowLong());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));

        // Check dropping of non-existing column
        dropColumnParams = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of(NEW_COLUMN_NAME, "VAL"))
                .build();

        assertThat(service.dropColumn(dropColumnParams), willThrow(ColumnNotFoundException.class));

        // Validate no column dropped.
        schema = service.activeSchema(clock.nowLong());

        assertNotNull(schema.table(TABLE_NAME).column("VAL"));
    }

    /**
     * Checks for possible changes to the default value of a column descriptor.
     *
     * <p>Set/drop default value allowed for any column.
     */
    @Test
    public void testAlterColumnDefault() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // NULL-> NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(null)),
                willBe(nullValue()));
        assertNull(service.schema(schemaVer + 1));

        // NULL -> 1 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(1)),
                willBe(nullValue()));
        assertNotNull(service.schema(++schemaVer));

        // 1 -> 1 : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(1)),
                willBe(nullValue()));
        assertNull(service.schema(schemaVer + 1));

        // 1 -> 2 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(2)),
                willBe(nullValue()));
        assertNotNull(service.schema(++schemaVer));

        // 2 -> NULL : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(null)),
                willBe(nullValue()));
        assertNotNull(service.schema(++schemaVer));
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
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // NULLABLE -> NULLABLE : No-op.
        // NOT NULL -> NOT NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, false, null), willBe(nullValue()));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null), willBe(nullValue()));
        assertNull(service.schema(schemaVer + 1));

        // NOT NULL -> NULlABLE : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, false, null), willBe(nullValue()));
        assertNotNull(service.schema(++schemaVer));

        // DROP NOT NULL for PK : Error.
        assertThat(changeColumn(TABLE_NAME, "ID", null, false, null),
                willThrowFast(SqlException.class, "Cannot change NOT NULL for the primary key column 'ID'."));

        // NULlABLE -> NOT NULL : Error.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, true, null),
                willThrowFast(SqlException.class, "Cannot set NOT NULL for column 'VAL'."));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null),
                willThrowFast(SqlException.class, "Cannot set NOT NULL for column 'VAL_NOT_NULL'."));

        assertNull(service.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the precision of a column descriptor.
     *
     * <ul>
     *  <li>Increasing precision is allowed for non-PK {@link ColumnType#DECIMAL} column.</li>
     *  <li>Decreasing precision is forbidden.</li>
     * </ul>
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"DECIMAL"}, mode = Mode.INCLUDE)
    public void testAlterColumnTypePrecision(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL_" + type).type(type).build();

        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // ANY-> UNDEFINED PRECISION : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willBe(nullValue()));
        assertNull(service.schema(schemaVer + 1));

        // UNDEFINED PRECISION -> 10 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), 10, null, null), null, null),
                willBe(nullValue())
        );
        assertNotNull(service.schema(++schemaVer));

        // 10 -> 11 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), 11, null, null), null, null),
                willBe(nullValue())
        );

        CatalogSchemaDescriptor schema = service.schema(++schemaVer);
        assertNotNull(schema);

        CatalogTableColumnDescriptor desc = schema.table(TABLE_NAME).column(col.name());

        assertNotSame(desc.length(), desc.precision());
        assertEquals(11, col.type() == ColumnType.DECIMAL ? desc.precision() : desc.length());

        // 11 -> 10 : Error.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), 10, null, null), null, null),
                willThrowFast(SqlException.class, "Cannot decrease precision to 10 for column '" + col.name() + "'.")
        );
        assertNull(service.schema(schemaVer + 1));
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "DECIMAL"}, mode = Mode.EXCLUDE)
    public void testAlterColumnTypeAnyPrecisionChangeIsRejected(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL").type(type).build();
        ColumnParams colWithPrecision = ColumnParams.builder().name("COL_PRECISION").type(type).precision(10).build();

        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col, colWithPrecision))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(type, 10, null, null), null, null),
                willThrowFast(SqlException.class, "Cannot change precision for column '" + col.name() + "'"));

        assertThat(changeColumn(TABLE_NAME, colWithPrecision.name(), new TestColumnTypeParams(type, 10, null, null), null, null),
                willBe(nullValue()));

        assertThat(changeColumn(TABLE_NAME, colWithPrecision.name(), new TestColumnTypeParams(type, 9, null, null), null, null),
                willThrowFast(SqlException.class, "Cannot change precision for column '" + colWithPrecision.name() + "'"));

        assertThat(changeColumn(TABLE_NAME, colWithPrecision.name(), new TestColumnTypeParams(type, 11, null, null), null, null),
                willThrowFast(SqlException.class, "Cannot change precision for column '" + colWithPrecision.name() + "'"));

        assertNull(service.schema(schemaVer + 1));
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
        ColumnParams col = ColumnParams.builder().name("COL_" + type).type(type).build();

        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // ANY-> UNDEFINED LENGTH : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willBe(nullValue()));
        assertNull(service.schema(schemaVer + 1));

        // UNDEFINED LENGTH -> 10 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 10, null), null, null),
                willBe(nullValue())
        );
        assertNotNull(service.schema(++schemaVer));

        // 10 -> 11 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 11, null), null, null),
                willBe(nullValue())
        );

        CatalogSchemaDescriptor schema = service.schema(++schemaVer);
        assertNotNull(schema);

        CatalogTableColumnDescriptor desc = schema.table(TABLE_NAME).column(col.name());

        assertNotSame(desc.length(), desc.precision());
        assertEquals(11, col.type() == ColumnType.DECIMAL ? desc.precision() : desc.length());

        // 11 -> 10 : Error.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 10, null), null, null),
                willThrowFast(SqlException.class, "Cannot decrease length to 10 for column '" + col.name() + "'.")
        );
        assertNull(service.schema(schemaVer + 1));
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "STRING", "BYTE_ARRAY"}, mode = Mode.EXCLUDE)
    public void testAlterColumnTypeAnyLengthChangeIsRejected(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL").type(type).build();
        ColumnParams colWithLength = ColumnParams.builder().name("COL_PRECISION").type(type).length(10).build();

        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col, colWithLength))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(type, null, 10, null), null, null),
                willThrowFast(SqlException.class, "Cannot change length for column '" + col.name() + "'"));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 10, null), null, null),
                willBe(nullValue()));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 9, null), null, null),
                willThrowFast(SqlException.class, "Cannot change length for column '" + colWithLength.name() + "'"));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 11, null), null, null),
                willThrowFast(SqlException.class, "Cannot change length for column '" + colWithLength.name() + "'"));

        assertNull(service.schema(schemaVer + 1));
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testAlterColumnTypeScaleIsRejected(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL_" + type).type(type).scale(3).build();
        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // ANY-> UNDEFINED SCALE : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willBe(nullValue()));
        assertNull(service.schema(schemaVer + 1));

        // 3 -> 3 : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 3), null, null),
                willBe(nullValue()));
        assertNull(service.schema(schemaVer + 1));

        // 3 -> 4 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 4), null, null),
                willThrowFast(SqlException.class, "Cannot change scale for column '" + col.name() + "'."));
        assertNull(service.schema(schemaVer + 1));

        // 3 -> 2 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 2), null, null),
                willThrowFast(SqlException.class, "Cannot change scale for column '" + col.name() + "'."));
        assertNull(service.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the type of a column descriptor.
     *
     * <p>The following transitions are allowed for non-PK columns:
     * <ul>
     *     <li>INT8 -> INT16 -> INT32 -> INT64</li>
     *     <li>FLOAT -> DOUBLE</li>
     * </ul>
     * All other transitions are forbidden.
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

        assertThat(service.createTable(createTableParams), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

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
            assertNotNull(service.schema(schemaVer));
            assertNull(service.schema(schemaVer + 1));
        }
    }

    @Test
    public void testAlterColumnTypeRejectedForPrimaryKey() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        assertThat(changeColumn(TABLE_NAME, "ID", new TestColumnTypeParams(ColumnType.INT64), null, null),
                willThrowFast(SqlException.class, "Cannot change data type for primary key column 'ID'."));
    }

    /**
     * Ensures that the compound change command {@code SET DATA TYPE BIGINT NULL DEFAULT NULL} will change the type, drop NOT NULL and the
     * default value at the same time.
     */
    @Test
    public void testAlterColumnMultipleChanges() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        Supplier<DefaultValue> dflt = () -> DefaultValue.constant(null);
        boolean notNull = false;
        TestColumnTypeParams typeParams = new TestColumnTypeParams(ColumnType.INT64);

        // Ensures that 3 different actions applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));

        CatalogSchemaDescriptor schema = service.schema(++schemaVer);
        assertNotNull(schema);

        CatalogTableColumnDescriptor desc = schema.table(TABLE_NAME).column("VAL_NOT_NULL");
        assertEquals(DefaultValue.constant(null), desc.defaultValue());
        assertTrue(desc.nullable());
        assertEquals(ColumnType.INT64, desc.type());

        // Ensures that only one of three actions applied.
        dflt = () -> DefaultValue.constant(2);
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));

        schema = service.schema(++schemaVer);
        assertNotNull(schema);
        assertEquals(DefaultValue.constant(2), schema.table(TABLE_NAME).column("VAL_NOT_NULL").defaultValue());

        // Ensures that no action will be applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));
        assertNull(service.schema(schemaVer + 1));
    }

    @Test
    public void testAlterColumnForNonExistingTableRejected() {
        assertNotNull(service.schema(0));
        assertNull(service.schema(1));

        assertThat(changeColumn(TABLE_NAME, "ID", null, null, null), willThrowFast(TableNotFoundException.class));

        assertNotNull(service.schema(0));
        assertNull(service.schema(1));
    }

    @Test
    public void testDropTableWithIndex() {
        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL"))
                .build();

        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(service.createIndex(params), willBe(nullValue()));

        long beforeDropTimestamp = clock.nowLong();

        DropTableParams dropTableParams = DropTableParams.builder().schemaName(SCHEMA_NAME).tableName(TABLE_NAME).build();

        assertThat(service.dropTable(dropTableParams), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = service.schema(2);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        CatalogIndexDescriptor index = schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, service.activeSchema(beforeDropTimestamp));

        assertSame(table, service.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(table, service.table(table.id(), beforeDropTimestamp));

        assertSame(index, service.index(INDEX_NAME, beforeDropTimestamp));
        assertSame(index, service.index(index.id(), beforeDropTimestamp));

        // Validate actual catalog
        schema = service.schema(3);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, service.activeSchema(clock.nowLong()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(service.table(TABLE_NAME, clock.nowLong()));
        assertNull(service.table(table.id(), clock.nowLong()));

        assertNull(schema.index(INDEX_NAME));
        assertNull(service.index(INDEX_NAME, clock.nowLong()));
        assertNull(service.index(index.id(), clock.nowLong()));
    }

    @Test
    public void testCreateHashIndex() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL", "ID"))
                .build();

        assertThat(service.createIndex(params), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = service.schema(1);

        assertNotNull(schema);
        assertNull(schema.index(INDEX_NAME));
        assertNull(service.index(INDEX_NAME, 123L));

        // Validate actual catalog
        schema = service.schema(2);

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, service.index(INDEX_NAME, clock.nowLong()));
        assertSame(index, service.index(index.id(), clock.nowLong()));

        // Validate newly created hash index
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals(List.of("VAL", "ID"), index.columns());
        assertFalse(index.unique());
        assertFalse(index.writeOnly());
    }

    @Test
    public void testCreateSortedIndex() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CreateSortedIndexParams params = CreateSortedIndexParams.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .unique()
                .columns(List.of("VAL", "ID"))
                .collations(List.of(CatalogColumnCollation.DESC_NULLS_FIRST, CatalogColumnCollation.ASC_NULLS_LAST))
                .build();

        assertThat(service.createIndex(params), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = service.schema(1);

        assertNotNull(schema);
        assertNull(schema.index(INDEX_NAME));
        assertNull(service.index(INDEX_NAME, 123L));
        assertNull(service.index(4, 123L));

        // Validate actual catalog
        schema = service.schema(2);

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, service.index(INDEX_NAME, clock.nowLong()));
        assertSame(index, service.index(index.id(), clock.nowLong()));

        // Validate newly created sorted index
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
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL"))
                .build();

        assertThat(service.createIndex(params), willBe(nullValue()));
        assertThat(service.createIndex(params), willThrow(IndexAlreadyExistsException.class));
    }

    @Test
    public void testCreateIndexOnDuplicateColumns() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL", "VAL"))
                .build();

        assertThat(service.createIndex(params),
                willThrow(IgniteInternalException.class, "Can't create index on duplicate columns: VAL, VAL"));
    }

    @Test
    public void operationWillBeRetriedFiniteAmountOfTimes() {
        UpdateLog updateLogMock = mock(UpdateLog.class);

        ArgumentCaptor<OnUpdateHandler> updateHandlerCapture = ArgumentCaptor.forClass(OnUpdateHandler.class);

        doNothing().when(updateLogMock).registerUpdateHandler(updateHandlerCapture.capture());

        CatalogServiceImpl service = new CatalogServiceImpl(updateLogMock, clockWaiter);
        service.start();

        when(updateLogMock.append(any())).thenAnswer(invocation -> {
            // here we emulate concurrent updates. First of all, we return a future completed with "false"
            // as if someone has concurrently appended an update. Besides, in order to unblock service and allow to
            // make another attempt, we must notify service with the same version as in current attempt.
            VersionedUpdate updateFromInvocation = invocation.getArgument(0, VersionedUpdate.class);

            VersionedUpdate update = new VersionedUpdate(
                    updateFromInvocation.version(),
                    updateFromInvocation.delayDurationMs(),
                    List.of(new ObjectIdGenUpdateEntry(1))
            );

            updateHandlerCapture.getValue().handle(update, clock.now(), 0);

            return completedFuture(false);
        });

        CompletableFuture<Void> createTableFut = service.createTable(simpleTable("T"));

        assertThat(createTableFut, willThrow(IgniteInternalException.class, "Max retry limit exceeded"));

        // retry limit is hardcoded at org.apache.ignite.internal.catalog.CatalogServiceImpl.MAX_RETRY_COUNT
        verify(updateLogMock, times(10)).append(any());
    }

    @Test
    public void catalogActivationTime() throws Exception {
        final long delayDuration = TimeUnit.DAYS.toMillis(365);

        CatalogServiceImpl service = new CatalogServiceImpl(updateLog, clockWaiter, delayDuration);

        service.start();

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

            service.createTable(params);

            verify(updateLog).append(any());
            // TODO IGNITE-19400: recheck createTable future completion guarantees

            // This waits till the new Catalog version lands in the internal structures.
            verify(clockWaiter, timeout(10_000)).waitFor(any());

            assertSame(service.schema(0), service.activeSchema(clock.nowLong()));
            assertNull(service.table(TABLE_NAME, clock.nowLong()));

            clock.update(clock.now().addPhysicalTime(delayDuration));

            assertSame(service.schema(1), service.activeSchema(clock.nowLong()));
            assertNotNull(service.table(TABLE_NAME, clock.nowLong()));
        } finally {
            service.stop();
        }
    }

    @Test
    public void catalogServiceManagesUpdateLogLifecycle() throws Exception {
        UpdateLog updateLogMock = mock(UpdateLog.class);

        CatalogServiceImpl service = new CatalogServiceImpl(updateLogMock, clockWaiter);

        service.start();

        verify(updateLogMock).start();

        service.stop();

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

        service.listen(CatalogEvent.TABLE_CREATE, eventListener);
        service.listen(CatalogEvent.TABLE_DROP, eventListener);

        assertThat(service.createTable(createTableParams), willBe(nullValue()));
        verify(eventListener).notify(any(CreateTableEventParameters.class), isNull());

        assertThat(service.dropTable(dropTableparams), willBe(nullValue()));
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

        service.listen(CatalogEvent.INDEX_CREATE, eventListener);
        service.listen(CatalogEvent.INDEX_DROP, eventListener);

        // Try to create index without table.
        assertThat(service.createIndex(createIndexParams), willThrow(TableNotFoundException.class));
        verifyNoInteractions(eventListener);

        // Create table with PK index.
        assertThat(service.createTable(createTableParams), willCompleteSuccessfully());
        verify(eventListener).notify(any(CreateIndexEventParameters.class), isNull());

        clearInvocations(eventListener);

        // Create index.
        assertThat(service.createIndex(createIndexParams), willCompleteSuccessfully());
        verify(eventListener).notify(any(CreateIndexEventParameters.class), isNull());

        clearInvocations(eventListener);

        // Drop index.
        assertThat(service.dropIndex(dropIndexParams), willBe(nullValue()));
        verify(eventListener).notify(any(DropIndexEventParameters.class), isNull());

        clearInvocations(eventListener);

        // Drop table with pk index.
        assertThat(service.dropTable(dropTableparams), willBe(nullValue()));

        // Try drop index once again.
        assertThat(service.dropIndex(dropIndexParams), willThrow(IndexNotFoundException.class));

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
                .filter("expression")
                .build();

        assertThat(service.createDistributionZone(params), willCompleteSuccessfully());

        // Validate catalog version from the past.
        assertNull(service.zone(zoneName, 0));
        assertNull(service.zone(2, 0));
        assertNull(service.zone(zoneName, 123L));
        assertNull(service.zone(2, 123L));

        // Validate actual catalog
        CatalogZoneDescriptor zone = service.zone(zoneName, clock.nowLong());

        assertNotNull(zone);
        assertSame(zone, service.zone(zone.id(), clock.nowLong()));

        // Validate newly created zone
        assertEquals(zoneName, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());
        assertEquals(73, zone.dataNodesAutoAdjust());
        assertEquals(Integer.MAX_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(Integer.MAX_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("expression", zone.filter());
    }

    @Test
    public void testDropZone() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams createZoneParams = CreateZoneParams.builder()
                .zoneName(zoneName)
                .build();

        assertThat(service.createDistributionZone(createZoneParams), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        DropZoneParams params = DropZoneParams.builder()
                .zoneName(zoneName)
                .build();

        CompletableFuture<Void> fut = service.dropDistributionZone(params);

        assertThat(fut, willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogZoneDescriptor zone = service.zone(zoneName, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());

        assertSame(zone, service.zone(zone.id(), beforeDropTimestamp));

        // Validate actual catalog
        assertNull(service.zone(zoneName, clock.nowLong()));
        assertNull(service.zone(zone.id(), clock.nowLong()));

        // Try to drop non-existing zone.
        assertThat(service.dropDistributionZone(params), willThrow(DistributionZoneNotFoundException.class));
    }

    @Test
    public void testRenameZone() throws InterruptedException {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams createParams = CreateZoneParams.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .build();

        assertThat(service.createDistributionZone(createParams), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        Thread.sleep(5);

        String newZoneName = "RenamedZone";

        RenameZoneParams renameZoneParams = RenameZoneParams.builder()
                .zoneName(zoneName)
                .newZoneName(newZoneName)
                .build();

        assertThat(service.renameDistributionZone(renameZoneParams), willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogZoneDescriptor zone = service.zone(zoneName, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());

        assertSame(zone, service.zone(zone.id(), beforeDropTimestamp));

        // Validate actual catalog
        zone = service.zone(newZoneName, clock.nowLong());

        assertNotNull(zone);
        assertNull(service.zone(zoneName, clock.nowLong()));
        assertEquals(newZoneName, zone.name());

        assertSame(zone, service.zone(zone.id(), clock.nowLong()));
    }

    @Test
    public void testDefaultZone() {
        CatalogZoneDescriptor defaultZone = service.zone(DEFAULT_ZONE_NAME, clock.nowLong());

        // Try to create zone with default zone name.
        CreateZoneParams createParams = CreateZoneParams.builder()
                .zoneName(DEFAULT_ZONE_NAME)
                .partitions(42)
                .replicas(15)
                .build();
        assertThat(service.createDistributionZone(createParams), willThrow(IgniteInternalException.class));

        // Validate default zone wasn't changed.
        assertSame(defaultZone, service.zone(DEFAULT_ZONE_NAME, clock.nowLong()));

        // Try to rename default zone.
        String newDefaultZoneName = "RenamedDefaultZone";

        RenameZoneParams renameZoneParams = RenameZoneParams.builder()
                .zoneName(DEFAULT_ZONE_NAME)
                .newZoneName(newDefaultZoneName)
                .build();
        assertThat(service.renameDistributionZone(renameZoneParams), willThrow(IgniteInternalException.class));

        // Validate default zone wasn't changed.
        assertNull(service.zone(newDefaultZoneName, clock.nowLong()));
        assertSame(defaultZone, service.zone(DEFAULT_ZONE_NAME, clock.nowLong()));

        // Try to drop default zone.
        DropZoneParams dropZoneParams = DropZoneParams.builder()
                .zoneName(DEFAULT_ZONE_NAME)
                .build();
        assertThat(service.dropDistributionZone(dropZoneParams), willThrow(IgniteInternalException.class));

        // Validate default zone wasn't changed.
        assertSame(defaultZone, service.zone(DEFAULT_ZONE_NAME, clock.nowLong()));
    }

    @Test
    public void testAlterZone() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams createParams = CreateZoneParams.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .dataNodesAutoAdjust(73)
                .filter("expression")
                .build();

        AlterZoneParams alterZoneParams = AlterZoneParams.builder()
                .zoneName(zoneName)
                .partitions(10)
                .replicas(2)
                .dataNodesAutoAdjustScaleUp(3)
                .dataNodesAutoAdjustScaleDown(4)
                .filter("newExpression")
                .build();

        assertThat(service.createDistributionZone(createParams), willCompleteSuccessfully());
        assertThat(service.alterDistributionZone(alterZoneParams), willCompleteSuccessfully());

        // Validate actual catalog
        CatalogZoneDescriptor zone = service.zone(zoneName, clock.nowLong());
        assertNotNull(zone);
        assertSame(zone, service.zone(zone.id(), clock.nowLong()));

        assertEquals(zoneName, zone.name());
        assertEquals(10, zone.partitions());
        assertEquals(2, zone.replicas());
        assertEquals(Integer.MAX_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(3, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(4, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("newExpression", zone.filter());
    }

    @Test
    public void testCreateZoneWithSameName() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams params = CreateZoneParams.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .build();

        assertThat(service.createDistributionZone(params), willCompleteSuccessfully());

        // Try to create zone with same name.
        params = CreateZoneParams.builder()
                .zoneName(zoneName)
                .partitions(8)
                .replicas(1)
                .build();

        assertThat(service.createDistributionZone(params), willThrowFast(DistributionZoneAlreadyExistsException.class));

        // Validate zone was NOT changed
        CatalogZoneDescriptor zone = service.zone(zoneName, clock.nowLong());

        assertNotNull(zone);
        assertSame(zone, service.zone(zoneName, clock.nowLong()));
        assertSame(zone, service.zone(zone.id(), clock.nowLong()));

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

        service.listen(CatalogEvent.ZONE_CREATE, eventListener);
        service.listen(CatalogEvent.ZONE_DROP, eventListener);

        CompletableFuture<Void> fut = service.createDistributionZone(createZoneParams);

        assertThat(fut, willCompleteSuccessfully());

        verify(eventListener).notify(any(CreateZoneEventParameters.class), isNull());

        fut = service.dropDistributionZone(dropZoneParams);

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

        service.listen(CatalogEvent.TABLE_ALTER, eventListener);

        // Try to add column without table.
        assertThat(service.addColumn(addColumnParams), willThrow(TableNotFoundException.class));
        verifyNoInteractions(eventListener);

        // Create table.
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        // Add column.
        assertThat(service.addColumn(addColumnParams), willBe(nullValue()));
        verify(eventListener).notify(any(AddColumnEventParameters.class), isNull());

        // Drop column.
        assertThat(service.dropColumn(dropColumnParams), willBe(nullValue()));
        verify(eventListener).notify(any(DropColumnEventParameters.class), isNull());

        // Try drop column once again.
        assertThat(service.dropColumn(dropColumnParams), willThrow(ColumnNotFoundException.class));

        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void userFutureCompletesAfterClusterWideActivationHappens() throws Exception {
        final long delayDuration = TimeUnit.DAYS.toMillis(365);

        HybridTimestamp startTs = clock.now();

        CatalogServiceImpl service = new CatalogServiceImpl(updateLog, clockWaiter, delayDuration);

        service.start();

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

            CompletableFuture<Void> future = service.createTable(params);

            assertThat(future.isDone(), is(false));

            ArgumentCaptor<HybridTimestamp> tsCaptor = ArgumentCaptor.forClass(HybridTimestamp.class);

            verify(clockWaiter, timeout(10_000)).waitFor(tsCaptor.capture());
            HybridTimestamp userWaitTs = tsCaptor.getValue();
            assertThat(
                    userWaitTs.getPhysical() - startTs.getPhysical(),
                    greaterThanOrEqualTo(delayDuration + HybridTimestamp.maxClockSkew())
            );
        } finally {
            service.stop();
        }
    }

    @Test
    void testGetCatalogEntityInCatalogEvent() {
        CompletableFuture<Void> result = new CompletableFuture<>();

        service.listen(CatalogEvent.TABLE_CREATE, (parameters, exception) -> {
            try {
                assertNotNull(service.schema(parameters.catalogVersion()));

                result.complete(null);

                return completedFuture(true);
            } catch (Throwable t) {
                result.completeExceptionally(t);

                return failedFuture(t);
            }
        });

        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(result, willCompleteSuccessfully());
    }

    @Test
    void testGetTableByIdAndCatalogVersion() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CatalogTableDescriptor table = service.table(TABLE_NAME, clock.nowLong());

        assertNull(service.table(table.id(), 0));
        assertSame(table, service.table(table.id(), 1));
    }

    @Test
    void testGetTableIdOnDropIndexEvent() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        assertThat(
                service.createIndex(
                        CreateHashIndexParams.builder()
                                .schemaName(SCHEMA_NAME)
                                .tableName(TABLE_NAME)
                                .indexName(INDEX_NAME)
                                .columns(List.of("VAL"))
                                .build()
                ),
                willBe(nullValue())
        );

        int tableId = service.table(TABLE_NAME, clock.nowLong()).id();
        int pkIndexId = service.index(createPkIndexName(TABLE_NAME), clock.nowLong()).id();
        int indexId = service.index(INDEX_NAME, clock.nowLong()).id();

        assertNotEquals(tableId, indexId);

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);

        ArgumentCaptor<DropIndexEventParameters> captor = ArgumentCaptor.forClass(DropIndexEventParameters.class);

        doReturn(completedFuture(false)).when(eventListener).notify(captor.capture(), any());

        service.listen(CatalogEvent.INDEX_DROP, eventListener);

        // Let's remove the index.
        assertThat(
                service.dropIndex(DropIndexParams.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).build()),
                willBe(nullValue())
        );

        DropIndexEventParameters eventParameters = captor.getValue();

        assertEquals(indexId, eventParameters.indexId());
        assertEquals(tableId, eventParameters.tableId());

        // Let's delete the table.
        assertThat(
                service.dropTable(DropTableParams.builder().schemaName(SCHEMA_NAME).tableName(TABLE_NAME).build()),
                willBe(nullValue())
        );

        // Let's make sure that the PK index has been deleted.
        eventParameters = captor.getValue();

        assertEquals(pkIndexId, eventParameters.indexId());
        assertEquals(tableId, eventParameters.tableId());
    }

    @Test
    void testCreateTableWithoutPkColumns() {
        assertThat(
                service.createTable(
                        CreateTableParams.builder()
                                .schemaName(SCHEMA_NAME)
                                .zone(ZONE_NAME)
                                .tableName(TABLE_NAME)
                                .columns(List.of())
                                .primaryKeyColumns(List.of())
                                .build()
                ),
                willThrowFast(IgniteInternalException.class, "Missing primary key columns")
        );
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

        return service.alterColumn(builder.build());
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
}
