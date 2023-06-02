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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.descriptors.ColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.HashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * Catalog service self test.
 */
public class CatalogServiceSelfTest {
    private static final String SCHEMA_NAME = CatalogService.PUBLIC;
    private static final String ZONE_NAME = "ZONE";
    private static final String TABLE_NAME = "myTable";
    private static final String TABLE_NAME_2 = "myTable2";
    private static final String NEW_COLUMN_NAME = "NEWCOL";
    private static final String NEW_COLUMN_NAME_2 = "NEWCOL2";
    private static final String INDEX_NAME = "myIndex";

    private MetaStorageManager metastore;

    private VaultManager vault;

    private CatalogServiceImpl service;

    @BeforeEach
    void setUp() throws NodeStoppingException {
        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(
                vault, new SimpleInMemoryKeyValueStorage("test")
        );

        service = new CatalogServiceImpl(new UpdateLogImpl(metastore, vault));

        vault.start();
        metastore.start();
        service.start();

        metastore.deployWatches();
    }

    @AfterEach
    public void tearDown() throws Exception {
        service.stop();
        metastore.stop();
        vault.stop();
    }

    @Test
    public void testEmptyCatalog() {
        assertNotNull(service.activeSchema(System.currentTimeMillis()));
        assertNotNull(service.schema(0));

        assertNull(service.schema(1));
        assertThrows(IllegalStateException.class, () -> service.activeSchema(-1L));

        assertNull(service.table(0, System.currentTimeMillis()));
        assertNull(service.index(0, System.currentTimeMillis()));

        SchemaDescriptor schema = service.schema(0);
        assertEquals(SCHEMA_NAME, schema.name());

        assertEquals(0, schema.id());
        assertEquals(0, schema.version());
        assertEquals(0, schema.tables().length);
        assertEquals(0, schema.indexes().length);
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

        CompletableFuture<Void> fut = service.createTable(params);

        assertThat(fut, willBe((Object) null));

        // Validate catalog version from the past.
        SchemaDescriptor schema = service.schema(0);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(SCHEMA_NAME, schema.name());
        assertEquals(0, schema.version());
        assertSame(schema, service.activeSchema(0L));
        assertSame(schema, service.activeSchema(123L));

        assertNull(schema.table(TABLE_NAME));
        assertNull(service.table(TABLE_NAME, 123L));
        assertNull(service.table(1, 123L));

        // Validate actual catalog
        schema = service.schema(1);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(SCHEMA_NAME, schema.name());
        assertEquals(1, schema.version());
        assertSame(schema, service.activeSchema(System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME), service.table(TABLE_NAME, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME), service.table(1, System.currentTimeMillis()));

        // Validate newly created table
        TableDescriptor table = schema.table(TABLE_NAME);

        assertEquals(1L, table.id());
        assertEquals(TABLE_NAME, table.name());
        assertEquals(0L, table.engineId());
        assertEquals(0L, table.zoneId());

        // Validate another table creation.
        assertThat(service.createTable(simpleTable(TABLE_NAME_2)), willBe((Object) null));

        // Validate actual catalog has both tables.
        schema = service.schema(2);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(SCHEMA_NAME, schema.name());
        assertEquals(2, schema.version());
        assertSame(schema, service.activeSchema(System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME), service.table(TABLE_NAME, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME), service.table(1, System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME_2), service.table(TABLE_NAME_2, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME_2), service.table(2, System.currentTimeMillis()));

        assertNotSame(schema.table(TABLE_NAME), schema.table(TABLE_NAME_2));

        // Try to create another table with same name.
        assertThat(service.createTable(simpleTable(TABLE_NAME_2)), willThrowFast(TableAlreadyExistsException.class));

        // Validate schema wasn't changed.
        assertSame(schema, service.activeSchema(System.currentTimeMillis()));
    }

    @Test
    public void testDropTable() throws InterruptedException {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));
        assertThat(service.createTable(simpleTable(TABLE_NAME_2)), willBe((Object) null));

        long beforeDropTimestamp = System.currentTimeMillis();

        Thread.sleep(5);

        DropTableParams dropTableParams = DropTableParams.builder().schemaName(SCHEMA_NAME).tableName(TABLE_NAME).build();

        assertThat(service.dropTable(dropTableParams), willBe((Object) null));

        // Validate catalog version from the past.
        SchemaDescriptor schema = service.schema(2);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(SCHEMA_NAME, schema.name());
        assertEquals(2, schema.version());
        assertSame(schema, service.activeSchema(beforeDropTimestamp));

        assertSame(schema.table(TABLE_NAME), service.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(schema.table(TABLE_NAME), service.table(1, beforeDropTimestamp));

        assertSame(schema.table(TABLE_NAME_2), service.table(TABLE_NAME_2, beforeDropTimestamp));
        assertSame(schema.table(TABLE_NAME_2), service.table(2, beforeDropTimestamp));

        // Validate actual catalog
        schema = service.schema(3);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(SCHEMA_NAME, schema.name());
        assertEquals(3, schema.version());
        assertSame(schema, service.activeSchema(System.currentTimeMillis()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(service.table(TABLE_NAME, System.currentTimeMillis()));
        assertNull(service.table(1, System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME_2), service.table(TABLE_NAME_2, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME_2), service.table(2, System.currentTimeMillis()));

        // Try to drop table once again.
        assertThat(service.dropTable(dropTableParams), willThrowFast(TableNotFoundException.class));

        // Validate schema wasn't changed.
        assertSame(schema, service.activeSchema(System.currentTimeMillis()));
    }

    @Test
    public void testAddColumn() throws InterruptedException {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

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

        long beforeAddedTimestamp = System.currentTimeMillis();

        Thread.sleep(5);

        assertThat(service.addColumn(params), willBe((Object) null));

        // Validate catalog version from the past.
        SchemaDescriptor schema = service.activeSchema(beforeAddedTimestamp);
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));

        // Validate actual catalog
        schema = service.activeSchema(System.currentTimeMillis());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        // Validate column descriptor.
        TableColumnDescriptor column = schema.table(TABLE_NAME).column(NEW_COLUMN_NAME);

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
    public void testDropColumn() throws InterruptedException {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        // Validate dropping column
        AlterTableDropColumnParams params = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"))
                .build();

        long beforeAddedTimestamp = System.currentTimeMillis();

        Thread.sleep(5);

        assertThat(service.dropColumn(params), willBe((Object) null));

        // Validate catalog version from the past.
        SchemaDescriptor schema = service.activeSchema(beforeAddedTimestamp);
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNotNull(schema.table(TABLE_NAME).column("VAL"));

        // Validate actual catalog
        schema = service.activeSchema(System.currentTimeMillis());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNull(schema.table(TABLE_NAME).column("VAL"));
    }

    @Test
    public void testCreateDropColumnIfTableNotExists() {
        assertNull(service.table(TABLE_NAME, System.currentTimeMillis()));

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
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));
        assertThat(service.createIndex(simpleIndex(INDEX_NAME, TABLE_NAME)), willBe((Object) null));

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
        SchemaDescriptor schema = service.activeSchema(System.currentTimeMillis());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));
        assertEquals(2, schema.version());

        assertNotNull(schema.table(TABLE_NAME).column("ID"));
        assertNotNull(schema.table(TABLE_NAME).column("VAL"));
    }

    @Test
    public void testAddDropMultipleColumns() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

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
        SchemaDescriptor schema = service.activeSchema(System.currentTimeMillis());

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

        assertThat(service.addColumn(addColumnParams), willBe((Object) null));

        // Validate both columns added.
        schema = service.activeSchema(System.currentTimeMillis());

        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));

        // Drop multiple columns.
        AlterTableDropColumnParams dropColumnParams = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of(NEW_COLUMN_NAME, NEW_COLUMN_NAME_2))
                .build();

        assertThat(service.dropColumn(dropColumnParams), willBe((Object) null));

        // Validate both columns dropped.
        schema = service.activeSchema(System.currentTimeMillis());

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
        schema = service.activeSchema(System.currentTimeMillis());

        assertNotNull(schema.table(TABLE_NAME).column("VAL"));
    }

    @Test
    public void testDropTableWithIndex() throws InterruptedException {
        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL"))
                .build();

        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));
        assertThat(service.createIndex(params), willBe((Object) null));

        long beforeDropTimestamp = System.currentTimeMillis();

        Thread.sleep(5);

        DropTableParams dropTableParams = DropTableParams.builder().schemaName("PUBLIC").tableName(TABLE_NAME).build();

        assertThat(service.dropTable(dropTableParams), willBe((Object) null));

        // Validate catalog version from the past.
        SchemaDescriptor schema = service.schema(2);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(CatalogService.PUBLIC, schema.name());
        assertEquals(2, schema.version());
        assertSame(schema, service.activeSchema(beforeDropTimestamp));

        assertSame(schema.table(TABLE_NAME), service.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(schema.table(TABLE_NAME), service.table(1, beforeDropTimestamp));

        assertSame(schema.index(INDEX_NAME), service.index(INDEX_NAME, beforeDropTimestamp));
        assertSame(schema.index(INDEX_NAME), service.index(2, beforeDropTimestamp));

        // Validate actual catalog
        schema = service.schema(3);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(CatalogService.PUBLIC, schema.name());
        assertEquals(3, schema.version());
        assertSame(schema, service.activeSchema(System.currentTimeMillis()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(service.table(TABLE_NAME, System.currentTimeMillis()));
        assertNull(service.table(1, System.currentTimeMillis()));

        assertNull(schema.index(INDEX_NAME));
        assertNull(service.index(INDEX_NAME, System.currentTimeMillis()));
        assertNull(service.index(2, System.currentTimeMillis()));
    }

    @Test
    public void testCreateHashIndex() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL", "ID"))
                .build();

        assertThat(service.createIndex(params), willBe((Object) null));

        // Validate catalog version from the past.
        SchemaDescriptor schema = service.schema(1);

        assertNotNull(schema);
        assertNull(schema.index(INDEX_NAME));
        assertNull(service.index(INDEX_NAME, 123L));
        assertNull(service.index(2, 123L));

        // Validate actual catalog
        schema = service.schema(2);

        assertNotNull(schema);
        assertNull(service.index(1, System.currentTimeMillis()));
        assertSame(schema.index(INDEX_NAME), service.index(INDEX_NAME, System.currentTimeMillis()));
        assertSame(schema.index(INDEX_NAME), service.index(2, System.currentTimeMillis()));

        // Validate newly created hash index
        HashIndexDescriptor index = (HashIndexDescriptor) schema.index(INDEX_NAME);

        assertEquals(2L, index.id());
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals(List.of("VAL", "ID"), index.columns());
        assertFalse(index.unique());
        assertFalse(index.writeOnly());
    }

    @Test
    public void testCreateSortedIndex() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        CreateSortedIndexParams params = CreateSortedIndexParams.builder()
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .unique()
                .columns(List.of("VAL", "ID"))
                .collations(List.of(ColumnCollation.DESC_NULLS_FIRST, ColumnCollation.ASC_NULLS_LAST))
                .build();

        assertThat(service.createIndex(params), willBe((Object) null));

        // Validate catalog version from the past.
        SchemaDescriptor schema = service.schema(1);

        assertNotNull(schema);
        assertNull(schema.index(INDEX_NAME));
        assertNull(service.index(INDEX_NAME, 123L));
        assertNull(service.index(2, 123L));

        // Validate actual catalog
        schema = service.schema(2);

        assertNotNull(schema);
        assertNull(service.index(1, System.currentTimeMillis()));
        assertSame(schema.index(INDEX_NAME), service.index(INDEX_NAME, System.currentTimeMillis()));
        assertSame(schema.index(INDEX_NAME), service.index(2, System.currentTimeMillis()));

        // Validate newly created sorted index
        SortedIndexDescriptor index = (SortedIndexDescriptor) schema.index(INDEX_NAME);

        assertEquals(2L, index.id());
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals("VAL", index.columns().get(0).name());
        assertEquals("ID", index.columns().get(1).name());
        assertEquals(ColumnCollation.DESC_NULLS_FIRST, index.columns().get(0).collation());
        assertEquals(ColumnCollation.ASC_NULLS_LAST, index.columns().get(1).collation());
        assertTrue(index.unique());
        assertFalse(index.writeOnly());
    }

    @Test
    public void testCreateIndexWithSameName() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL"))
                .build();

        assertThat(service.createIndex(params), willBe((Object) null));
        assertThat(service.createIndex(params), willThrow(IndexAlreadyExistsException.class));
    }

    @Test
    public void operationWillBeRetriedFiniteAmountOfTimes() {
        UpdateLog updateLogMock = Mockito.mock(UpdateLog.class);

        ArgumentCaptor<OnUpdateHandler> updateHandlerCapture = ArgumentCaptor.forClass(OnUpdateHandler.class);

        doNothing().when(updateLogMock).registerUpdateHandler(updateHandlerCapture.capture());

        CatalogServiceImpl service = new CatalogServiceImpl(updateLogMock);
        service.start();

        when(updateLogMock.append(any())).thenAnswer(invocation -> {
            // here we emulate concurrent updates. First of all, we return a future completed with "false"
            // as if someone has concurrently appended an update. Besides, in order to unblock service and allow to
            // make another attempt, we must notify service with the same version as in current attempt.
            VersionedUpdate updateFromInvocation = invocation.getArgument(0, VersionedUpdate.class);

            VersionedUpdate update = new VersionedUpdate(
                    updateFromInvocation.version(),
                    List.of(new ObjectIdGenUpdateEntry(1))
            );

            updateHandlerCapture.getValue().handle(update);

            return completedFuture(false);
        });

        CompletableFuture<Void> createTableFut = service.createTable(simpleTable("T"));

        assertThat(createTableFut, willThrow(IgniteInternalException.class, "Max retry limit exceeded"));

        // retry limit is hardcoded at org.apache.ignite.internal.catalog.CatalogServiceImpl.MAX_RETRY_COUNT
        Mockito.verify(updateLogMock, times(10)).append(any());
    }

    @Test
    public void catalogServiceManagesUpdateLogLifecycle() throws Exception {
        UpdateLog updateLogMock = Mockito.mock(UpdateLog.class);

        CatalogServiceImpl service = new CatalogServiceImpl(updateLogMock);

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

        EventListener<CatalogEventParameters> eventListener = Mockito.mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        service.listen(CatalogEvent.TABLE_CREATE, eventListener);
        service.listen(CatalogEvent.TABLE_DROP, eventListener);

        assertThat(service.createTable(createTableParams), willBe((Object) null));
        verify(eventListener).notify(any(CreateTableEventParameters.class), ArgumentMatchers.isNull());

        assertThat(service.dropTable(dropTableparams), willBe((Object) null));
        verify(eventListener).notify(any(DropTableEventParameters.class), ArgumentMatchers.isNull());

        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testCreateIndexEvents() {
        CreateTableParams createTableParams = CreateTableParams.builder()
                .schemaName(CatalogService.PUBLIC)
                .tableName(TABLE_NAME)
                .zone("ZONE")
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
                .schemaName(CatalogService.PUBLIC)
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("key2"))
                .build();

        DropIndexParams dropIndexParams = DropIndexParams.builder().indexName(INDEX_NAME).build();

        EventListener<CatalogEventParameters> eventListener = Mockito.mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        service.listen(CatalogEvent.INDEX_CREATE, eventListener);
        service.listen(CatalogEvent.INDEX_DROP, eventListener);

        // Try to create index without table.
        assertThat(service.createIndex(createIndexParams), willThrow(TableNotFoundException.class));
        verifyNoInteractions(eventListener);

        // Create table.
        assertThat(service.createTable(createTableParams), willBe((Object) null));

        // Create index.
        assertThat(service.createIndex(createIndexParams), willBe((Object) null));
        verify(eventListener).notify(any(CreateIndexEventParameters.class), ArgumentMatchers.isNull());

        // Drop index.
        assertThat(service.dropIndex(dropIndexParams), willBe((Object) null));
        verify(eventListener).notify(any(DropIndexEventParameters.class), ArgumentMatchers.isNull());

        // Drop table.
        assertThat(service.dropTable(dropTableparams), willBe((Object) null));

        // Try drop index once again.
        assertThat(service.dropIndex(dropIndexParams), willThrow(IndexNotFoundException.class));

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

        EventListener<CatalogEventParameters> eventListener = Mockito.mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        service.listen(CatalogEvent.TABLE_ALTER, eventListener);

        // Try to add column without table.
        assertThat(service.addColumn(addColumnParams), willThrow(TableNotFoundException.class));
        verifyNoInteractions(eventListener);

        // Create table.
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        // Add column.
        assertThat(service.addColumn(addColumnParams), willBe((Object) null));
        verify(eventListener).notify(any(AddColumnEventParameters.class), ArgumentMatchers.isNull());

        // Drop column.
        assertThat(service.dropColumn(dropColumnParams), willBe((Object) null));
        verify(eventListener).notify(any(DropColumnEventParameters.class), ArgumentMatchers.isNull());

        // Try drop column once again.
        assertThat(service.dropColumn(dropColumnParams), willThrow(ColumnNotFoundException.class));

        verifyNoMoreInteractions(eventListener);
    }

    private static CreateTableParams simpleTable(String name) {
        return CreateTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(name)
                .zone(ZONE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name("ID").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("VAL").type(ColumnType.INT32).nullable(true).build()
                ))
                .primaryKeyColumns(List.of("ID"))
                .build();
    }

    private static CreateSortedIndexParams simpleIndex(String indexName, String tableName) {
        return CreateSortedIndexParams.builder()
                .indexName(indexName)
                .tableName(tableName)
                .unique()
                .columns(List.of("VAL"))
                .collations(List.of(ColumnCollation.ASC_NULLS_LAST))
                .build();
    }
}
