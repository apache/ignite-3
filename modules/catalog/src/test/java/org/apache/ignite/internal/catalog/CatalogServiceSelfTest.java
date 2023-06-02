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
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.catalog.descriptors.DistributionZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
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
import org.apache.ignite.lang.DistributionZoneAlreadyExistsException;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.IgniteInternalException;
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

        DistributionZoneDescriptor zone = service.zone(1, System.currentTimeMillis());
        assertEquals(CatalogService.DEFAULT_ZONE_NAME, zone.name());

        assertEquals(1, zone.id());
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
        assertSame(schema.table(TABLE_NAME), service.table(2, System.currentTimeMillis()));

        // Validate newly created table
        TableDescriptor table = schema.table(TABLE_NAME);

        assertEquals(2L, table.id());
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
        assertSame(schema.table(TABLE_NAME), service.table(2, System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME_2), service.table(TABLE_NAME_2, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME_2), service.table(3, System.currentTimeMillis()));

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
        assertSame(schema.table(TABLE_NAME), service.table(2, beforeDropTimestamp));

        assertSame(schema.table(TABLE_NAME_2), service.table(TABLE_NAME_2, beforeDropTimestamp));
        assertSame(schema.table(TABLE_NAME_2), service.table(3, beforeDropTimestamp));

        // Validate actual catalog
        schema = service.schema(3);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(SCHEMA_NAME, schema.name());
        assertEquals(3, schema.version());
        assertSame(schema, service.activeSchema(System.currentTimeMillis()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(service.table(TABLE_NAME, System.currentTimeMillis()));
        assertNull(service.table(2, System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME_2), service.table(TABLE_NAME_2, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME_2), service.table(3, System.currentTimeMillis()));

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

        // Try to drop indexed column
        AlterTableDropColumnParams params = AlterTableDropColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(Set.of("VAL"))
                .build();

        //TODO: uncomment "https://issues.apache.org/jira/browse/IGNITE-19460"
        // assertThat(service.createIndex("CREATE INDEX myIndex ON myTable (VAL)"), willBe((Object) null));
        // assertThat(service.dropColumn(params), willThrow(IllegalArgumentException.class));

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
        assertEquals(1, schema.version());

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

        EventListener<CatalogEventParameters> eventListener = Mockito.mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        service.listen(CatalogEvent.TABLE_CREATE, eventListener);
        service.listen(CatalogEvent.TABLE_DROP, eventListener);

        CompletableFuture<Void> fut = service.createTable(params);

        assertThat(fut, willBe((Object) null));

        verify(eventListener).notify(any(CreateTableEventParameters.class), ArgumentMatchers.isNull());

        DropTableParams dropTableparams = DropTableParams.builder().tableName(TABLE_NAME).build();

        fut = service.dropTable(dropTableparams);

        assertThat(fut, willBe((Object) null));

        verify(eventListener).notify(any(DropTableEventParameters.class), ArgumentMatchers.isNull());
        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testCreateZone() {
        CreateZoneParams params = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(42)
                .replicas(15)
                .dataNodesAutoAdjust(73)
                .filter("expression")
                .build();

        CompletableFuture<Void> fut = service.createDistributionZone(params);

        assertThat(fut, willBe((Object) null));

        // Validate catalog version from the past.
        assertNull(service.zone(ZONE_NAME, 0));
        assertNull(service.zone(2, 0));
        assertNull(service.zone(ZONE_NAME, 123L));
        assertNull(service.zone(2, 123L));

        // Validate actual catalog
        DistributionZoneDescriptor zone = service.zone(ZONE_NAME, System.currentTimeMillis());

        assertNotNull(zone);
        assertSame(zone, service.zone(2, System.currentTimeMillis()));

        // Validate newly created zone
        assertEquals(2L, zone.id());
        assertEquals(ZONE_NAME, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());
        assertEquals(73, zone.dataNodesAutoAdjust());
        assertEquals(Integer.MAX_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(Integer.MAX_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("expression", zone.filter());
    }

    @Test
    public void testDropZone() throws InterruptedException {
        CreateZoneParams createZoneParams = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .build();

        assertThat(service.createDistributionZone(createZoneParams), willBe((Object) null));

        long beforeDropTimestamp = System.currentTimeMillis();

        Thread.sleep(5);

        DropZoneParams params = DropZoneParams.builder()
                .zoneName(ZONE_NAME)
                .build();

        CompletableFuture<Void> fut = service.dropDistributionZone(params);

        assertThat(fut, willBe((Object) null));

        // Validate catalog version from the past.
        DistributionZoneDescriptor zone = service.zone(ZONE_NAME, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(ZONE_NAME, zone.name());
        assertEquals(2, zone.id());

        assertSame(zone, service.zone(2, beforeDropTimestamp));

        // Validate actual catalog
        assertNull(service.zone(ZONE_NAME, System.currentTimeMillis()));
        assertNull(service.zone(2, System.currentTimeMillis()));

        // Try to drop non-existing zone.
        assertThat(service.dropDistributionZone(params), willThrow(DistributionZoneNotFoundException.class));
    }

    @Test
    public void testRenameZone() throws InterruptedException {
        CreateZoneParams createParams = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(42)
                .replicas(15)
                .build();

        RenameZoneParams renameZoneParams = RenameZoneParams.builder()
                .zoneName(ZONE_NAME)
                .newZoneName("RenamedZone")
                .build();

        assertThat(service.createDistributionZone(createParams), willBe((Object) null));

        long beforeDropTimestamp = System.currentTimeMillis();

        Thread.sleep(5);

        assertThat(service.renameDistributionZone(renameZoneParams), willBe((Object) null));

        // Validate catalog version from the past.
        DistributionZoneDescriptor zone = service.zone(ZONE_NAME, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(ZONE_NAME, zone.name());
        assertEquals(2, zone.id());

        assertSame(zone, service.zone(2, beforeDropTimestamp));

        // Validate actual catalog
        zone = service.zone("RenamedZone", System.currentTimeMillis());

        assertNotNull(zone);
        assertNull(service.zone(ZONE_NAME, System.currentTimeMillis()));
        assertEquals("RenamedZone", zone.name());
        assertEquals(2, zone.id());

        assertSame(zone, service.zone(2, System.currentTimeMillis()));
    }

    @Test
    public void testDefaultZone() {
        DistributionZoneDescriptor defaultZone = service.zone(CatalogService.DEFAULT_ZONE_NAME, System.currentTimeMillis());

        // Try to create zone with default zone name.
        CreateZoneParams createParams = CreateZoneParams.builder()
                .zoneName(CatalogService.DEFAULT_ZONE_NAME)
                .partitions(42)
                .replicas(15)
                .build();
        assertThat(service.createDistributionZone(createParams), willThrow(IgniteInternalException.class));

        // Validate default zone wasn't changed.
        assertSame(defaultZone, service.zone(CatalogService.DEFAULT_ZONE_NAME, System.currentTimeMillis()));

        // Try to rename default zone.
        RenameZoneParams renameZoneParams = RenameZoneParams.builder()
                .zoneName(CatalogService.DEFAULT_ZONE_NAME)
                .newZoneName("RenamedDefaultZone")
                .build();
        assertThat(service.renameDistributionZone(renameZoneParams), willThrow(IgniteInternalException.class));

        // Validate default zone wasn't changed.
        assertNull(service.zone("RenamedDefaultZone", System.currentTimeMillis()));
        assertSame(defaultZone, service.zone(CatalogService.DEFAULT_ZONE_NAME, System.currentTimeMillis()));

        // Try to drop default zone.
        DropZoneParams dropZoneParams = DropZoneParams.builder()
                .zoneName(CatalogService.DEFAULT_ZONE_NAME)
                .build();
        assertThat(service.dropDistributionZone(dropZoneParams), willThrow(IgniteInternalException.class));

        // Validate default zone wasn't changed.
        assertSame(defaultZone, service.zone(CatalogService.DEFAULT_ZONE_NAME, System.currentTimeMillis()));

        // Try to rename to a zone with default name.
        createParams = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(42)
                .replicas(15)
                .build();
        renameZoneParams = RenameZoneParams.builder()
                .zoneName(ZONE_NAME)
                .newZoneName(CatalogService.DEFAULT_ZONE_NAME)
                .build();

        assertThat(service.createDistributionZone(createParams), willBe((Object) null));
        defaultZone = service.zone(CatalogService.DEFAULT_ZONE_NAME, System.currentTimeMillis());

        assertThat(service.renameDistributionZone(renameZoneParams), willThrow(DistributionZoneAlreadyExistsException.class));

        // Validate default zone wasn't changed.
        assertSame(defaultZone, service.zone(CatalogService.DEFAULT_ZONE_NAME, System.currentTimeMillis()));
    }

    @Test
    public void testAlterZone() {
        CreateZoneParams createParams = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(42)
                .replicas(15)
                .dataNodesAutoAdjust(73)
                .filter("expression")
                .build();

        AlterZoneParams alterZoneParams = AlterZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(10)
                .replicas(2)
                .dataNodesAutoAdjustScaleUp(3)
                .dataNodesAutoAdjustScaleDown(4)
                .filter("newExpression")
                .build();

        assertThat(service.createDistributionZone(createParams), willBe((Object) null));
        assertThat(service.alterDistributionZone(alterZoneParams), willBe((Object) null));

        // Validate actual catalog
        DistributionZoneDescriptor zone = service.zone(ZONE_NAME, System.currentTimeMillis());
        assertNotNull(zone);
        assertSame(zone, service.zone(2, System.currentTimeMillis()));

        assertEquals(ZONE_NAME, zone.name());
        assertEquals(2, zone.id());
        assertEquals(10, zone.partitions());
        assertEquals(2, zone.replicas());
        assertEquals(Integer.MAX_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(3, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(4, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("newExpression", zone.filter());
    }

    @Test
    public void testCreateZoneWithSameName() {
        CreateZoneParams params = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(42)
                .replicas(15)
                .build();

        assertThat(service.createDistributionZone(params), willBe((Object) null));

        // Try to create zone with same name.
        params = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(8)
                .replicas(1)
                .build();

        assertThat(service.createDistributionZone(params), willThrowFast(DistributionZoneAlreadyExistsException.class));

        // Validate zone was NOT changed
        DistributionZoneDescriptor zone = service.zone(ZONE_NAME, System.currentTimeMillis());

        assertNotNull(zone);
        assertSame(zone, service.zone(2, System.currentTimeMillis()));
        assertNull(service.zone(3, System.currentTimeMillis()));

        assertEquals(2L, zone.id());
        assertEquals(ZONE_NAME, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());
    }

    @Test
    public void testDropZoneIfExistsFlag() {
        CreateZoneParams createZoneParams = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .build();

        assertThat(service.createDistributionZone(createZoneParams), willBe((Object) null));

        assertNotNull(service.zone(ZONE_NAME, System.currentTimeMillis()));
        assertNotNull(service.zone(1, System.currentTimeMillis()));

        DropZoneParams params = DropZoneParams.builder()
                .zoneName(ZONE_NAME)
                .build();

        assertThat(service.dropDistributionZone(params), willBe((Object) null));

        // Drop non-existing zone.
        assertThat(service.dropDistributionZone(params), willThrowFast(DistributionZoneNotFoundException.class));

        // Validate actual catalog
        assertNull(service.zone(ZONE_NAME, System.currentTimeMillis()));
        assertNull(service.zone(2, System.currentTimeMillis()));
    }

    @Test
    public void testCreateZoneEvents() {
        CreateZoneParams createZoneParams = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .build();

        DropZoneParams dropZoneParams = DropZoneParams.builder()
                .zoneName(ZONE_NAME)
                .build();

        EventListener<CatalogEventParameters> eventListener = Mockito.mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        service.listen(CatalogEvent.ZONE_CREATE, eventListener);
        service.listen(CatalogEvent.ZONE_DROP, eventListener);

        CompletableFuture<Void> fut = service.createDistributionZone(createZoneParams);

        assertThat(fut, willBe((Object) null));

        verify(eventListener).notify(any(CreateZoneEventParameters.class), ArgumentMatchers.isNull());

        fut = service.dropDistributionZone(dropZoneParams);

        assertThat(fut, willBe((Object) null));

        verify(eventListener).notify(any(DropZoneEventParameters.class), ArgumentMatchers.isNull());
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
}
