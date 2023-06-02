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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
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
import org.apache.ignite.lang.NodeStoppingException;
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
    }

    @Test
    public void testCreateTable() {
        CreateTableParams params = CreateTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .ifTableExists(true)
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
        fut = service.createTable(simpleTable(TABLE_NAME_2));

        assertThat(fut, willBe((Object) null));

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
    }

    @Test
    public void testCreateTableIfExistsFlag() {
        CreateTableParams params = CreateTableParams.builder()
                .tableName(TABLE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name("key").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("val").type(ColumnType.INT32).build()
                ))
                .primaryKeyColumns(List.of("key"))
                .ifTableExists(true)
                .build();

        assertThat(service.createTable(params), willBe((Object) null));
        assertThat(service.createTable(params), willThrowFast(TableAlreadyExistsException.class));

        CompletableFuture<?> fut = service.createTable(
                CreateTableParams.builder()
                        .tableName(TABLE_NAME)
                        .columns(List.of(
                                ColumnParams.builder().name("key").type(ColumnType.INT32).build(),
                                ColumnParams.builder().name("val").type(ColumnType.INT32).build()
                        ))
                        .primaryKeyColumns(List.of("key"))
                        .ifTableExists(false)
                        .build());

        assertThat(fut, willThrowFast(TableAlreadyExistsException.class));
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
    }

    @Test
    public void testDropTableIfExistsFlag() {
        CreateTableParams createTableParams = CreateTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name("key").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("val").type(ColumnType.INT32).build()
                ))
                .primaryKeyColumns(List.of("key"))
                .build();

        assertThat(service.createTable(createTableParams), willBe((Object) null));

        DropTableParams params = DropTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .ifTableExists(true)
                .build();

        assertThat(service.dropTable(params), willBe((Object) null));
        assertThat(service.dropTable(params), willThrowFast(TableNotFoundException.class));

        params = DropTableParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .ifTableExists(false)
                .build();

        assertThat(service.dropTable(params), willThrowFast(TableNotFoundException.class));
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
    public void testDropColumnIfTableExistsFlag() {
        assertNull(service.table(TABLE_NAME, System.currentTimeMillis()));

        AlterTableAddColumnParams params = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(ColumnParams.builder().name(NEW_COLUMN_NAME).type(ColumnType.INT32).nullable(true).build()))
                .ifTableExists(false)
                .build();

        assertThat(service.addColumn(params), willThrow(TableNotFoundException.class));

        params = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(ColumnParams.builder().name(NEW_COLUMN_NAME).type(ColumnType.INT32).nullable(true).build()))
                .ifTableExists(true)
                .build();

        assertThat(service.addColumn(params), willThrow(TableNotFoundException.class));
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
    public void testAddColumnIfTableExistsFlag() {
        assertNull(service.table(TABLE_NAME, System.currentTimeMillis()));

        AlterTableAddColumnParams params = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(ColumnParams.builder().name(NEW_COLUMN_NAME).type(ColumnType.INT32).nullable(true).build()))
                .ifTableExists(false)
                .build();

        assertThat(service.addColumn(params), willThrow(TableNotFoundException.class));

        params = AlterTableAddColumnParams.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(ColumnParams.builder().name(NEW_COLUMN_NAME).type(ColumnType.INT32).nullable(true).build()))
                .ifTableExists(true)
                .build();

        assertThat(service.addColumn(params), willThrow(TableNotFoundException.class));
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

    /**
     * Checks for possible changes to the default value of a column descriptor.
     *
     * <p>Set/drop default value allowed for any column.
     */
    @Test
    public void testAlterColumnDefault() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // NULL-> NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(null)),
                willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // NULL -> 1 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(1)),
                willBe((Object) null));
        assertNotNull(service.schema(++schemaVer));

        // 1 -> 1 : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(1)),
                willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // 1 -> 2 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(2)),
                willBe((Object) null));
        assertNotNull(service.schema(++schemaVer));

        // 2 -> NULL : Ok (for nullable column).
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> DefaultValue.constant(null)),
                willBe((Object) null));
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
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // NULLABLE -> NULLABLE : No-op.
        // NOT NULL -> NOT NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, false, null), willBe((Object) null));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null), willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // NOT NULL -> NULlABLE : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, false, null), willBe((Object) null));
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

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "DECIMAL", "STRING", "BYTE_ARRAY"}, mode = Mode.EXCLUDE)
    public void testAlterColumnTypeAnyPrecisionChangeIsRejected(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL").type(type).build();
        ColumnParams colWithPrecision = ColumnParams.builder().name("COL_PRECISION").type(type).precision(10).build();

        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col, colWithPrecision))), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        TestColumnTypeParams typeParams = new TestColumnTypeParams(type, 10, null);

        assertThat(
                changeColumn(TABLE_NAME, col.name(), typeParams, null, null),
                willThrowFast(SqlException.class, "Cannot change precision to 10 for column '" + col.name() + "'")
        );

        assertThat(
                changeColumn(TABLE_NAME, colWithPrecision.name(), typeParams, null, null),
                willThrowFast(SqlException.class, "Cannot change precision to 10 for column '" + colWithPrecision.name() + "'")
        );

        assertNull(service.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the precision of a column descriptor.
     *
     * <ul>
     *  <li>Increasing precision is allowed for non-PK {@link ColumnType#DECIMAL} column.</li>
     *  <li>Increasing length is allowed for non-PK {@link ColumnType#STRING} and {@link ColumnType#BYTE_ARRAY} column.</li>
     *  <li>Decreasing precision (and length for varlen types) is forbidden.</li>
     * </ul>
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"DECIMAL", "STRING", "BYTE_ARRAY"}, mode = Mode.INCLUDE)
    public void testAlterColumnTypePrecision(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL_" + type).type(type).build();

        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col))), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // ANY-> UNDEFINED PRECISION : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // UNDEFINED PRECISION -> 10 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), 10, null), null, null),
                willBe((Object) null)
        );
        assertNotNull(service.schema(++schemaVer));

        // 10 -> 11 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), 11, null), null, null),
                willBe((Object) null)
        );

        SchemaDescriptor schema = service.schema(++schemaVer);
        assertNotNull(schema);

        TableColumnDescriptor desc = schema.table(TABLE_NAME).column(col.name());

        assertNotSame(desc.length(), desc.precision());
        assertEquals(11, col.type() == ColumnType.DECIMAL ? desc.precision() : desc.length());

        // 11 -> 10 : Error.
        String expMsg = col.type() == ColumnType.DECIMAL
                ? "Cannot decrease precision to 10 for column '" + col.name() + "'."
                : "Cannot decrease length to 10 for column '" + col.name() + "'.";

        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), 10, null), null, null),
                willThrowFast(SqlException.class, expMsg)
        );
        assertNull(service.schema(schemaVer + 1));
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testAlterColumnTypeScaleIsRejected(ColumnType type) {
        ColumnParams pkCol = ColumnParams.builder().name("ID").type(ColumnType.INT32).build();
        ColumnParams col = ColumnParams.builder().name("COL_" + type).type(type).scale(3).build();
        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(pkCol, col))), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // ANY-> UNDEFINED SCALE : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // 3 -> 3 : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 3), null, null),
                willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // 3 -> 4 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 4), null, null),
                willThrowFast(SqlException.class, "Cannot change scale to 4 for column '" + col.name() + "'."));
        assertNull(service.schema(schemaVer + 1));

        // 3 -> 2 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 2), null, null),
                willThrowFast(SqlException.class, "Cannot change scale to 2 for column '" + col.name() + "'."));
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

        Map<ColumnType, Set<ColumnType>> validTransitions = new EnumMap<>(ColumnType.class);
        validTransitions.put(ColumnType.INT8, EnumSet.of(ColumnType.INT16, ColumnType.INT32, ColumnType.INT64));
        validTransitions.put(ColumnType.INT16, EnumSet.of(ColumnType.INT32, ColumnType.INT64));
        validTransitions.put(ColumnType.INT32, EnumSet.of(ColumnType.INT64));
        validTransitions.put(ColumnType.FLOAT, EnumSet.of(ColumnType.DOUBLE));

        assertThat(service.createTable(createTableParams), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        for (ColumnParams col : testColumns) {
            TypeSafeMatcher<CompletableFuture<?>> matcher;
            boolean sameType = col.type() == target;

            if (sameType || (validTransitions.containsKey(col.type()) && validTransitions.get(col.type()).contains(target))) {
                matcher = willBe((Object) null);
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
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        assertThat(changeColumn(TABLE_NAME, "ID", new TestColumnTypeParams(ColumnType.INT64), null, null),
                willThrowFast(SqlException.class, "Cannot change data type for primary key column 'ID'."));
    }

    /**
     * Ensures that the compound change command {@code SET DATA TYPE BIGINT NULL DEFAULT NULL}
     * will change the type, drop NOT NULL and the default value at the same time.
     */
    @Test
    public void testAlterColumnMultipleChanges() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        Supplier<DefaultValue> dflt = () -> DefaultValue.constant(null);
        boolean notNull = false;
        TestColumnTypeParams typeParams = new TestColumnTypeParams(ColumnType.INT64);

        // Ensures that 3 different actions applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe((Object) null));

        SchemaDescriptor schema = service.schema(++schemaVer);
        assertNotNull(schema);

        TableColumnDescriptor desc = schema.table(TABLE_NAME).column("VAL_NOT_NULL");
        assertEquals(DefaultValue.constant(null), desc.defaultValue());
        assertTrue(desc.nullable());
        assertEquals(ColumnType.INT64, desc.type());

        // Ensures that only one of three actions applied.
        dflt = () -> DefaultValue.constant(2);
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe((Object) null));

        schema = service.schema(++schemaVer);
        assertNotNull(schema);
        assertEquals(DefaultValue.constant(2), schema.table(TABLE_NAME).column("VAL_NOT_NULL").defaultValue());

        // Ensures that no action will be applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe((Object) null));
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
                .ifTableExists(true)
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
                .ifTableExists(true)
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

    private CompletableFuture<Void> changeColumn(
            String tab,
            String col,
            @Nullable TestColumnTypeParams typeParams,
            @Nullable Boolean notNull,
            @Nullable Supplier<DefaultValue> dflt
    ) {
        Builder builder = AlterColumnParams.builder()
                .tableName(tab)
                .columnName(col);

        if (typeParams != null) {
            builder.type(typeParams.type);

            if (typeParams.precision != null) {
                builder.precision(typeParams.precision);
            }

            if (typeParams.scale != null) {
                builder.scale(typeParams.scale);
            }
        }

        if (notNull != null) {
            builder.notNull(notNull);
        }

        if (dflt != null) {
            builder.defaultResolver(ignore -> dflt.get());
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
                .zone("ZONE")
                .columns(cols)
                .primaryKeyColumns(List.of(cols.get(0).name()))
                .build();
    }

    private static class TestColumnTypeParams {
        private final ColumnType type;
        private final Integer precision;
        private final Integer scale;

        private TestColumnTypeParams(ColumnType type) {
            this(type, null, null);
        }

        private TestColumnTypeParams(ColumnType type, @Nullable Integer precision, @Nullable Integer scale) {
            this.type = type;
            this.precision = precision;
            this.scale = scale;
        }
    }
}
