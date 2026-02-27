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

import static java.lang.Double.compare;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.addColumnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.applyNecessaryLength;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.applyNecessaryPrecision;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParamsBuilder;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.dropColumnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.initializeColumnWithDefaults;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PRECISION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_SCALE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_ZONE_QUORUM_SIZE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.commands.DefaultValue.constant;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.STRONG_CONSISTENCY;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.DURATION;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.apache.ignite.sql.ColumnType.NULL;
import static org.apache.ignite.sql.ColumnType.PERIOD;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommandBuilder;
import org.apache.ignite.internal.catalog.commands.AlterTableSetPropertyCommand;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.RenameTableCommand;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

/** Tests for table related commands. */
public class CatalogTableTest extends BaseCatalogManagerTest {

    private static final String NEW_COLUMN_NAME = "NEWCOL";
    private static final String NEW_COLUMN_NAME_2 = "NEWCOL2";
    private static final int DFLT_TEST_PRECISION = 11;

    @Test
    public void testCreateTable() {
        long timePriorToTableCreation = clock.nowLong();

        CatalogCommand command = CreateTableCommand.builder()
                .tableName(TABLE_NAME)
                .schemaName(SCHEMA_NAME)
                .columns(List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1", "key2")).build())
                .colocationColumns(List.of("key2"))
                .build();

        tryApplyAndExpectApplied(command);

        // Validate catalog version from the past.
        Catalog catalog = manager.activeCatalog(timePriorToTableCreation);

        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);

        assertNotNull(schema);
        assertNull(schema.table(TABLE_NAME));
        assertNull(schema.aliveIndex(pkIndexName(TABLE_NAME)));

        assertNull(catalog.table(SCHEMA_NAME, TABLE_NAME));
        assertNull(catalog.aliveIndex(SCHEMA_NAME, pkIndexName(TABLE_NAME)));

        // Validate actual catalog
        catalog = manager.activeCatalog(clock.nowLong());

        schema = catalog.schema(SCHEMA_NAME);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        CatalogHashIndexDescriptor pkIndex = (CatalogHashIndexDescriptor) schema.aliveIndex(pkIndexName(TABLE_NAME));

        assertNotNull(schema);

        assertSame(table, catalog.table(SCHEMA_NAME, TABLE_NAME));
        assertSame(table, catalog.table(table.id()));
        assertSame(pkIndex, catalog.aliveIndex(SCHEMA_NAME, pkIndexName(TABLE_NAME)));
        assertSame(pkIndex, catalog.index(pkIndex.id()));

        // Validate newly created table
        assertEquals(TABLE_NAME, table.name());
        assertEquals(catalog.defaultZone().id(), table.zoneId());
        assertEquals(IntList.of(0, 1), table.primaryKeyColumns());
        assertEquals(IntList.of(1), table.colocationColumns());

        // Validate newly created pk index
        assertEquals(pkIndexName(TABLE_NAME), pkIndex.name());
        assertEquals(table.id(), pkIndex.tableId());
        assertEquals(IntList.of(0, 1), pkIndex.columnIds());
        assertTrue(pkIndex.unique());
        assertTrue(pkIndex.isCreatedWithTable());
        assertEquals(AVAILABLE, pkIndex.status());

        CatalogTableColumnDescriptor desc = table.column("key1");
        assertNotNull(desc);
        // INT32 key
        assertThat(desc.precision(), is(DEFAULT_PRECISION));

        assertEquals(0, table.columnIndex("key1"));
        assertEquals(1, table.columnIndex("key2"));

        // Validate column ids
        assertEquals(0, table.column("key1").id());
        assertEquals(1, table.column("key2").id());
        assertEquals(2, table.column("val").id());
    }

    @Test
    public void testCreateTableWithNamedPk() {
        long timePriorToTableCreation = clock.nowLong();
        String expectedName = "CUSTOM_PK_NAME";

        CatalogCommand command = CreateTableCommand.builder()
                .tableName(TABLE_NAME)
                .schemaName(SCHEMA_NAME)
                .columns(List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder()
                        .name(expectedName)
                        .columns(List.of("key1", "key2"))
                        .build())
                .colocationColumns(List.of("key2"))
                .build();

        tryApplyAndExpectApplied(command);

        // Validate catalog version from the past.
        Catalog catalog = manager.activeCatalog(timePriorToTableCreation);

        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);

        assertNotNull(schema);
        assertNull(schema.table(TABLE_NAME));
        assertNull(schema.aliveIndex(expectedName));

        assertNull(catalog.table(SCHEMA_NAME, TABLE_NAME));
        assertNull(catalog.aliveIndex(SCHEMA_NAME, pkIndexName(TABLE_NAME)));
        assertNull(catalog.aliveIndex(SCHEMA_NAME, expectedName));

        // Validate actual catalog
        catalog = manager.activeCatalog(clock.nowLong());

        schema = catalog.schema(SCHEMA_NAME);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);

        // There should be no index with default name.
        assertNull(catalog.aliveIndex(SCHEMA_NAME, pkIndexName(TABLE_NAME)));

        CatalogHashIndexDescriptor pkIndex = (CatalogHashIndexDescriptor) schema.aliveIndex(expectedName);

        assertSame(pkIndex, catalog.aliveIndex(SCHEMA_NAME, expectedName));
        assertSame(pkIndex, catalog.index(pkIndex.id()));

        // Validate newly created pk index
        assertEquals(expectedName, pkIndex.name());
        assertEquals(table.id(), pkIndex.tableId());
        assertEquals(IntList.of(0, 1), pkIndex.columnIds());
        assertTrue(pkIndex.unique());
        assertTrue(pkIndex.isCreatedWithTable());
        assertEquals(AVAILABLE, pkIndex.status());
    }

    @Test
    public void testCreateMultipleTables() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        tryApplyAndExpectApplied(simpleTable(TABLE_NAME_2));

        Catalog catalog = manager.latestCatalog();
        assertNotNull(catalog);

        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);
        assertNotNull(schema);

        CatalogTableDescriptor table1 = schema.table(TABLE_NAME);
        assertNotNull(table1, "table1");

        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);
        assertNotNull(table2, "table2");

        assertNotEquals(table1.id(), table2.id());

        CatalogIndexDescriptor index1 = schema.aliveIndex(pkIndexName(TABLE_NAME));
        assertNotNull(index1, "index1");

        CatalogIndexDescriptor index2 = schema.aliveIndex(pkIndexName(TABLE_NAME_2));
        assertNotNull(index2, "index2");

        assertNotEquals(index1.id(), index2.id());
        assertNotSame(index1, index2);
    }

    @Test
    public void testCreateTableWithSameNameIsNotPossible() {
        CatalogCommand command1 = CreateTableCommand.builder()
                .tableName(TABLE_NAME)
                .schemaName(SCHEMA_NAME)
                .columns(List.of(columnParams("key1", INT32), columnParams("key2", INT32)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1", "key2")).build())
                .build();

        tryApplyAndExpectApplied(command1);

        assertThat(
                manager.execute(command1),
                willThrowFast(CatalogValidationException.class)
        );
    }

    @Test
    public void testDropTable() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        long beforeDropTimestamp = clock.nowLong();

        tryApplyAndExpectApplied(dropTableCommand(TABLE_NAME));

        // Validate catalog version from the past.
        Catalog catalog = manager.activeCatalog(beforeDropTimestamp);

        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);
        CatalogTableDescriptor table1 = schema.table(TABLE_NAME);
        CatalogIndexDescriptor pkIndex1 = schema.aliveIndex(pkIndexName(TABLE_NAME));

        assertNotNull(schema);

        assertSame(table1, catalog.table(SCHEMA_NAME, TABLE_NAME));
        assertSame(table1, catalog.table(table1.id()));

        assertSame(pkIndex1, catalog.aliveIndex(SCHEMA_NAME, pkIndexName(TABLE_NAME)));
        assertSame(pkIndex1, catalog.index(pkIndex1.id()));

        // Validate actual catalog
        catalog = manager.activeCatalog(clock.nowLong());
        schema = catalog.schema(SCHEMA_NAME);

        assertNotNull(schema);

        assertNull(schema.table(TABLE_NAME));
        assertNull(catalog.table(SCHEMA_NAME, TABLE_NAME));
        assertNull(catalog.table(table1.id()));

        assertThat(schema.aliveIndex(pkIndexName(TABLE_NAME)), is(nullValue()));
        assertThat(catalog.aliveIndex(SCHEMA_NAME, pkIndexName(TABLE_NAME)), is(nullValue()));
        assertThat(catalog.index(pkIndex1.id()), is(nullValue()));
    }

    @Test
    public void testReCreateTableWithSameName() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        long beforeDropTimestamp = clock.nowLong();

        Catalog catalog = manager.activeCatalog(beforeDropTimestamp);

        CatalogTableDescriptor table1 = catalog.table(SCHEMA_NAME, TABLE_NAME);
        assertNotNull(table1);

        // Drop table.
        tryApplyAndExpectApplied(dropTableCommand(TABLE_NAME));
        assertNull(actualTable(TABLE_NAME));

        // Re-create table with same name.
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        CatalogTableDescriptor table2 = manager.activeCatalog(clock.nowLong()).table(SCHEMA_NAME, TABLE_NAME);
        assertNotNull(table2);

        // Ensure these are different tables.
        assertNotEquals(table1.id(), table2.id());

        // Ensure table is available for historical queries.
        assertNotNull(manager.activeCatalog(beforeDropTimestamp).table(table1.id()));
        assertNull(manager.activeCatalog(beforeDropTimestamp).table(table2.id()));
    }

    @Test
    public void testAddColumn() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        long beforeAddedTimestamp = clock.nowLong();

        tryApplyAndExpectApplied(addColumnParams(TABLE_NAME,
                columnParamsBuilder(NEW_COLUMN_NAME, STRING, 11, true).defaultValue(constant("Ignite!")).build()
        ));

        // Validate catalog version from the past.
        CatalogTableDescriptor table = manager.activeCatalog(beforeAddedTimestamp).table(SCHEMA_NAME, TABLE_NAME);

        assertNotNull(table);
        assertNull(table.column(NEW_COLUMN_NAME));

        int maxColumnId = table.columns().stream().mapToInt(CatalogTableColumnDescriptor::id).max().orElseThrow();

        // Validate actual catalog
        table = actualTable(TABLE_NAME);

        assertNotNull(table);

        // Validate column descriptor.
        CatalogTableColumnDescriptor column = table.column(NEW_COLUMN_NAME);

        int expectedColumnId = maxColumnId + 1;
        assertEquals(expectedColumnId, column.id());
        assertEquals(NEW_COLUMN_NAME, column.name());
        assertEquals(STRING, column.type());
        assertTrue(column.nullable());

        assertEquals(DefaultValue.Type.CONSTANT, column.defaultValue().type());
        assertEquals("Ignite!", ((DefaultValue.ConstantValue) column.defaultValue()).value());

        assertEquals(11, column.length());
        assertEquals(DEFAULT_PRECISION, column.precision());
        assertEquals(DEFAULT_SCALE, column.scale());

        assertEquals(6, table.columnIndex(NEW_COLUMN_NAME));
        assertSame(column, table.columnById(expectedColumnId));
    }

    @Test
    public void testDropColumn() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        long beforeAddedTimestamp = clock.nowLong();

        tryApplyAndExpectApplied(dropColumnParams(TABLE_NAME, "VAL"));

        // Validate catalog version from the past.
        CatalogTableDescriptor table = manager.activeCatalog(beforeAddedTimestamp).table(SCHEMA_NAME, TABLE_NAME);
        List<CatalogTableColumnDescriptor> columns = table.columns();

        assertNotNull(table.column("VAL"));
        assertEquals(6, columns.size());

        // Validate actual catalog
        table = actualTable(TABLE_NAME);

        assertNull(table.column("VAL"));
        assertEquals(5, table.columns().size());
        assertEquals(columns.stream().filter(c -> !c.name().equals("VAL")).collect(toList()), table.columns());
    }

    @Test
    public void testAddDropMultipleColumns() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        // Add duplicate column.
        assertThat(
                manager.execute(addColumnParams(TABLE_NAME, columnParams(NEW_COLUMN_NAME, INT32, true), columnParams("VAL", INT32, true))),
                willThrow(CatalogValidationException.class)
        );

        // Validate no column added.
        CatalogTableDescriptor table = actualTable(TABLE_NAME);

        assertNull(table.column(NEW_COLUMN_NAME));

        // Add multiple columns.
        tryApplyAndExpectApplied(addColumnParams(TABLE_NAME,
                columnParams(NEW_COLUMN_NAME, INT32, true), columnParams(NEW_COLUMN_NAME_2, INT32, true)
        ));

        // Validate both columns added.
        table = actualTable(TABLE_NAME);

        assertNotNull(table.column(NEW_COLUMN_NAME));
        assertNotNull(table.column(NEW_COLUMN_NAME_2));

        // Drop multiple columns.
        tryApplyAndExpectApplied(dropColumnParams(TABLE_NAME, NEW_COLUMN_NAME, NEW_COLUMN_NAME_2));

        // Validate both columns dropped.
        table = actualTable(TABLE_NAME);

        assertNull(table.column(NEW_COLUMN_NAME));
        assertNull(table.column(NEW_COLUMN_NAME_2));
    }

    @Test
    public void testDropNotExistingTable() {
        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willThrowFast(CatalogValidationException.class));
    }

    @Test
    public void testDropColumnWithNotExistingTable() {
        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "key")),
                willThrowFast(CatalogValidationException.class, "Table with name 'PUBLIC.test_table' not found."));
    }

    @Test
    public void testDropColumnWithMissingTableColumns() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "fake")),
                willThrowFast(CatalogValidationException.class, "Column with name 'fake' not found in table 'PUBLIC.test_table'."));
    }

    @Test
    public void testDropColumnWithPrimaryKeyColumns() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        assertThat(
                manager.execute(dropColumnParams(TABLE_NAME, "ID")),
                willThrowFast(CatalogValidationException.class, "Deleting column `ID` belonging to primary key is not allowed")
        );
    }

    @Test
    public void testDropColumnWithIndexColumns() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));
        tryApplyAndExpectApplied(simpleIndex());

        assertThat(
                manager.execute(dropColumnParams(TABLE_NAME, "VAL")),
                willThrowFast(
                        CatalogValidationException.class,
                        "Deleting column 'VAL' used by index(es) [myIndex], it is not allowed"
                )
        );
    }

    @Test
    public void testAddColumnWithNotExistingTable() {
        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams("key", INT32, true))),
                willThrowFast(CatalogValidationException.class, "Table with name 'PUBLIC.test_table' not found."));
    }

    @Test
    public void testAddColumnWithExistingName() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams("ID", INT32, true))),
                willThrowFast(CatalogValidationException.class, "Column with name 'ID' already exists."));
    }

    @Test
    public void testTableEvents() {
        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any())).thenReturn(falseCompletedFuture());

        manager.listen(CatalogEvent.TABLE_CREATE, eventListener);
        manager.listen(CatalogEvent.TABLE_DROP, eventListener);

        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME_2));
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME_3));
        verify(eventListener, times(3)).notify(any(CreateTableEventParameters.class));

        tryApplyAndExpectApplied(dropTableCommand(TABLE_NAME));
        tryApplyAndExpectApplied(dropTableCommand(TABLE_NAME_2));
        verify(eventListener, times(2)).notify(any(DropTableEventParameters.class));

        verifyNoMoreInteractions(eventListener);
        clearInvocations(eventListener);
    }

    @Test
    public void testTables() {
        int initialVersion = manager.latestCatalogVersion();

        tryApplyAndExpectApplied(simpleTable(TABLE_NAME + 0));

        int afterFirstTableCreated = manager.latestCatalogVersion();

        tryApplyAndExpectApplied(simpleTable(TABLE_NAME + 1));

        assertThat(manager.catalog(initialVersion).tables(), empty());
        assertThat(
                manager.catalog(afterFirstTableCreated).tables(),
                hasItems(table(afterFirstTableCreated, TABLE_NAME + 0))
        );
        assertThat(
                manager.activeCatalog(clock.nowLong()).tables(),
                hasItems(table(manager.latestCatalogVersion(), TABLE_NAME + 0), table(manager.latestCatalogVersion(), TABLE_NAME + 1))
        );
    }

    @Test
    public void testCreateDefaultZoneLazilyIfNoZonesProvided() {
        // Check that initially there no default zone and zones at all.
        Catalog initialCatalog = manager.latestCatalog();
        assertThat(initialCatalog.zones(), empty());
        assertThat(initialCatalog.defaultZone(), nullValue());

        // Create table that should triggers new default zone creation.
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        // Check that initially there is only new default zone is presented.
        int afterTableCreatedCatalogVersion = manager.latestCatalogVersion();
        Catalog afterTableCreatedCatalog = manager.catalog(afterTableCreatedCatalogVersion);
        CatalogZoneDescriptor defaultZone = afterTableCreatedCatalog.defaultZone();
        assertThat(defaultZone, notNullValue());
        assertThat(afterTableCreatedCatalog.zones(), contains(defaultZone));
        checkDefaultZoneProperties(defaultZone);
    }

    @Test
    public void testDefaultZoneCreationIsIdempotent() {
        // Check that initially there no default zone and zones at all.
        Catalog initialCatalog = manager.latestCatalog();
        assertThat(initialCatalog.zones(), empty());
        assertThat(initialCatalog.defaultZone(), nullValue());

        // Create table that should triggers new default zone creation.
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME + 0));
        // Check that new tables won't trigger new default zone and will be placed into the created one.
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME + 1));

        int afterSecondTableCreatedCatalogVersion = manager.latestCatalogVersion();
        Catalog afterSecondTableCreatedCatalog =  manager.catalog(afterSecondTableCreatedCatalogVersion);
        CatalogZoneDescriptor defaultZone = afterSecondTableCreatedCatalog.defaultZone();
        assertThat(defaultZone, notNullValue());
        assertThat(afterSecondTableCreatedCatalog.zones(), contains(defaultZone));
        checkDefaultZoneProperties(defaultZone);
    }

    @Test
    public void testDefaultZoneCannotBeCreatedIfDefaultNameIsAlreadyInUse() {
        // Check that initially there no default zone and zones at all.
        Catalog initialCatalog = manager.latestCatalog();
        assertThat(initialCatalog.zones(), empty());
        assertThat(initialCatalog.defaultZone(), nullValue());

        // Create table that should triggers new default zone creation.
        tryApplyAndExpectApplied(simpleZone(DEFAULT_ZONE_NAME));
        // Check that new tables won't trigger new default zone and will be placed into the created one.
        assertThat(
                manager.execute(simpleTable(TABLE_NAME)),
                willThrowFast(
                        CatalogValidationException.class,
                        "Distribution zone with name '" + DEFAULT_ZONE_NAME + "' already exists."
                )
        );

        Catalog lastCatalog = manager.latestCatalog();
        assertThat(lastCatalog.zones(), hasSize(1));
        assertThat(initialCatalog.defaultZone(), nullValue());
    }

    @Test
    public void testTableRename() {
        createSomeTable(TABLE_NAME);

        int prevVersion = manager.latestCatalogVersion();

        CatalogCommand command = RenameTableCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .newTableName(TABLE_NAME_2)
                .build();

        tryApplyAndExpectApplied(command);

        int curVersion = manager.latestCatalogVersion();

        CatalogTableDescriptor prevDescriptor = table(prevVersion, TABLE_NAME);
        CatalogTableDescriptor curDescriptor = table(curVersion, TABLE_NAME_2);

        assertThat(prevDescriptor, is(notNullValue()));
        assertThat(prevDescriptor.name(), is(TABLE_NAME));

        assertThat(curDescriptor, is(notNullValue()));
        assertThat(curDescriptor.name(), is(TABLE_NAME_2));

        assertThat(table(prevVersion, TABLE_NAME_2), is(nullValue()));
        assertThat(table(curVersion, TABLE_NAME), is(nullValue()));

        // Assert that all other properties have been left intact.
        assertThat(curDescriptor.id(), is(prevDescriptor.id()));
        assertThat(curDescriptor.columns(), is(prevDescriptor.columns()));
        assertThat(curDescriptor.colocationColumns(), is(prevDescriptor.colocationColumns()));
        assertThat(curDescriptor.primaryKeyColumns(), is(prevDescriptor.primaryKeyColumns()));
        assertThat(curDescriptor.primaryKeyIndexId(), is(prevDescriptor.primaryKeyIndexId()));
        assertThat(curDescriptor.schemaId(), is(prevDescriptor.schemaId()));
        assertThat(curDescriptor.latestSchemaVersion(), is(prevDescriptor.latestSchemaVersion()));
    }

    @Test
    public void testTableRenameAndCreateTableWithSameName() {
        createSomeTable(TABLE_NAME);

        CatalogCommand command = RenameTableCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .newTableName(TABLE_NAME_2)
                .build();

        tryApplyAndExpectApplied(command);

        createSomeTable(TABLE_NAME);

        int catalogVersion = manager.latestCatalogVersion();

        assertThat(table(catalogVersion, TABLE_NAME), is(notNullValue()));
        assertThat(table(catalogVersion, TABLE_NAME_2), is(notNullValue()));
    }

    @Test
    public void addColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        tryApplyAndExpectApplied(addColumnParams(TABLE_NAME, columnParams("val2", INT32, true)));

        CatalogTableDescriptor table = actualTable(TABLE_NAME);

        assertThat(table.latestSchemaVersion(), is(2));
    }

    @Test
    public void createTableProducesTableVersion1() {
        createSomeTable(TABLE_NAME);

        CatalogTableDescriptor table = actualTable(TABLE_NAME);

        assertThat(table.latestSchemaVersion(), is(1));
    }

    @Test
    public void dropColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        tryApplyAndExpectApplied(dropColumnParams(TABLE_NAME, "val1"));

        CatalogTableDescriptor table = actualTable(TABLE_NAME);

        assertThat(table.latestSchemaVersion(), is(2));
    }

    @Test
    public void testTableRenameFiresEvent() {
        createSomeTable(TABLE_NAME);

        var fireEventFuture = new CompletableFuture<Void>();

        manager.listen(CatalogEvent.TABLE_ALTER, fromConsumer(fireEventFuture, (RenameTableEventParameters parameters) -> {
            CatalogTableDescriptor tableDescriptor = table(manager.latestCatalogVersion(), TABLE_NAME_2);

            assertThat(parameters.tableId(), is(tableDescriptor.id()));
            assertThat(parameters.newTableName(), is(tableDescriptor.name()));
        }));

        CatalogCommand command = RenameTableCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .newTableName(TABLE_NAME_2)
                .build();

        tryApplyAndExpectApplied(command);
        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    public void testColumnEvents() {
        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any())).thenReturn(falseCompletedFuture());

        manager.listen(CatalogEvent.TABLE_ALTER, eventListener);

        // Try to add column without table.
        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams(NEW_COLUMN_NAME, INT32, true))),
                willThrow(CatalogValidationException.class, "Table with name 'PUBLIC.test_table' not found."));
        verifyNoInteractions(eventListener);

        // Create table.
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        // Add column.
        tryApplyAndExpectApplied(addColumnParams(TABLE_NAME, columnParams(NEW_COLUMN_NAME, INT32, true)));
        verify(eventListener).notify(any(AddColumnEventParameters.class));

        // Drop column.
        tryApplyAndExpectApplied(dropColumnParams(TABLE_NAME, NEW_COLUMN_NAME));
        verify(eventListener).notify(any(DropColumnEventParameters.class));

        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testGetCatalogEntityInCatalogEvent() {
        var fireEventFuture = new CompletableFuture<Void>();

        manager.listen(CatalogEvent.TABLE_CREATE, fromConsumer(fireEventFuture, parameters -> {
            assertNotNull(manager.catalog(parameters.catalogVersion()));
        }));

        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));
        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    public void testGetTableByIdAndCatalogVersion() {
        int tableCreationVersion = tryApplyAndExpectApplied((simpleTable(TABLE_NAME))).getCatalogVersion();

        CatalogTableDescriptor table = manager.activeCatalog(clock.nowLong()).table(SCHEMA_NAME, TABLE_NAME);

        assertNull(manager.catalog(tableCreationVersion - 1).table(table.id()));
        assertSame(table, manager.catalog(tableCreationVersion).table(table.id()));
    }

    @Test
    public void alterColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        tryApplyAndExpectApplied(
                AlterTableAlterColumnCommand.builder()
                        .schemaName(SCHEMA_NAME)
                        .tableName(TABLE_NAME)
                        .columnName("val1")
                        .type(INT64)
                        .build()
        );

        CatalogTableDescriptor table = actualTable(TABLE_NAME);

        assertThat(table.latestSchemaVersion(), is(2));
    }

    /**
     * Checks for possible changes to the default value of a column descriptor.
     *
     * <p>Set/drop default value allowed for any column.
     */
    @Test
    public void testAlterColumnDefault() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        int catalogVersion = manager.latestCatalogVersion();
        assertEquals(constant(null), actualTable(TABLE_NAME).column("VAL").defaultValue());

        // NULL-> NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(null)),
                willBeNotApplied());

        assertEquals(constant(null), actualTable(TABLE_NAME).column("VAL").defaultValue());
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // NULL -> 1 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(1)),
                willBeApplied());
        catalogVersion++;

        assertEquals(constant(1), actualTable(TABLE_NAME).column("VAL").defaultValue());
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // 1 -> 1 : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(1)),
                willBeNotApplied());

        assertEquals(constant(1), actualTable(TABLE_NAME).column("VAL").defaultValue());
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // 1 -> 2 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(2)),
                willBeApplied());
        catalogVersion++;

        assertEquals(constant(2), actualTable(TABLE_NAME).column("VAL").defaultValue());
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // 2 -> NULL : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(null)),
                willBeApplied());
        catalogVersion++;

        assertEquals(constant(null), actualTable(TABLE_NAME).column("VAL").defaultValue());
        assertEquals(catalogVersion, manager.latestCatalogVersion());
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
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        int catalogVersion = manager.latestCatalogVersion();

        // NULLABLE -> NULLABLE : No-op.
        // NOT NULL -> NOT NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, false, null), willCompleteSuccessfully());
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null), willCompleteSuccessfully());
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // NOT NULL -> NULlABLE : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, false, null), willCompleteSuccessfully());
        catalogVersion++;

        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // DROP NOT NULL for PK : PK column can't be `null`.
        assertThat(changeColumn(TABLE_NAME, "ID", null, false, null),
                willThrowFast(CatalogValidationException.class, "Dropping NOT NULL constraint on key column is not allowed"));

        // NULlABLE -> NOT NULL : Forbidden because this change lead to incompatible schemas.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, true, null),
                willThrowFast(CatalogValidationException.class, "Adding NOT NULL constraint is not allowed"));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null),
                willThrowFast(CatalogValidationException.class, "Adding NOT NULL constraint is not allowed"));

        assertEquals(catalogVersion, manager.latestCatalogVersion());
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
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col1 = columnParamsBuilder("COL_DECIMAL1", DECIMAL).precision(DFLT_TEST_PRECISION - 1).scale(1).build();
        ColumnParams col2 = columnParamsBuilder("COL_DECIMAL2", DECIMAL).precision(DFLT_TEST_PRECISION).scale(1).build();

        tryApplyAndExpectApplied(simpleTable(TABLE_NAME, List.of(pkCol, col1, col2)));

        int catalogVersion = manager.latestCatalogVersion();

        // precision increment : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col1.name(), new TestColumnTypeParams(col1.type(), DFLT_TEST_PRECISION, null, null), null, null),
                willCompleteSuccessfully()
        );
        catalogVersion++;
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        assertThat(
                changeColumn(TABLE_NAME, col2.name(), new TestColumnTypeParams(col2.type(), DFLT_TEST_PRECISION, null, null), null, null),
                willCompleteSuccessfully()
        );
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // No change.
        assertThat(
                changeColumn(TABLE_NAME, col1.name(), new TestColumnTypeParams(col1.type(), DFLT_TEST_PRECISION, null, null), null, null),
                willCompleteSuccessfully()
        );
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // precision decrement : Forbidden because this change lead to incompatible schemas.
        assertThat(
                changeColumn(TABLE_NAME, col1.name(),
                        new TestColumnTypeParams(col1.type(), DFLT_TEST_PRECISION - 1, null, null), null, null),
                willThrowFast(CatalogValidationException.class, "Decreasing the precision for column of type '"
                        + col1.type() + "' is not allowed")
        );
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        assertThat(
                changeColumn(TABLE_NAME, col2.name(),
                        new TestColumnTypeParams(col2.type(), DFLT_TEST_PRECISION - 1, null, null), null, null),
                willThrowFast(CatalogValidationException.class, "Decreasing the precision for column of type '"
                        + col1.type() + "' is not allowed")
        );
        assertEquals(catalogVersion, manager.latestCatalogVersion());
    }

    /**
     * Changing precision is not supported for all types other than DECIMAL.
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "DECIMAL", "PERIOD", "DURATION"}, mode = Mode.EXCLUDE)
    public void testAlterColumnTypeAnyPrecisionChangeIsRejected(ColumnType type) {
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams colWithPrecision;
        Builder colWithPrecisionBuilder = columnParamsBuilder("COL_PRECISION", type).precision(3);
        applyNecessaryLength(type, colWithPrecisionBuilder);

        if (type.scaleAllowed()) {
            colWithPrecisionBuilder.scale(0);
        }

        if (!type.precisionAllowed() && !type.scaleAllowed()) {
            assertThrowsWithCause(colWithPrecisionBuilder::build, CatalogValidationException.class);
            return;
        }

        colWithPrecision = colWithPrecisionBuilder.build();

        tryApplyAndExpectApplied(simpleTable(TABLE_NAME, List.of(pkCol, colWithPrecision)));

        int catalogVersion = manager.latestCatalogVersion();

        int origPrecision = colWithPrecision.precision() == null ? 11 : colWithPrecision.precision();

        // change precision different from default
        assertThat(changeColumn(TABLE_NAME, colWithPrecision.name(),
                        new TestColumnTypeParams(type, origPrecision - 1, null, null), null, null),
                willThrowFast(CatalogValidationException.class,
                        "Changing the precision for column of type '" + colWithPrecision.type() + "' is not allowed"));

        assertThat(changeColumn(TABLE_NAME, colWithPrecision.name(),
                        new TestColumnTypeParams(type, origPrecision + 1, null, null), null, null),
                willThrowFast(CatalogValidationException.class,
                        "Changing the precision for column of type '" + colWithPrecision.type() + "' is not allowed"));

        assertEquals(catalogVersion, manager.latestCatalogVersion());
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
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col = columnParamsBuilder("COL_" + type, type).length(10).build();

        tryApplyAndExpectApplied(simpleTable(TABLE_NAME, List.of(pkCol, col)));

        int catalogVersion = manager.latestCatalogVersion();

        // 10 -> 11 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 11, null), null, null),
                willCompleteSuccessfully()
        );
        catalogVersion++;
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // 11 -> 10 : Error.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 10, null), null, null),
                willThrowFast(CatalogValidationException.class, "Decreasing the length for column of type '"
                        + col.type() + "' is not allowed")
        );
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // 11 -> 11 : No-op.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 11, null), null, null),
                willCompleteSuccessfully()
        );
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // No change.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willCompleteSuccessfully());
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // 11 -> 10 : failed.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 10, null), null, null),
                willThrowFast(CatalogValidationException.class)
        );
        assertEquals(catalogVersion, manager.latestCatalogVersion());
    }

    /**
     * Changing length is forbidden for all types other than STRING and BYTE_ARRAY.
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"STRING", "BYTE_ARRAY", "NULL"}, mode = Mode.EXCLUDE)
    public void testAlterColumnTypeAnyLengthChangeIsRejected(ColumnType type) {
        ColumnParams pkCol = columnParams("ID", INT32);
        Builder colBuilder = columnParamsBuilder("COL", type);
        Builder colWithLengthBuilder = columnParamsBuilder("COL_PRECISION", type).length(10);

        applyNecessaryLength(type, colWithLengthBuilder);

        initializeColumnWithDefaults(type, colBuilder);
        initializeColumnWithDefaults(type, colWithLengthBuilder);

        ColumnParams col = colBuilder.build();

        if (!type.lengthAllowed()) {
            assertThrowsWithCause(colWithLengthBuilder::build, CatalogValidationException.class);
            return;
        }

        ColumnParams colWithLength = colWithLengthBuilder.build();

        assertThat(
                manager.execute(simpleTable(TABLE_NAME, List.of(pkCol, col, colWithLength))),
                willCompleteSuccessfully()
        );

        int catalogVersion = manager.latestCatalogVersion();

        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(type, null, 1 << 6, null), null, null),
                willThrowFast(CatalogValidationException.class,
                        "Changing the length for column of type '" + col.type() + "' is not allowed"));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 1 << 5, null), null, null),
                willCompleteSuccessfully());

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 9, null), null, null),
                willThrowFast(CatalogValidationException.class,
                        "Changing the length for column of type '" + colWithLength.type() + "' is not allowed"));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 11, null), null, null),
                willThrowFast(CatalogValidationException.class,
                        "Changing the length for column of type '" + colWithLength.type() + "' is not allowed"));

        assertEquals(catalogVersion, manager.latestCatalogVersion());
    }

    /**
     * Changing scale is incompatible change, thus it's forbidden for all types.
     */
    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void testAlterColumnTypeScaleIsRejected(ColumnType type) {
        ColumnParams pkCol = columnParams("ID", INT32);
        Builder colWithPrecisionBuilder = columnParamsBuilder("COL_" + type, type).scale(3);
        ColumnParams col;

        applyNecessaryPrecision(type, colWithPrecisionBuilder);
        applyNecessaryLength(type, colWithPrecisionBuilder);

        if (!type.scaleAllowed()) {
            assertThrowsWithCause(colWithPrecisionBuilder::build, CatalogValidationException.class);
            return;
        }

        col = colWithPrecisionBuilder.build();

        assertThat(manager.execute(simpleTable(TABLE_NAME, List.of(pkCol, col))), willCompleteSuccessfully());

        int catalogVersion = manager.latestCatalogVersion();

        // ANY-> UNDEFINED SCALE : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willCompleteSuccessfully());
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // 3 -> 3 : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 3), null, null),
                willCompleteSuccessfully());
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // 3 -> 4 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 4), null, null),
                willThrowFast(CatalogValidationException.class, "Changing the scale for column of type"));
        assertEquals(catalogVersion, manager.latestCatalogVersion());

        // 3 -> 2 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 2), null, null),
                willThrowFast(CatalogValidationException.class, "Changing the scale for column of type"));
        assertEquals(catalogVersion, manager.latestCatalogVersion());
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
    @EnumSource(value = ColumnType.class, names = {"NULL", "PERIOD", "DURATION"}, mode = Mode.EXCLUDE)
    public void testAlterColumnType(ColumnType target) {
        EnumSet<ColumnType> types = EnumSet.allOf(ColumnType.class);
        types.remove(NULL);
        types.remove(PERIOD);
        types.remove(DURATION);

        List<ColumnParams> testColumns = types.stream()
                .map(t -> initializeColumnWithDefaults(t, columnParamsBuilder("COL_" + t, t)))
                .map(Builder::build)
                .collect(toList());

        List<ColumnParams> tableColumns = new ArrayList<>(List.of(columnParams("ID", INT32)));
        tableColumns.addAll(testColumns);

        assertThat(manager.execute(simpleTable(TABLE_NAME, tableColumns)), willCompleteSuccessfully());

        int catalogVersion = manager.latestCatalogVersion();

        for (ColumnParams col : testColumns) {
            TypeSafeMatcher<CompletableFuture<?>> matcher;
            boolean sameType = col.type() == target;

            if (sameType || CatalogUtils.isSupportedColumnTypeChange(col.type(), target)) {
                matcher = willCompleteSuccessfully();
                catalogVersion += sameType ? 0 : 1;
            } else {
                matcher = willThrowFast(CatalogValidationException.class,
                        format("Changing the type from {} to {} is not allowed", col.type(), target));
            }

            TestColumnTypeParams typeParams = new TestColumnTypeParams(target);

            assertThat(col.type() + " -> " + target, changeColumn(TABLE_NAME, col.name(), typeParams, null, null), matcher);
            assertEquals(catalogVersion, manager.latestCatalogVersion());
        }
    }

    @Test
    public void testAlterColumnTypeRejectedForPrimaryKey() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(changeColumn(TABLE_NAME, "ID", new TestColumnTypeParams(INT64), null, null),
                willThrowFast(CatalogValidationException.class, "Changing the type of key column is not allowed"));
    }

    /**
     * Ensures that the compound change command {@code SET DATA TYPE BIGINT NULL DEFAULT NULL} will change the type, drop NOT NULL and the
     * default value at the same time.
     */
    @Test
    public void testAlterColumnMultipleChanges() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        Supplier<DefaultValue> dflt = () -> constant(null);
        boolean notNull = false;
        TestColumnTypeParams typeParams = new TestColumnTypeParams(INT64);

        int catalogVersion = manager.latestCatalogVersion();

        // Ensures that 3 different actions applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willCompleteSuccessfully());
        catalogVersion++;

        assertEquals(catalogVersion, manager.latestCatalogVersion());

        CatalogTableColumnDescriptor desc = actualTable(TABLE_NAME).column("VAL_NOT_NULL");
        assertEquals(constant(null), desc.defaultValue());
        assertTrue(desc.nullable());
        assertEquals(INT64, desc.type());

        // Ensures that only one of three actions applied.
        dflt = () -> constant(2);
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willCompleteSuccessfully());
        catalogVersion++;

        assertEquals(catalogVersion, manager.latestCatalogVersion());
        assertEquals(constant(2), actualTable(TABLE_NAME).column("VAL_NOT_NULL").defaultValue());

        // Ensures that no action will be applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willCompleteSuccessfully());

        assertEquals(catalogVersion, manager.latestCatalogVersion());
    }

    @Test
    public void testAlterColumnForNonExistingTableRejected() {
        int versionBefore = manager.latestCatalogVersion();

        assertThat(changeColumn(TABLE_NAME, "ID", null, null, null),
                willThrowFast(CatalogValidationException.class, "Table with name 'PUBLIC.test_table' not found."));

        int versionAfter = manager.latestCatalogVersion();

        assertEquals(versionBefore, versionAfter);
    }

    @Test
    public void testFunctionalDefaultTypeMismatch() {
        ColumnParams key1 = ColumnParams.builder()
                .name("key1")
                .type(STRING)
                .length(100)
                .defaultValue(DefaultValue.functionCall("RAND_UUID"))
                .build();

        CreateTableCommandBuilder commandBuilder = CreateTableCommand.builder()
                .tableName(TABLE_NAME)
                .schemaName(SCHEMA_NAME)
                .columns(List.of(key1, columnParams("key2", INT32)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1")).build());

        String error = "[col=key1, functionName=RAND_UUID, expectedType=UUID, actualType=STRING]";
        assertThrows(CatalogValidationException.class, commandBuilder::build, error);
    }

    @Test
    public void testCreateTablesWithinDifferentZones() {
        // Create a new table in the default distribution zone.
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        // Create an additional distribution zone.
        String customZoneName = "TEST_ZONE_NAME";

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(customZoneName)
                .partitions(42)
                .replicas(15)
                .filter("expression")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile("test_profile").build()))
                .build();

        tryApplyAndExpectApplied(cmd);

        String customTableName = "test_table_zone";
        CatalogCommand createTableCmd = createTableCommandBuilder(
                SqlCommon.DEFAULT_SCHEMA_NAME,
                customTableName,
                List.of(columnParams("ID", INT32), columnParams("VAL", INT32)),
                List.of("ID"),
                null)
                .zone(customZoneName)
                .build();

        tryApplyAndExpectApplied(createTableCmd);

        Catalog catalog = manager.latestCatalog();

        CatalogZoneDescriptor defaultZoneDescriptor = catalog.zone(DEFAULT_ZONE_NAME);
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(customZoneName);
        assertNotNull(defaultZoneDescriptor);
        assertNotNull(zoneDescriptor);

        assertThat(catalog.tables(), hasSize(2));

        int tableId = catalog.table(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME).id();
        int customTableId = catalog.table(SqlCommon.DEFAULT_SCHEMA_NAME, customTableName).id();

        assertThat(catalog.tables(defaultZoneDescriptor.id()), hasSize(1));
        assertThat(catalog.tables(defaultZoneDescriptor.id()).stream().map(d -> d.id()).collect(toList()), hasItem(tableId));

        assertThat(catalog.tables(zoneDescriptor.id()), hasSize(1));
        assertThat(catalog.tables(zoneDescriptor.id()).stream().map(d -> d.id()).collect(toList()), hasItem(customTableId));
    }

    @Test
    void createTableWithStalenessConfiguration() {
        {
            CatalogCommand tableCmdWithDefault = createTableCommandBuilder(
                    SqlCommon.DEFAULT_SCHEMA_NAME,
                    "defaults",
                    List.of(columnParams("ID", INT32), columnParams("VAL", INT32)),
                    List.of("ID"),
                    null)
                    .build();
            tryApplyAndExpectApplied(tableCmdWithDefault);
        }

        {
            CatalogCommand tableCmdWithOnlyRowFraction = createTableCommandBuilder(
                    SqlCommon.DEFAULT_SCHEMA_NAME,
                    "stale_rows",
                    List.of(columnParams("ID", INT32), columnParams("VAL", INT32)),
                    List.of("ID"),
                    null)
                    .staleRowsFraction(0.8)
                    .build();
            tryApplyAndExpectApplied(tableCmdWithOnlyRowFraction);
        }

        {
            CatalogCommand tableCmdWithOnlyMinRowCount = createTableCommandBuilder(
                    SqlCommon.DEFAULT_SCHEMA_NAME,
                    "min_rows",
                    List.of(columnParams("ID", INT32), columnParams("VAL", INT32)),
                    List.of("ID"),
                    null)
                    .minStaleRowsCount(2000L)
                    .build();
            tryApplyAndExpectApplied(tableCmdWithOnlyMinRowCount);
        }

        {
            CatalogCommand tableCmdWithEverything = createTableCommandBuilder(
                    SqlCommon.DEFAULT_SCHEMA_NAME,
                    "everything",
                    List.of(columnParams("ID", INT32), columnParams("VAL", INT32)),
                    List.of("ID"),
                    null)
                    .minStaleRowsCount(4000L)
                    .staleRowsFraction(0.5)
                    .build();
            tryApplyAndExpectApplied(tableCmdWithEverything);
        }

        assertThat(manager.latestCatalog().tables(), hasItems(
                tableThatSatisfies("table with stale rows conf that matches defaults", d -> 
                        "defaults".equals(d.name())
                                && d.properties().minStaleRowsCount() == CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT
                                && compare(d.properties().staleRowsFraction(), CatalogUtils.DEFAULT_STALE_ROWS_FRACTION) == 0),
                tableThatSatisfies("table with non-default stale rows fraction conf", d ->
                        "stale_rows".equals(d.name())
                                && d.properties().minStaleRowsCount() == CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT
                                && compare(d.properties().staleRowsFraction(), 0.8) == 0),
                tableThatSatisfies("table with non-default min stale rows conf", d ->
                        "min_rows".equals(d.name())
                                && d.properties().minStaleRowsCount() == 2000L
                                && compare(d.properties().staleRowsFraction(), CatalogUtils.DEFAULT_STALE_ROWS_FRACTION) == 0),
                tableThatSatisfies("table with non-deafult stale rows con", d ->
                        "everything".equals(d.name())
                                && d.properties().minStaleRowsCount() == 4000L
                                && compare(d.properties().staleRowsFraction(), 0.5) == 0)
        ));
    }

    @Test
    void alterTableStalenessConfiguration() {
        { // create table with default row staleness configuration
            CatalogCommand tableCmdWithDefault = createTableCommandBuilder(
                    SqlCommon.DEFAULT_SCHEMA_NAME,
                    TABLE_NAME,
                    List.of(columnParams("ID", INT32), columnParams("VAL", INT32)),
                    List.of("ID"),
                    null)
                    .build();
            tryApplyAndExpectApplied(tableCmdWithDefault);

            assertThat(
                    actualTable(TABLE_NAME),
                    tableThatSatisfies("table with default configuration", d ->
                            d.properties().minStaleRowsCount() == CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT
                                    && compare(d.properties().staleRowsFraction(), CatalogUtils.DEFAULT_STALE_ROWS_FRACTION) == 0)
            );
        }

        { // let's change row fraction first
            CatalogCommand tableCmdWithDefault = AlterTableSetPropertyCommand.builder()
                    .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                    .tableName(TABLE_NAME)
                    .staleRowsFraction(CatalogUtils.DEFAULT_STALE_ROWS_FRACTION * 2)
                    .build();
            tryApplyAndExpectApplied(tableCmdWithDefault);

            assertThat(
                    actualTable(TABLE_NAME),
                    tableThatSatisfies("table with default configuration", d ->
                            d.properties().minStaleRowsCount() == CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT
                                    && compare(d.properties().staleRowsFraction(), CatalogUtils.DEFAULT_STALE_ROWS_FRACTION * 2) == 0)
            );
        }

        { // now let's change min rows count
            CatalogCommand tableCmdWithDefault = AlterTableSetPropertyCommand.builder()
                    .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                    .tableName(TABLE_NAME)
                    .minStaleRowsCount(CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT * 2)
                    .build();
            tryApplyAndExpectApplied(tableCmdWithDefault);

            assertThat(
                    actualTable(TABLE_NAME),
                    tableThatSatisfies("table with default configuration", d ->
                            d.properties().minStaleRowsCount() == CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT * 2
                                    && compare(d.properties().staleRowsFraction(), CatalogUtils.DEFAULT_STALE_ROWS_FRACTION * 2) == 0)
            );
        }

        { // finally, let's change both back to defaults
            CatalogCommand tableCmdWithDefault = AlterTableSetPropertyCommand.builder()
                    .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                    .tableName(TABLE_NAME)
                    .minStaleRowsCount(CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT)
                    .staleRowsFraction(CatalogUtils.DEFAULT_STALE_ROWS_FRACTION)
                    .build();
            tryApplyAndExpectApplied(tableCmdWithDefault);

            assertThat(
                    actualTable(TABLE_NAME),
                    tableThatSatisfies("table with default configuration", d ->
                            d.properties().minStaleRowsCount() == CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT
                                    && compare(d.properties().staleRowsFraction(), CatalogUtils.DEFAULT_STALE_ROWS_FRACTION) == 0)
            );
        }
    }

    private static BaseMatcher<CatalogTableDescriptor> tableThatSatisfies(String description, Predicate<CatalogTableDescriptor> predicate) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object o) {
                return o instanceof CatalogTableDescriptor && predicate.test((CatalogTableDescriptor) o);
            }

            @Override
            public void describeTo(Description description0) {
                description0.appendText(description);
            }
        };
    }

    private CompletableFuture<CatalogApplyResult> changeColumn(
            String tab,
            String col,
            @Nullable TestColumnTypeParams typeParams,
            @Nullable Boolean notNull,
            @Nullable Supplier<DefaultValue> dflt
    ) {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(tab)
                .columnName(col);

        if (notNull != null) {
            builder.nullable(!notNull);
        }

        if (dflt != null) {
            builder.deferredDefaultValue(ignore -> dflt.get());
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

        return manager.execute(builder.build());
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

    private @Nullable CatalogTableDescriptor actualTable(String tableName) {
        return manager.activeCatalog(clock.nowLong()).table(SCHEMA_NAME, tableName);
    }

    private @Nullable CatalogTableDescriptor table(int catalogVersion, String tableName) {
        return manager.catalog(catalogVersion).table(SCHEMA_NAME, tableName);
    }

    private static void checkDefaultZoneProperties(CatalogZoneDescriptor defaultZoneDescriptor) {
        assertThat(defaultZoneDescriptor.name(), is(DEFAULT_ZONE_NAME));
        assertThat(defaultZoneDescriptor.partitions(), is(DEFAULT_PARTITION_COUNT));
        assertThat(defaultZoneDescriptor.replicas(), is(DEFAULT_REPLICA_COUNT));
        assertThat(defaultZoneDescriptor.quorumSize(), is(DEFAULT_ZONE_QUORUM_SIZE));
        assertThat(defaultZoneDescriptor.dataNodesAutoAdjustScaleUp(), is(IMMEDIATE_TIMER_VALUE));
        assertThat(defaultZoneDescriptor.dataNodesAutoAdjustScaleDown(), is(INFINITE_TIMER_VALUE));
        assertThat(defaultZoneDescriptor.filter(), is(DEFAULT_FILTER));
        assertThat(
                defaultZoneDescriptor.storageProfiles().profiles(),
                contains(new CatalogStorageProfileDescriptor(DEFAULT_STORAGE_PROFILE))
        );
        assertThat(defaultZoneDescriptor.consistencyMode(), is(STRONG_CONSISTENCY));
    }
}
