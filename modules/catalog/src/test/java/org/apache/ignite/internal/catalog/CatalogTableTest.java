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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.addColumnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.applyNecessaryLength;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.applyNecessaryPrecision;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParamsBuilder;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.dropColumnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.initializeColumnWithDefaults;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PRECISION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_SCALE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.commands.DefaultValue.constant;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.apache.ignite.sql.ColumnType.NULL;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommandBuilder;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.RenameTableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
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
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

/** Tests for table related commands. */
public class CatalogTableTest extends BaseCatalogManagerTest {

    private static final String SCHEMA_NAME = DEFAULT_SCHEMA_NAME;
    private static final String NEW_COLUMN_NAME = "NEWCOL";
    private static final String NEW_COLUMN_NAME_2 = "NEWCOL2";
    private static final int DFLT_TEST_PRECISION = 11;

    @Test
    public void testCreateTable() {
        long timePriorToTableCreation = clock.nowLong();

        int tableCreationVersion = await(
                manager.execute(createTableCommand(
                        TABLE_NAME,
                        List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)),
                        List.of("key1", "key2"),
                        List.of("key2")
                ))
        );

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(tableCreationVersion - 1);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(timePriorToTableCreation));

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, 123L));
        assertNull(manager.aliveIndex(pkIndexName(TABLE_NAME), 123L));

        // Validate actual catalog
        schema = manager.schema(SCHEMA_NAME, tableCreationVersion);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        CatalogHashIndexDescriptor pkIndex = (CatalogHashIndexDescriptor) schema.aliveIndex(pkIndexName(TABLE_NAME));

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertSame(table, manager.table(TABLE_NAME, clock.nowLong()));
        assertSame(table, manager.table(table.id(), clock.nowLong()));

        assertSame(pkIndex, manager.aliveIndex(pkIndexName(TABLE_NAME), clock.nowLong()));
        assertSame(pkIndex, manager.index(pkIndex.id(), clock.nowLong()));

        // Validate newly created table
        assertEquals(TABLE_NAME, table.name());

        CatalogZoneDescriptor defaultZone = latestActiveCatalog().defaultZone();

        assertEquals(defaultZone.id(), table.zoneId());

        // Validate newly created pk index
        assertEquals(pkIndexName(TABLE_NAME), pkIndex.name());
        assertEquals(table.id(), pkIndex.tableId());
        assertEquals(table.primaryKeyColumns(), pkIndex.columns());
        assertTrue(pkIndex.unique());
        assertEquals(AVAILABLE, pkIndex.status());
        assertEquals(manager.latestCatalogVersion(), pkIndex.txWaitCatalogVersion());

        CatalogTableColumnDescriptor desc = table.columnDescriptor("key1");
        assertNotNull(desc);
        // INT32 key
        assertThat(desc.precision(), is(DEFAULT_PRECISION));

        // Validate another table creation.
        int secondTableCreationVersion = await(manager.execute(simpleTable(TABLE_NAME_2)));

        // Validate actual catalog has both tables.
        schema = manager.schema(secondTableCreationVersion);
        table = schema.table(TABLE_NAME);
        pkIndex = (CatalogHashIndexDescriptor) schema.aliveIndex(pkIndexName(TABLE_NAME));
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);
        CatalogHashIndexDescriptor pkIndex2 = (CatalogHashIndexDescriptor) schema.aliveIndex(pkIndexName(TABLE_NAME_2));

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertSame(table, manager.table(TABLE_NAME, clock.nowLong()));
        assertSame(table, manager.table(table.id(), clock.nowLong()));

        assertSame(pkIndex, manager.aliveIndex(pkIndexName(TABLE_NAME), clock.nowLong()));
        assertSame(pkIndex, manager.index(pkIndex.id(), clock.nowLong()));

        assertSame(table2, manager.table(TABLE_NAME_2, clock.nowLong()));
        assertSame(table2, manager.table(table2.id(), clock.nowLong()));

        assertSame(pkIndex2, manager.aliveIndex(pkIndexName(TABLE_NAME_2), clock.nowLong()));
        assertSame(pkIndex2, manager.index(pkIndex2.id(), clock.nowLong()));

        assertNotSame(table, table2);
        assertNotSame(pkIndex, pkIndex2);

        // Try to create another table with same name.
        assertThat(
                manager.execute(simpleTable(TABLE_NAME_2)),
                willThrowFast(CatalogValidationException.class)
        );

        // Validate schema wasn't changed.
        assertSame(schema, manager.activeSchema(clock.nowLong()));
    }


    @Test
    public void testDropTable() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.execute(simpleTable(TABLE_NAME_2)), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        int tableDropVersion = await(manager.execute(dropTableCommand(TABLE_NAME)));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(tableDropVersion - 1);
        CatalogTableDescriptor table1 = schema.table(TABLE_NAME);
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);
        CatalogIndexDescriptor pkIndex1 = schema.aliveIndex(pkIndexName(TABLE_NAME));
        CatalogIndexDescriptor pkIndex2 = schema.aliveIndex(pkIndexName(TABLE_NAME_2));

        assertNotEquals(table1.id(), table2.id());
        assertNotEquals(pkIndex1.id(), pkIndex2.id());

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(beforeDropTimestamp));

        assertSame(table1, manager.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(table1, manager.table(table1.id(), beforeDropTimestamp));

        assertSame(pkIndex1, manager.aliveIndex(pkIndexName(TABLE_NAME), beforeDropTimestamp));
        assertSame(pkIndex1, manager.index(pkIndex1.id(), beforeDropTimestamp));

        assertSame(table2, manager.table(TABLE_NAME_2, beforeDropTimestamp));
        assertSame(table2, manager.table(table2.id(), beforeDropTimestamp));

        assertSame(pkIndex2, manager.aliveIndex(pkIndexName(TABLE_NAME_2), beforeDropTimestamp));
        assertSame(pkIndex2, manager.index(pkIndex2.id(), beforeDropTimestamp));

        // Validate actual catalog
        schema = manager.schema(tableDropVersion);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, clock.nowLong()));
        assertNull(manager.table(table1.id(), clock.nowLong()));

        assertThat(schema.aliveIndex(pkIndexName(TABLE_NAME)), is(nullValue()));
        assertThat(manager.aliveIndex(pkIndexName(TABLE_NAME), clock.nowLong()), is(nullValue()));
        assertThat(manager.index(pkIndex1.id(), clock.nowLong()), is(nullValue()));

        assertSame(table2, manager.table(TABLE_NAME_2, clock.nowLong()));
        assertSame(table2, manager.table(table2.id(), clock.nowLong()));

        assertSame(pkIndex2, manager.aliveIndex(pkIndexName(TABLE_NAME_2), clock.nowLong()));
        assertSame(pkIndex2, manager.index(pkIndex2.id(), clock.nowLong()));

        // Validate schema wasn't changed.
        assertSame(schema, manager.activeSchema(clock.nowLong()));
    }


    @Test
    public void testReCreateTableWithSameName() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        int catalogVersion = manager.latestCatalogVersion();
        CatalogTableDescriptor table1 = manager.table(TABLE_NAME, clock.nowLong());
        assertNotNull(table1);

        // Drop table.
        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willCompleteSuccessfully());
        assertNull(manager.table(TABLE_NAME, clock.nowLong()));

        // Re-create table with same name.
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        CatalogTableDescriptor table2 = manager.table(TABLE_NAME, clock.nowLong());
        assertNotNull(table2);

        // Ensure these are different tables.
        assertNotEquals(table1.id(), table2.id());

        // Ensure table is available for historical queries.
        assertNotNull(manager.table(table1.id(), catalogVersion));
    }

    @Test
    public void testAddColumn() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(
                manager.execute(addColumnParams(TABLE_NAME,
                        columnParamsBuilder(NEW_COLUMN_NAME, STRING, 11, true).defaultValue(constant("Ignite!")).build()
                )),
                willCompleteSuccessfully()
        );

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
        assertEquals(STRING, column.type());
        assertTrue(column.nullable());

        assertEquals(DefaultValue.Type.CONSTANT, column.defaultValue().type());
        assertEquals("Ignite!", ((DefaultValue.ConstantValue) column.defaultValue()).value());

        assertEquals(11, column.length());
        assertEquals(DEFAULT_PRECISION, column.precision());
        assertEquals(DEFAULT_SCALE, column.scale());
    }

    @Test
    public void testDropColumn() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "VAL")), willCompleteSuccessfully());

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
    public void testAddDropMultipleColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        // Add duplicate column.
        assertThat(
                manager.execute(addColumnParams(TABLE_NAME, columnParams(NEW_COLUMN_NAME, INT32, true), columnParams("VAL", INT32, true))),
                willThrow(CatalogValidationException.class)
        );

        // Validate no column added.
        CatalogSchemaDescriptor schema = manager.activeSchema(clock.nowLong());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));

        // Add multiple columns.
        assertThat(
                manager.execute(addColumnParams(TABLE_NAME,
                        columnParams(NEW_COLUMN_NAME, INT32, true), columnParams(NEW_COLUMN_NAME_2, INT32, true)
                )),
                willCompleteSuccessfully()
        );

        // Validate both columns added.
        schema = manager.activeSchema(clock.nowLong());

        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));

        // Drop multiple columns.
        assertThat(manager.execute(dropColumnParams(TABLE_NAME, NEW_COLUMN_NAME, NEW_COLUMN_NAME_2)), willCompleteSuccessfully());

        // Validate both columns dropped.
        schema = manager.activeSchema(clock.nowLong());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));
    }

    @Test
    public void testDropNotExistingTable() {
        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willThrowFast(CatalogValidationException.class));
    }

    @Test
    public void testDropColumnWithNotExistingTable() {
        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "key")), willThrowFast(TableNotFoundValidationException.class));
    }

    @Test
    public void testDropColumnWithMissingTableColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "fake")), willThrowFast(CatalogValidationException.class));
    }

    @Test
    public void testDropColumnWithPrimaryKeyColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.execute(dropColumnParams(TABLE_NAME, "ID")),
                willThrowFast(CatalogValidationException.class, "Deleting column `ID` belonging to primary key is not allowed")
        );
    }

    @Test
    public void testDropColumnWithIndexColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.execute(simpleIndex()), willCompleteSuccessfully());

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
        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams("key", INT32))),
                willThrowFast(TableNotFoundValidationException.class));
    }

    @Test
    public void testAddColumnWithExistingName() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams("ID", INT32))),
                willThrowFast(CatalogValidationException.class));
    }

    @Test
    public void testTableEvents() {
        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any())).thenReturn(falseCompletedFuture());

        manager.listen(CatalogEvent.TABLE_CREATE, eventListener);
        manager.listen(CatalogEvent.TABLE_DROP, eventListener);

        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.execute(simpleTable(TABLE_NAME_2)), willCompleteSuccessfully());
        assertThat(manager.execute(simpleTable(TABLE_NAME_3)), willCompleteSuccessfully());
        verify(eventListener, times(3)).notify(any(CreateTableEventParameters.class));

        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.execute(dropTableCommand(TABLE_NAME_2)), willCompleteSuccessfully());
        verify(eventListener, times(2)).notify(any(DropTableEventParameters.class));

        verifyNoMoreInteractions(eventListener);
        clearInvocations(eventListener);
    }

    @Test
    public void testTables() {
        int initialVersion = manager.latestCatalogVersion();

        assertThat(manager.execute(simpleTable(TABLE_NAME + 0)), willCompleteSuccessfully());
        assertThat(manager.execute(simpleTable(TABLE_NAME + 1)), willCompleteSuccessfully());

        assertThat(manager.tables(initialVersion), empty());
        assertThat(
                manager.tables(initialVersion + 1),
                hasItems(table(initialVersion + 1, TABLE_NAME + 0))
        );
        assertThat(
                manager.tables(initialVersion + 2),
                hasItems(table(initialVersion + 2, TABLE_NAME + 0), table(initialVersion + 2, TABLE_NAME + 1))
        );
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

        assertThat(manager.execute(command), willCompleteSuccessfully());

        int curVersion = manager.latestCatalogVersion();

        CatalogTableDescriptor prevDescriptor = table(prevVersion, TABLE_NAME);
        CatalogTableDescriptor curDescriptor = table(curVersion, TABLE_NAME_2);

        assertThat(prevDescriptor, is(notNullValue()));
        assertThat(prevDescriptor.name(), is(TABLE_NAME));

        assertThat(curDescriptor, is(notNullValue()));
        assertThat(curDescriptor.name(), is(TABLE_NAME_2));

        assertThat(table(prevVersion, TABLE_NAME_2), is(nullValue()));
        assertThat(table(curVersion, TABLE_NAME), is(nullValue()));

        assertThat(curDescriptor.tableVersion(), is(prevDescriptor.tableVersion() + 1));

        // Assert that all other properties have been left intact.
        assertThat(curDescriptor.id(), is(prevDescriptor.id()));
        assertThat(curDescriptor.columns(), is(prevDescriptor.columns()));
        assertThat(curDescriptor.colocationColumns(), is(prevDescriptor.colocationColumns()));
        assertThat(curDescriptor.creationToken(), is(prevDescriptor.creationToken()));
        assertThat(curDescriptor.primaryKeyColumns(), is(prevDescriptor.primaryKeyColumns()));
        assertThat(curDescriptor.primaryKeyIndexId(), is(prevDescriptor.primaryKeyIndexId()));
        assertThat(curDescriptor.schemaId(), is(prevDescriptor.schemaId()));
    }

    @Test
    public void testTableRenameAndCreateTableWithSameName() {
        createSomeTable(TABLE_NAME);

        CatalogCommand command = RenameTableCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .newTableName(TABLE_NAME_2)
                .build();

        assertThat(manager.execute(command), willCompleteSuccessfully());

        createSomeTable(TABLE_NAME);

        int catalogVersion = manager.latestCatalogVersion();

        assertThat(table(catalogVersion, TABLE_NAME), is(notNullValue()));
        assertThat(table(catalogVersion, TABLE_NAME_2), is(notNullValue()));
    }

    @Test
    public void addColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams("val2", INT32))), willCompleteSuccessfully());

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));
    }

    @Test
    public void createTableProducesTableVersion1() {
        createSomeTable(TABLE_NAME);

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(1));
    }

    @Test
    public void dropColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "val1")), willCompleteSuccessfully());

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));
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

        assertThat(manager.execute(command), willCompleteSuccessfully());
        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    public void testColumnEvents() {
        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any())).thenReturn(falseCompletedFuture());

        manager.listen(CatalogEvent.TABLE_ALTER, eventListener);

        // Try to add column without table.
        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams(NEW_COLUMN_NAME, INT32))),
                willThrow(TableNotFoundValidationException.class));
        verifyNoInteractions(eventListener);

        // Create table.
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        // Add column.
        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams(NEW_COLUMN_NAME, INT32))), willCompleteSuccessfully());
        verify(eventListener).notify(any(AddColumnEventParameters.class));

        // Drop column.
        assertThat(manager.execute(dropColumnParams(TABLE_NAME, NEW_COLUMN_NAME)), willCompleteSuccessfully());
        verify(eventListener).notify(any(DropColumnEventParameters.class));

        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testGetCatalogEntityInCatalogEvent() {
        var fireEventFuture = new CompletableFuture<Void>();

        manager.listen(CatalogEvent.TABLE_CREATE, fromConsumer(fireEventFuture, parameters -> {
            assertNotNull(manager.schema(parameters.catalogVersion()));
        }));

        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    public void testGetTableByIdAndCatalogVersion() {
        int tableCreationVersion = await(manager.execute(simpleTable(TABLE_NAME)));

        CatalogTableDescriptor table = manager.table(TABLE_NAME, clock.nowLong());

        assertNull(manager.table(table.id(), tableCreationVersion - 1));
        assertSame(table, manager.table(table.id(), tableCreationVersion));
    }

    @Test
    public void alterColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        CompletableFuture<?> future = manager.execute(
                AlterTableAlterColumnCommand.builder()
                        .schemaName(SCHEMA_NAME)
                        .tableName(TABLE_NAME)
                        .columnName("val1")
                        .type(INT64)
                        .build()
        );
        assertThat(future, willCompleteSuccessfully());

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));
    }

    /**
     * Checks for possible changes to the default value of a column descriptor.
     *
     * <p>Set/drop default value allowed for any column.
     */
    @Test
    public void testAlterColumnDefault() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        int schemaVer = manager.latestCatalogVersion();
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // NULL-> NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(null)),
                willCompleteSuccessfully());
        assertNull(manager.schema(schemaVer + 1));

        // NULL -> 1 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(1)),
                willCompleteSuccessfully());
        assertNotNull(manager.schema(++schemaVer));

        // 1 -> 1 : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(1)),
                willCompleteSuccessfully());
        assertNull(manager.schema(schemaVer + 1));

        // 1 -> 2 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(2)),
                willCompleteSuccessfully());
        assertNotNull(manager.schema(++schemaVer));

        // 2 -> NULL : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(null)),
                willCompleteSuccessfully());
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
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        int schemaVer = manager.latestCatalogVersion();
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // NULLABLE -> NULLABLE : No-op.
        // NOT NULL -> NOT NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, false, null), willCompleteSuccessfully());
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null), willCompleteSuccessfully());
        assertNull(manager.schema(schemaVer + 1));

        // NOT NULL -> NULlABLE : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, false, null), willCompleteSuccessfully());
        assertNotNull(manager.schema(++schemaVer));

        // DROP NOT NULL for PK : PK column can't be `null`.
        assertThat(changeColumn(TABLE_NAME, "ID", null, false, null),
                willThrowFast(CatalogValidationException.class, "Dropping NOT NULL constraint on key column is not allowed"));

        // NULlABLE -> NOT NULL : Forbidden because this change lead to incompatible schemas.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, true, null),
                willThrowFast(CatalogValidationException.class, "Adding NOT NULL constraint is not allowed"));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null),
                willThrowFast(CatalogValidationException.class, "Adding NOT NULL constraint is not allowed"));

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
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col1 = columnParamsBuilder("COL_DECIMAL1", DECIMAL).precision(DFLT_TEST_PRECISION - 1).scale(1).build();
        ColumnParams col2 = columnParamsBuilder("COL_DECIMAL2", DECIMAL).precision(DFLT_TEST_PRECISION).scale(1).build();

        assertThat(manager.execute(simpleTable(TABLE_NAME, List.of(pkCol, col1, col2))), willCompleteSuccessfully());

        int schemaVer = manager.latestCatalogVersion();
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // precision increment : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col1.name(), new TestColumnTypeParams(col1.type(), DFLT_TEST_PRECISION, null, null), null, null),
                willCompleteSuccessfully()
        );
        assertNotNull(manager.schema(++schemaVer));

        assertThat(
                changeColumn(TABLE_NAME, col2.name(), new TestColumnTypeParams(col2.type(), DFLT_TEST_PRECISION, null, null), null, null),
                willCompleteSuccessfully()
        );
        assertNull(manager.schema(schemaVer + 1));

        // No change.
        assertThat(
                changeColumn(TABLE_NAME, col1.name(), new TestColumnTypeParams(col1.type(), DFLT_TEST_PRECISION, null, null), null, null),
                willCompleteSuccessfully()
        );
        assertNull(manager.schema(schemaVer + 1));

        // precision decrement : Forbidden because this change lead to incompatible schemas.
        assertThat(
                changeColumn(TABLE_NAME, col1.name(),
                        new TestColumnTypeParams(col1.type(), DFLT_TEST_PRECISION - 1, null, null), null, null),
                willThrowFast(CatalogValidationException.class, "Decreasing the precision for column of type '"
                        + col1.type() + "' is not allowed")
        );
        assertNull(manager.schema(schemaVer + 1));

        assertThat(
                changeColumn(TABLE_NAME, col2.name(),
                        new TestColumnTypeParams(col2.type(), DFLT_TEST_PRECISION - 1, null, null), null, null),
                willThrowFast(CatalogValidationException.class, "Decreasing the precision for column of type '"
                        + col1.type() + "' is not allowed")
        );
        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Changing precision is not supported for all types other than DECIMAL.
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "DECIMAL"}, mode = Mode.EXCLUDE)
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

        assertThat(manager.execute(
                simpleTable(TABLE_NAME, List.of(pkCol, colWithPrecision))), willCompleteSuccessfully()
        );

        int schemaVer = manager.latestCatalogVersion();
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

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
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col = columnParamsBuilder("COL_" + type, type).length(10).build();

        assertThat(manager.execute(simpleTable(TABLE_NAME, List.of(pkCol, col))), willCompleteSuccessfully());

        int schemaVer = manager.latestCatalogVersion();
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // 10 -> 11 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 11, null), null, null),
                willCompleteSuccessfully()
        );

        CatalogSchemaDescriptor schema = manager.schema(++schemaVer);
        assertNotNull(schema);

        // 11 -> 10 : Error.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 10, null), null, null),
                willThrowFast(CatalogValidationException.class, "Decreasing the length for column of type '"
                        + col.type() + "' is not allowed")
        );
        assertNull(manager.schema(schemaVer + 1));

        // 11 -> 11 : No-op.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 11, null), null, null),
                willCompleteSuccessfully()
        );
        assertNull(manager.schema(schemaVer + 1));

        // No change.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willCompleteSuccessfully());
        assertNull(manager.schema(schemaVer + 1));

        // 11 -> 10 : failed.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 10, null), null, null),
                willThrowFast(CatalogValidationException.class)
        );
        assertNull(manager.schema(schemaVer + 1));
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

        int schemaVer = manager.latestCatalogVersion();
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

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

        assertNull(manager.schema(schemaVer + 1));
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

        int schemaVer = manager.latestCatalogVersion();
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // ANY-> UNDEFINED SCALE : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willCompleteSuccessfully());
        assertNull(manager.schema(schemaVer + 1));

        // 3 -> 3 : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 3), null, null),
                willCompleteSuccessfully());
        assertNull(manager.schema(schemaVer + 1));

        // 3 -> 4 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 4), null, null),
                willThrowFast(CatalogValidationException.class, "Changing the scale for column of type"));
        assertNull(manager.schema(schemaVer + 1));

        // 3 -> 2 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 2), null, null),
                willThrowFast(CatalogValidationException.class, "Changing the scale for column of type"));
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
        types.remove(NULL);

        List<ColumnParams> testColumns = types.stream()
                .map(t -> initializeColumnWithDefaults(t, columnParamsBuilder("COL_" + t, t)))
                .map(Builder::build)
                .collect(toList());

        List<ColumnParams> tableColumns = new ArrayList<>(List.of(columnParams("ID", INT32)));
        tableColumns.addAll(testColumns);

        assertThat(manager.execute(simpleTable(TABLE_NAME, tableColumns)), willCompleteSuccessfully());

        int schemaVer = manager.latestCatalogVersion();
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        for (ColumnParams col : testColumns) {
            TypeSafeMatcher<CompletableFuture<?>> matcher;
            boolean sameType = col.type() == target;

            if (sameType || CatalogUtils.isSupportedColumnTypeChange(col.type(), target)) {
                matcher = willCompleteSuccessfully();
                schemaVer += sameType ? 0 : 1;
            } else {
                matcher = willThrowFast(CatalogValidationException.class,
                        format("Changing the type from {} to {} is not allowed", col.type(), target));
            }

            TestColumnTypeParams typeParams = new TestColumnTypeParams(target);

            assertThat(col.type() + " -> " + target, changeColumn(TABLE_NAME, col.name(), typeParams, null, null), matcher);
            assertNotNull(manager.schema(schemaVer));
            assertNull(manager.schema(schemaVer + 1));
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

        int schemaVer = manager.latestCatalogVersion();
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        Supplier<DefaultValue> dflt = () -> constant(null);
        boolean notNull = false;
        TestColumnTypeParams typeParams = new TestColumnTypeParams(INT64);

        // Ensures that 3 different actions applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willCompleteSuccessfully());

        CatalogSchemaDescriptor schema = manager.schema(++schemaVer);
        assertNotNull(schema);

        CatalogTableColumnDescriptor desc = schema.table(TABLE_NAME).column("VAL_NOT_NULL");
        assertEquals(constant(null), desc.defaultValue());
        assertTrue(desc.nullable());
        assertEquals(INT64, desc.type());

        // Ensures that only one of three actions applied.
        dflt = () -> constant(2);
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willCompleteSuccessfully());

        schema = manager.schema(++schemaVer);
        assertNotNull(schema);
        assertEquals(constant(2), schema.table(TABLE_NAME).column("VAL_NOT_NULL").defaultValue());

        // Ensures that no action will be applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willCompleteSuccessfully());
        assertNull(manager.schema(schemaVer + 1));
    }

    @Test
    public void testAlterColumnForNonExistingTableRejected() {
        int versionBefore = manager.latestCatalogVersion();

        assertThat(changeColumn(TABLE_NAME, "ID", null, null, null), willThrowFast(TableNotFoundValidationException.class));

        int versionAfter = manager.latestCatalogVersion();

        assertEquals(versionBefore, versionAfter);
    }


    private CompletableFuture<?> changeColumn(
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

    private @Nullable CatalogTableDescriptor table(int catalogVersion, String tableName) {
        return manager.schema(catalogVersion).table(tableName);
    }
}
