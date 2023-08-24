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
import static java.util.stream.Collectors.toList;
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
import static org.apache.ignite.internal.catalog.commands.DefaultValue.constant;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.DESC_NULLS_FIRST;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.apache.ignite.sql.ColumnType.NULL;
import static org.apache.ignite.sql.ColumnType.STRING;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams.Builder;
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
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
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
import org.apache.ignite.lang.IgniteException;
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
    private static final String TABLE_NAME_2 = "myTable2";
    private static final String NEW_COLUMN_NAME = "NEWCOL";
    private static final String NEW_COLUMN_NAME_2 = "NEWCOL2";

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
        assertThat(
                manager.createTable(createTableParams(
                        TABLE_NAME,
                        List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)),
                        List.of("key1", "key2"),
                        List.of("key2")
                )),
                willBe(nullValue())
        );

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

        assertThat(manager.dropTable(dropTableParams(TABLE_NAME)), willBe(nullValue()));

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

        // Validate schema wasn't changed.
        assertSame(schema, manager.activeSchema(clock.nowLong()));
    }

    @Test
    public void testAddColumn() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(
                manager.addColumn(addColumnParams(
                        columnParamsBuilder(NEW_COLUMN_NAME, STRING, true).defaultValue(constant("Ignite!")).build()
                )),
                willBe(nullValue())
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

        assertEquals(DEFAULT_VARLEN_LENGTH, column.length());
        assertEquals(0, column.precision());
        assertEquals(0, column.scale());
    }

    @Test
    public void testDropColumn() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(manager.dropColumn(dropColumnParams("VAL")), willBe(nullValue()));

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
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        // Add duplicate column.
        assertThat(
                manager.addColumn(addColumnParams(columnParams(NEW_COLUMN_NAME, INT32, true), columnParams("VAL", INT32, true))),
                willThrow(ColumnAlreadyExistsException.class)
        );

        // Validate no column added.
        CatalogSchemaDescriptor schema = manager.activeSchema(clock.nowLong());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));

        // Add multiple columns.
        assertThat(
                manager.addColumn(addColumnParams(
                        columnParams(NEW_COLUMN_NAME, INT32, true), columnParams(NEW_COLUMN_NAME_2, INT32, true)
                )),
                willBe(nullValue())
        );

        // Validate both columns added.
        schema = manager.activeSchema(clock.nowLong());

        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));

        // Drop multiple columns.
        assertThat(manager.dropColumn(dropColumnParams(NEW_COLUMN_NAME, NEW_COLUMN_NAME_2)), willBe(nullValue()));

        // Validate both columns dropped.
        schema = manager.activeSchema(clock.nowLong());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));
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
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(null)),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // NULL -> 1 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(1)),
                willBe(nullValue()));
        assertNotNull(manager.schema(++schemaVer));

        // 1 -> 1 : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(1)),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // 1 -> 2 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(2)),
                willBe(nullValue()));
        assertNotNull(manager.schema(++schemaVer));

        // 2 -> NULL : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(null)),
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
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col = columnParamsBuilder("COL_DECIMAL", DECIMAL).precision(10).build();

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
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col = columnParams("COL", type);
        ColumnParams colWithPrecision = columnParamsBuilder("COL_PRECISION", type).precision(3).build();

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
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col = columnParamsBuilder("COL_" + type, type).length(10).build();

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
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col = columnParams("COL", type);
        ColumnParams colWithLength = columnParamsBuilder("COL_PRECISION", type).length(10).build();

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
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col = columnParamsBuilder("COL_" + type, type).scale(3).build();
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
        types.remove(NULL);

        List<ColumnParams> testColumns = types.stream()
                .map(t -> columnParams("COL_" + t, t))
                .collect(toList());

        List<ColumnParams> tableColumns = new ArrayList<>(List.of(columnParams("ID", INT32)));
        tableColumns.addAll(testColumns);

        assertThat(manager.createTable(simpleTable(TABLE_NAME, tableColumns)), willBe(nullValue()));

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

        assertThat(changeColumn(TABLE_NAME, "ID", new TestColumnTypeParams(INT64), null, null),
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

        Supplier<DefaultValue> dflt = () -> constant(null);
        boolean notNull = false;
        TestColumnTypeParams typeParams = new TestColumnTypeParams(INT64);

        // Ensures that 3 different actions applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));

        CatalogSchemaDescriptor schema = manager.schema(++schemaVer);
        assertNotNull(schema);

        CatalogTableColumnDescriptor desc = schema.table(TABLE_NAME).column("VAL_NOT_NULL");
        assertEquals(constant(null), desc.defaultValue());
        assertTrue(desc.nullable());
        assertEquals(INT64, desc.type());

        // Ensures that only one of three actions applied.
        dflt = () -> constant(2);
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));

        schema = manager.schema(++schemaVer);
        assertNotNull(schema);
        assertEquals(constant(2), schema.table(TABLE_NAME).column("VAL_NOT_NULL").defaultValue());

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
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(manager.createIndex(simpleIndex()), willBe(nullValue()));

        long beforeDropTimestamp = clock.nowLong();

        assertThat(manager.dropTable(dropTableParams(TABLE_NAME)), willBe(nullValue()));

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

        assertThat(manager.createIndex(createHashIndexParams(INDEX_NAME, List.of("VAL", "ID"))), willBe(nullValue()));

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

        CreateSortedIndexParams params = createSortedIndexParams(
                INDEX_NAME,
                true,
                List.of("VAL", "ID"),
                List.of(DESC_NULLS_FIRST, ASC_NULLS_LAST)
        );

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
        assertEquals(DESC_NULLS_FIRST, index.columns().get(0).collation());
        assertEquals(ASC_NULLS_LAST, index.columns().get(1).collation());
        assertTrue(index.unique());
        assertFalse(index.writeOnly());
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
        long delayDuration = TimeUnit.DAYS.toMillis(365);

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLog, clockWaiter, () -> delayDuration);

        manager.start();

        try {
            CompletableFuture<Void> createTableFuture = manager.createTable(simpleTable(TABLE_NAME));

            assertFalse(createTableFuture.isDone());

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
        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        manager.listen(CatalogEvent.TABLE_CREATE, eventListener);
        manager.listen(CatalogEvent.TABLE_DROP, eventListener);

        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        verify(eventListener).notify(any(CreateTableEventParameters.class), isNull());

        assertThat(manager.dropTable(dropTableParams(TABLE_NAME)), willBe(nullValue()));
        verify(eventListener).notify(any(DropTableEventParameters.class), isNull());

        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testCreateIndexEvents() {
        CreateHashIndexParams createIndexParams = createHashIndexParams(INDEX_NAME, List.of("ID"));

        DropIndexParams dropIndexParams = DropIndexParams.builder().indexName(INDEX_NAME).build();

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        manager.listen(CatalogEvent.INDEX_CREATE, eventListener);
        manager.listen(CatalogEvent.INDEX_DROP, eventListener);

        // Try to create index without table.
        assertThat(manager.createIndex(createIndexParams), willThrow(TableNotFoundException.class));
        verifyNoInteractions(eventListener);

        // Create table with PK index.
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
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
        assertThat(manager.dropTable(dropTableParams(TABLE_NAME)), willBe(nullValue()));

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
                .filter("expression")
                .build();

        AlterZoneParams alterZoneParams = AlterZoneParams.builder()
                .zoneName(zoneName)
                .partitions(10)
                .replicas(2)
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
        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        manager.listen(CatalogEvent.TABLE_ALTER, eventListener);

        // Try to add column without table.
        assertThat(manager.addColumn(addColumnParams(columnParams(NEW_COLUMN_NAME, INT32))), willThrow(TableNotFoundException.class));
        verifyNoInteractions(eventListener);

        // Create table.
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));

        // Add column.
        assertThat(manager.addColumn(addColumnParams(columnParams(NEW_COLUMN_NAME, INT32))), willBe(nullValue()));
        verify(eventListener).notify(any(AddColumnEventParameters.class), isNull());

        // Drop column.
        assertThat(manager.dropColumn(dropColumnParams(NEW_COLUMN_NAME)), willBe(nullValue()));
        verify(eventListener).notify(any(DropColumnEventParameters.class), isNull());

        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void userFutureCompletesAfterClusterWideActivationHappens() throws Exception {
        long delayDuration = TimeUnit.DAYS.toMillis(365);

        HybridTimestamp startTs = clock.now();

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLog, clockWaiter, () -> delayDuration);

        manager.start();

        try {
            CompletableFuture<Void> createTableFuture = manager.createTable(simpleTable(TABLE_NAME));

            assertFalse(createTableFuture.isDone());

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

        assertThat(manager.createIndex(createHashIndexParams(INDEX_NAME, List.of("VAL"))), willBe(nullValue()));

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
        assertThat(manager.dropTable(dropTableParams(TABLE_NAME)), willBe(nullValue()));

        // Let's make sure that the PK index has been deleted.
        eventParameters = captor.getValue();

        assertEquals(pkIndexId, eventParameters.indexId());
        assertEquals(tableId, eventParameters.tableId());
    }

    @Test
    void testLatestCatalogVersion() {
        assertEquals(0, manager.latestCatalogVersion());

        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertEquals(1, manager.latestCatalogVersion());

        assertThat(manager.createIndex(simpleIndex()), willBe(nullValue()));
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
        assertThat(manager.createIndex(simpleIndex()), willBe(nullValue()));

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

        assertThat(manager.addColumn(addColumnParams(columnParams("val2", INT32))), willCompleteSuccessfully());

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));
    }

    @Test
    public void dropColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        assertThat(manager.dropColumn(dropColumnParams("val1")), willCompleteSuccessfully());

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
                        .type(INT64)
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

    @Test
    void testCreateIndexWithAlreadyExistingName() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.createIndex(simpleIndex()), willCompleteSuccessfully());

        assertThat(
                manager.createIndex(createHashIndexParams(INDEX_NAME, List.of("VAL"))),
                willThrowFast(IndexAlreadyExistsException.class)
        );

        assertThat(
                manager.createIndex(createSortedIndexParams(INDEX_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(IndexAlreadyExistsException.class)
        );
    }

    @Test
    void testCreateIndexWithSameNameAsExistingTable() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.createIndex(createHashIndexParams(TABLE_NAME, List.of("VAL"))),
                willThrowFast(TableAlreadyExistsException.class)
        );

        assertThat(
                manager.createIndex(createSortedIndexParams(TABLE_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(TableAlreadyExistsException.class)
        );
    }

    @Test
    void testCreateIndexWithNotExistingTable() {
        assertThat(
                manager.createIndex(createHashIndexParams(TABLE_NAME, List.of("VAL"))),
                willThrowFast(TableNotFoundException.class)
        );

        assertThat(
                manager.createIndex(createSortedIndexParams(TABLE_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(TableNotFoundException.class)
        );
    }

    @Test
    void testCreateIndexWithMissingTableColumns() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.createIndex(createHashIndexParams(INDEX_NAME, List.of("fake"))),
                willThrowFast(ColumnNotFoundException.class)
        );

        assertThat(
                manager.createIndex(createSortedIndexParams(INDEX_NAME, List.of("fake"), List.of(ASC_NULLS_LAST))),
                willThrowFast(ColumnNotFoundException.class)
        );
    }

    @Test
    void testCreateUniqIndexWithMissingTableColocationColumns() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.createIndex(createHashIndexParams(INDEX_NAME, true, List.of("VAL"))),
                willThrowFast(IgniteException.class, "Unique index must include all colocation columns")
        );

        assertThat(
                manager.createIndex(createSortedIndexParams(INDEX_NAME, true, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(IgniteException.class, "Unique index must include all colocation columns")
        );
    }

    @Test
    void testDropNotExistingIndex() {
        assertThat(manager.dropIndex(DropIndexParams.builder().indexName(INDEX_NAME).build()), willThrowFast(IndexNotFoundException.class));
    }

    @Test
    void testDropNotExistingTable() {
        assertThat(manager.dropTable(dropTableParams(TABLE_NAME)), willThrowFast(TableNotFoundException.class));
    }

    @Test
    void testCreateTableWithAlreadyExistingName() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willThrowFast(TableAlreadyExistsException.class));
    }

    @Test
    void testCreateTableWithSameNameAsExistingIndex() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.createIndex(simpleIndex()), willCompleteSuccessfully());

        assertThat(manager.createTable(simpleTable(INDEX_NAME)), willThrowFast(IndexAlreadyExistsException.class));
    }

    @Test
    void testDropColumnWithNotExistingTable() {
        assertThat(manager.dropColumn(dropColumnParams("key")), willThrowFast(TableNotFoundException.class));
    }

    @Test
    void testDropColumnWithMissingTableColumns() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(manager.dropColumn(dropColumnParams("fake")), willThrowFast(ColumnNotFoundException.class));
    }

    @Test
    void testDropColumnWithPrimaryKeyColumns() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.dropColumn(dropColumnParams("ID")),
                willThrowFast(CatalogValidationException.class, "Can't drop primary key columns: [ID]")
        );
    }

    @Test
    void testDropColumnWithIndexColumns() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.createIndex(simpleIndex()), willCompleteSuccessfully());

        assertThat(
                manager.dropColumn(dropColumnParams("VAL")),
                willThrowFast(
                        CatalogValidationException.class,
                        String.format("Can't drop indexed column: [columnName=VAL, indexName=%s]", INDEX_NAME)
                )
        );
    }

    @Test
    void testAddColumnWithNotExistingTable() {
        assertThat(manager.addColumn(addColumnParams(columnParams("key", INT32))), willThrowFast(TableNotFoundException.class));
    }

    @Test
    void testAddColumnWithExistingName() {
        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(manager.addColumn(addColumnParams(columnParams("ID", INT32))), willThrowFast(ColumnAlreadyExistsException.class));
    }

    private void createSomeTable(String tableName) {
        assertThat(
                manager.createTable(createTableParams(
                        tableName,
                        List.of(columnParams("key1", INT32), columnParams("val1", INT32)),
                        List.of("key1"),
                        List.of("key1")
                )),
                willCompleteSuccessfully()
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

        return manager.alterColumn(builder.build());
    }

    private static CreateTableParams simpleTable(String name) {
        List<ColumnParams> cols = List.of(
                columnParams("ID", INT32),
                columnParamsBuilder("VAL", INT32, true).defaultValue(constant(null)).build(),
                columnParamsBuilder("VAL_NOT_NULL", INT32).defaultValue(constant(1)).build(),
                columnParams("DEC", DECIMAL, true),
                columnParams("STR", STRING, true),
                columnParamsBuilder("DEC_SCALE", DECIMAL).scale(3).build()
        );

        return simpleTable(name, cols);
    }

    private static CreateTableParams simpleTable(String tableName, List<ColumnParams> cols) {
        return createTableParams(tableName, cols, List.of(cols.get(0).name()), List.of(cols.get(0).name()));
    }

    private static CreateSortedIndexParams simpleIndex() {
        return createSortedIndexParams(INDEX_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST));
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
