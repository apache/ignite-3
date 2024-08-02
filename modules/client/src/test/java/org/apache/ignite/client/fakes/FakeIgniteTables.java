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

package org.apache.ignite.client.fakes;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.client.handler.FakePlacementDriver;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;

/**
 * Fake tables.
 */
public class FakeIgniteTables implements IgniteTablesInternal {
    public static final String TABLE_EXISTS = "Table exists";

    public static final String TABLE_ALL_COLUMNS = "all-columns";

    public static final String TABLE_ONE_COLUMN = "one-column";

    public static final String TABLE_WITH_DEFAULT_VALUES = "default-columns";

    public static final String TABLE_COMPOSITE_KEY = "composite-key";

    public static final String TABLE_COLOCATION_KEY = "colocation-key";

    public static final String BAD_TABLE = "bad-table";

    public static final String BAD_TABLE_ERR = "Err!";

    private final ConcurrentHashMap<String, TableViewInternal> tables = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, TableViewInternal> tablesById = new ConcurrentHashMap<>();

    private final AtomicInteger nextTableId = new AtomicInteger(1);

    private final IgniteCompute compute;

    private final FakePlacementDriver placementDriver;

    FakeIgniteTables(IgniteCompute compute, FakePlacementDriver placementDriver) {
        this.compute = compute;
        this.placementDriver = placementDriver;
    }

    /**
     * Creates a table.
     *
     * @param name Table name.
     * @return Table.
     */
    public Table createTable(String name) {
        return createTable(name, nextTableId.getAndIncrement());
    }

    /**
     * Creates a table.
     *
     * @param name Table name.
     * @param id Table id.
     * @return Table.
     */
    public TableViewInternal createTable(String name, int id) {
        var newTable = getNewTable(name, id);

        var oldTable = tables.putIfAbsent(name, newTable);

        if (oldTable != null) {
            throw new IgniteException(TABLE_EXISTS);
        }

        tablesById.put(newTable.tableId(), newTable);

        return newTable;
    }

    /**
     * Drops a table.
     *
     * @param name Table name.
     */
    public void dropTable(String name) {
        var table = tables.remove(name);

        if (table != null) {
            tablesById.remove(table.tableId());
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<Table> tables() {
        return new ArrayList<>(tables.values());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        return completedFuture(tables());
    }

    /** {@inheritDoc} */
    @Override
    public Table table(String name) {
        if (BAD_TABLE.equals(name)) {
            throw new RuntimeException(BAD_TABLE_ERR);
        }

        return tableView(name);
    }

    /** {@inheritDoc} */
    @Override
    public TableViewInternal table(int id) {
        return tablesById.get(id);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        return completedFuture(table(name));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TableViewInternal> tableAsync(int id) {
        return completedFuture(tablesById.get(id));
    }

    /** {@inheritDoc} */
    @Override
    public TableViewInternal tableView(String name) {
        return tables.get(name);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TableViewInternal> tableViewAsync(String name) {
        return completedFuture(tableView(name));
    }

    @Override
    public @Nullable TableViewInternal cachedTable(int tableId) {
        return table(tableId);
    }

    @Override
    public void setStreamerReceiverRunner(StreamerReceiverRunner runner) {
    }

    private TableViewInternal getNewTable(String name, int id) {
        Function<Integer, SchemaDescriptor> history;

        switch (name) {
            case TABLE_ALL_COLUMNS:
                history = this::getAllColumnsSchema;
                break;

            case TABLE_ONE_COLUMN:
                history = this::getOneColumnSchema;
                break;

            case TABLE_WITH_DEFAULT_VALUES:
                history = this::getDefaultColumnValuesSchema;
                break;

            case TABLE_COMPOSITE_KEY:
                history = this::getCompositeKeySchema;
                break;

            case TABLE_COLOCATION_KEY:
                history = this::getColocationKeySchema;
                break;

            default:
                history = this::getSchema;
                break;
        }

        FakeSchemaRegistry schemaReg = new FakeSchemaRegistry(history);

        ColumnsExtractor keyExtractor = row -> {
            SchemaDescriptor schema = schemaReg.schema(row.schemaVersion());

            return BinaryRowConverter.keyExtractor(schema).extractColumns(row);
        };

        return new TableImpl(
                new FakeInternalTable(name, id, keyExtractor, compute),
                schemaReg,
                new HeapLockManager(),
                new SchemaVersions() {
                    @Override
                    public CompletableFuture<Integer> schemaVersionAt(HybridTimestamp timestamp, int tableId) {
                        return completedFuture(schemaReg.lastKnownSchemaVersion());
                    }

                    @Override
                    public CompletableFuture<Integer> schemaVersionAtNow(int tableId) {
                        return completedFuture(schemaReg.lastKnownSchemaVersion());
                    }
                },
                mock(IgniteSql.class),
                -1
        );
    }

    /**
     * Gets the schema.
     *
     * @param v Version.
     * @return Schema descriptor.
     */
    private SchemaDescriptor getSchema(Integer v) {
        switch (v) {
            case 1:
                return new SchemaDescriptor(
                        1,
                        new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                        new Column[]{new Column("name".toUpperCase(), NativeTypes.STRING, true)});

            case 2:
                return new SchemaDescriptor(
                        2,
                        new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                        new Column[]{
                                new Column("name".toUpperCase(), NativeTypes.STRING, true),
                                new Column("xyz".toUpperCase(), NativeTypes.STRING, true)
                        });
            default:
                return null;
        }
    }

    /**
     * Gets the schema.
     *
     * @param v Version.
     * @return Schema descriptor.
     */
    private SchemaDescriptor getAllColumnsSchema(Integer v) {
        return new SchemaDescriptor(
                v,
                new Column[]{
                        new Column("gid".toUpperCase(), NativeTypes.INT64, false),
                        new Column("id".toUpperCase(), NativeTypes.STRING, false)
                },
                new Column[]{
                        new Column("zboolean".toUpperCase(), NativeTypes.BOOLEAN, true),
                        new Column("zbyte".toUpperCase(), NativeTypes.INT8, true),
                        new Column("zshort".toUpperCase(), NativeTypes.INT16, true),
                        new Column("zint".toUpperCase(), NativeTypes.INT32, true),
                        new Column("zlong".toUpperCase(), NativeTypes.INT64, true),
                        new Column("zfloat".toUpperCase(), NativeTypes.FLOAT, true),
                        new Column("zdouble".toUpperCase(), NativeTypes.DOUBLE, true),
                        new Column("zdate".toUpperCase(), NativeTypes.DATE, true),
                        new Column("ztime".toUpperCase(), NativeTypes.time(0), true),
                        new Column("ztimestamp".toUpperCase(), NativeTypes.timestamp(6), true),
                        new Column("zstring".toUpperCase(), NativeTypes.STRING, true),
                        new Column("zbytes".toUpperCase(), NativeTypes.BYTES, true),
                        new Column("zuuid".toUpperCase(), NativeTypes.UUID, true),
                        new Column("zdecimal".toUpperCase(), NativeTypes.decimalOf(20, 10), true),
                });
    }

    /**
     * Gets the schema.
     *
     * @param v Version.
     * @return Schema descriptor.
     */
    private SchemaDescriptor getDefaultColumnValuesSchema(Integer v) {
        return new SchemaDescriptor(
                v,
                new Column[]{
                        new Column("id".toUpperCase(), NativeTypes.INT32, false)
                },
                new Column[]{
                        new Column("num".toUpperCase(), NativeTypes.INT8, true, DefaultValueProvider.constantProvider((byte) 42)),
                        new Column("str".toUpperCase(), NativeTypes.STRING, true, DefaultValueProvider.constantProvider("def_str")),
                        new Column("strNonNull".toUpperCase(), NativeTypes.STRING,
                                false, DefaultValueProvider.constantProvider("def_str2")),
                });
    }

    /**
     * Gets the schema.
     *
     * @param v Version.
     * @return Schema descriptor.
     */
    private SchemaDescriptor getCompositeKeySchema(Integer v) {
        return new SchemaDescriptor(
                v,
                new Column[]{
                        new Column("ID1", NativeTypes.INT32, false),
                        new Column("ID2", NativeTypes.STRING, false)
                },
                new Column[]{
                        new Column("STR", NativeTypes.STRING, true)
                });
    }


    /**
     * Gets the schema.
     *
     * @param v Version.
     * @return Schema descriptor.
     */
    private SchemaDescriptor getColocationKeySchema(Integer v) {
        Column colocationCol1 = new Column("COLO-1", NativeTypes.STRING, false);
        Column colocationCol2 = new Column("COLO-2", NativeTypes.INT64, false);

        return new SchemaDescriptor(
                v,
                List.of(
                        new Column("ID", NativeTypes.INT32, false),
                        colocationCol1,
                        colocationCol2,
                        new Column("STR", NativeTypes.STRING, true)
                ),
                List.of("ID", colocationCol1.name(), colocationCol2.name()),
                List.of(colocationCol1.name(), colocationCol2.name())
        );
    }

    /**
     * Gets the schema.
     *
     * @param v Version.
     * @return Schema descriptor.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private SchemaDescriptor getOneColumnSchema(Integer v) {
        return new SchemaDescriptor(
                v,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.STRING, false)},
                new Column[0]);
    }
}
