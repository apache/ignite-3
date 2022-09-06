/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.NotNull;

/**
 * Fake tables.
 */
public class FakeIgniteTables implements IgniteTables, IgniteTablesInternal {
    public static final String TABLE_EXISTS = "Table exists";

    public static final String TABLE_ALL_COLUMNS = "all-columns";

    public static final String TABLE_ONE_COLUMN = "one-column";

    public static final String TABLE_WITH_DEFAULT_VALUES = "default-columns";

    public static final String BAD_TABLE = "bad-table";

    public static final String BAD_TABLE_ERR = "Err!";

    private final ConcurrentHashMap<String, TableImpl> tables = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<UUID, TableImpl> tablesById = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override
    public Table createTable(String name, Consumer<TableChange> tableInitChange) {
        var newTable = getNewTable(name);

        var oldTable = tables.putIfAbsent(name, newTable);

        if (oldTable != null) {
            throw new IgniteException(TABLE_EXISTS);
        }

        tablesById.put(newTable.tableId(), newTable);

        return newTable;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange) {
        return CompletableFuture.completedFuture(createTable(name, tableInitChange));
    }

    /** {@inheritDoc} */
    @Override
    public void alterTable(String name, Consumer<TableChange> tableChange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> alterTableAsync(String name, Consumer<TableChange> tableChange) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void dropTable(String name) {
        var table = tables.remove(name);

        if (table != null) {
            tablesById.remove(table.tableId());
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> dropTableAsync(String name) {
        dropTable(name);

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    public List<Table> tables() {
        return new ArrayList<>(tables.values());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        return CompletableFuture.completedFuture(tables());
    }

    /** {@inheritDoc} */
    @Override
    public Table table(String name) {
        if (BAD_TABLE.equals(name)) {
            throw new RuntimeException(BAD_TABLE_ERR);
        }

        return tableImpl(name);
    }

    /** {@inheritDoc} */
    @Override
    public TableImpl table(UUID id) {
        return tablesById.get(id);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        return CompletableFuture.completedFuture(table(name));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TableImpl> tableAsync(UUID id) {
        return CompletableFuture.completedFuture(tablesById.get(id));
    }

    /** {@inheritDoc} */
    @Override
    public TableImpl tableImpl(String name) {
        return tables.get(name);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TableImpl> tableImplAsync(String name) {
        return CompletableFuture.completedFuture(tableImpl(name));
    }

    @NotNull
    private TableImpl getNewTable(String name) {
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

            default:
                history = this::getSchema;
                break;
        }

        return new TableImpl(
                new FakeInternalTable(name, UUID.randomUUID()),
                new FakeSchemaRegistry(history)
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
                        new Column("zbyte".toUpperCase(), NativeTypes.INT8, true),
                        new Column("zshort".toUpperCase(), NativeTypes.INT16, true),
                        new Column("zint".toUpperCase(), NativeTypes.INT32, true),
                        new Column("zlong".toUpperCase(), NativeTypes.INT64, true),
                        new Column("zfloat".toUpperCase(), NativeTypes.FLOAT, true),
                        new Column("zdouble".toUpperCase(), NativeTypes.DOUBLE, true),
                        new Column("zdate".toUpperCase(), NativeTypes.DATE, true),
                        new Column("ztime".toUpperCase(), NativeTypes.time(), true),
                        new Column("ztimestamp".toUpperCase(), NativeTypes.timestamp(), true),
                        new Column("zstring".toUpperCase(), NativeTypes.STRING, true),
                        new Column("zbytes".toUpperCase(), NativeTypes.BYTES, true),
                        new Column("zuuid".toUpperCase(), NativeTypes.UUID, true),
                        new Column("zbitmask".toUpperCase(), NativeTypes.bitmaskOf(16), true),
                        new Column("zdecimal".toUpperCase(), NativeTypes.decimalOf(20, 100), true),
                        new Column("znumber".toUpperCase(), NativeTypes.numberOf(24), true),
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
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private SchemaDescriptor getOneColumnSchema(Integer v) {
        return new SchemaDescriptor(
                v,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.STRING, false)},
                new Column[0]);
    }
}
