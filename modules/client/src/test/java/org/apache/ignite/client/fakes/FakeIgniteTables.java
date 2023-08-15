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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.NotNull;

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

    private final ConcurrentHashMap<String, TableImpl> tables = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, TableImpl> tablesById = new ConcurrentHashMap<>();

    private final CopyOnWriteArrayList<Consumer<IgniteTablesInternal>> assignmentsChangeListeners = new CopyOnWriteArrayList<>();

    private volatile List<String> partitionAssignments = null;

    private final AtomicInteger nextTableId = new AtomicInteger(1);

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
    public TableImpl createTable(String name, int id) {
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
    public TableImpl table(int id) {
        return tablesById.get(id);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        return CompletableFuture.completedFuture(table(name));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TableImpl> tableAsync(int id) {
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

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<String>> assignmentsAsync(int tableId) {
        return CompletableFuture.completedFuture(partitionAssignments);
    }

    /** {@inheritDoc} */
    @Override
    public void addAssignmentsChangeListener(Consumer<IgniteTablesInternal> listener) {
        Objects.requireNonNull(listener);

        assignmentsChangeListeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override
    public boolean removeAssignmentsChangeListener(Consumer<IgniteTablesInternal> listener) {
        Objects.requireNonNull(listener);

        return assignmentsChangeListeners.remove(listener);
    }

    /**
     * Sets partition assignments.
     *
     * @param assignments Assignments.
     */
    public void setPartitionAssignments(List<String> assignments) {
        partitionAssignments = assignments;

        for (var listener : assignmentsChangeListeners) {
            listener.accept(this);
        }
    }

    @NotNull
    private TableImpl getNewTable(String name, int id) {
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

        ColumnsExtractor keyExtractor = new ColumnsExtractor() {
            @Override
            public BinaryTuple extractColumnsFromKeyOnlyRow(BinaryRow keyOnlyRow) {
                return keyExtractor(keyOnlyRow).extractColumnsFromKeyOnlyRow(keyOnlyRow);
            }

            @Override
            public BinaryTuple extractColumns(BinaryRow row) {
                return keyExtractor(row).extractColumns(row);
            }

            private ColumnsExtractor keyExtractor(BinaryRow row) {
                SchemaDescriptor schema = schemaReg.schema(row.schemaVersion());

                return BinaryRowConverter.keyExtractor(schema);
            }
        };

        return new TableImpl(
                new FakeInternalTable(name, id, keyExtractor),
                schemaReg,
                new HeapLockManager()
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
                        new Column("ztime".toUpperCase(), NativeTypes.time(), true),
                        new Column("ztimestamp".toUpperCase(), NativeTypes.timestamp(), true),
                        new Column("zstring".toUpperCase(), NativeTypes.STRING, true),
                        new Column("zbytes".toUpperCase(), NativeTypes.BYTES, true),
                        new Column("zuuid".toUpperCase(), NativeTypes.UUID, true),
                        new Column("zbitmask".toUpperCase(), NativeTypes.bitmaskOf(16), true),
                        new Column("zdecimal".toUpperCase(), NativeTypes.decimalOf(20, 10), true),
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
        Column colocationCol2 = new Column("COLO-2", NativeTypes.INT64, true);

        return new SchemaDescriptor(
                v,
                new Column[]{
                        new Column("ID", NativeTypes.INT32, false),
                },
                new String[]{ colocationCol1.name(), colocationCol2.name() },
                new Column[]{
                        colocationCol1,
                        colocationCol2,
                        new Column("STR", NativeTypes.STRING, true)
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
