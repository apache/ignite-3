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

package org.apache.ignite.internal.sql.engine.schema;

import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Holds actual schema and mutates it on schema change, requested by Ignite.
 */
public class SqlSchemaManagerImpl implements SqlSchemaManager {
    private final VersionedValue<Map<String, IgniteSchema>> schemasVv;

    private final VersionedValue<Map<UUID, IgniteTable>> tablesVv;

    private final Runnable onSchemaUpdatedCallback;

    private final TableManager tableManager;

    private final VersionedValue<SchemaPlus> calciteSchemaVv;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public SqlSchemaManagerImpl(
            TableManager tableManager,
            Consumer<Consumer<Long>> storageRevisionUpdater,
            Runnable onSchemaUpdatedCallback
    ) {
        this.onSchemaUpdatedCallback = onSchemaUpdatedCallback;

        this.tableManager = tableManager;
        schemasVv = new VersionedValue<>(null, storageRevisionUpdater, 2, HashMap::new);
        tablesVv = new VersionedValue<>(null, storageRevisionUpdater, 2, HashMap::new);

        calciteSchemaVv = new VersionedValue<>(null, storageRevisionUpdater, 2, () -> {
            SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
            newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));
            return newCalciteSchema;
        });
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus schema(@Nullable String schema) {
        CompletableFuture<SchemaPlus> fut = calciteSchemaVv.get();

        return schema != null ? fut.join().getSubSchema(schema) : fut.join();
    }

    /** {@inheritDoc} */
    @Override
    @NotNull
    public IgniteTable tableById(UUID id) {
        Map<UUID, IgniteTable> tablesById = tablesVv.get().join();

        IgniteTable table = tablesById.get(id);

        // there is a chance that someone tries to resolve table before
        // the distributed event of that table creation has been processed
        // by TableManager, so we need to get in sync with the TableManager
        if (table == null) {
            ensureTableStructuresCreated(id);

            tablesById = tablesVv.get().join();

            // at this point the table is either null means no such table
            // really exists or the table itself
            table = tablesById.get(id);
        }

        if (table == null) {
            throw new IgniteInternalException(
                IgniteStringFormatter.format("Table not found [tableId={}]", id));
        }

        return table;
    }

    private void ensureTableStructuresCreated(UUID tableId) {
        try {
            tableManager.table(tableId);
        } catch (NodeStoppingException e) {
            // Discard the exception
        }
    }

    /**
     * Schema creation handler.
     *
     * @param schemaName Schema name.
     * @param causalityToken Causality token.
     */
    public synchronized void onSchemaCreated(String schemaName, long causalityToken) {
        schemasVv.update(
                causalityToken,
                schemas -> {
                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    res.putIfAbsent(schemaName, new IgniteSchema(schemaName));

                    return res;
                },
                e -> {
                    throw new IgniteInternalException(e);
                }
        );

        rebuild(causalityToken);
    }

    /**
     * Schema drop handler.
     *
     * @param schemaName Schema name.
     * @param causalityToken Causality token.
     */
    public synchronized void onSchemaDropped(String schemaName, long causalityToken) {
        schemasVv.update(
                    causalityToken,
                    schemas -> {
                        Map<String, IgniteSchema> res = new HashMap<>(schemas);

                        res.remove(schemaName);

                        return res;
                    },
                    e -> {
                        throw new IgniteInternalException(e);
                    }
        );

        rebuild(causalityToken);
    }

    /**
     * OnSqlTypeCreated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public synchronized void onTableCreated(
            String schemaName,
            TableImpl table,
            long causalityToken
    ) {
        final AtomicReference<IgniteTableImpl> tableRef = new AtomicReference<>();

        schemasVv.update(
                    causalityToken,
                    schemas -> {
                        IgniteSchema prevSchema = schemas.computeIfAbsent(schemaName, IgniteSchema::new);
                        IgniteSchema schema = new IgniteSchema(prevSchema.getName(), prevSchema.getTableMap());

                        SchemaDescriptor descriptor = table.schemaView().schema();

                        List<ColumnDescriptor> colDescriptors = descriptor.columnNames().stream()
                                .map(descriptor::column)
                                .sorted(comparingInt(Column::columnOrder))
                                .map(col -> new ColumnDescriptorImpl(
                                    col.name(),
                                    descriptor.isKeyColumn(col.schemaIndex()),
                                    col.columnOrder(),
                                    col.schemaIndex(),
                                    col.type(),
                                    col::defaultValue
                                ))
                                .collect(toList());

                        IgniteTableImpl igniteTable = new IgniteTableImpl(
                                new TableDescriptorImpl(colDescriptors),
                                table.internalTable(),
                                table.schemaView()
                        );

                        schema.addTable(removeSchema(schemaName, table.name()), igniteTable);

                        tableRef.set(igniteTable);

                        Map<String, IgniteSchema> res = new HashMap<>(schemas);

                        res.put(schemaName, schema);

                        return res;
                    },
                    e -> {
                        throw new IgniteInternalException(e);
                    }
        );

        tablesVv.update(
                    causalityToken,
                    tables -> {
                        Map<UUID, IgniteTable> res = new HashMap<>(tables);

                        res.put(tableRef.get().id(), tableRef.get());

                        return res;
                    },
                    e -> {
                        throw new IgniteInternalException(e);
                    }
        );

        rebuild(causalityToken);
    }

    /**
     * OnSqlTypeUpdated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public void onTableUpdated(
            String schemaName,
            TableImpl table,
            long causalityToken
    ) {
        onTableCreated(schemaName, table, causalityToken);
    }

    /**
     * OnSqlTypeDropped.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public synchronized void onTableDropped(
            String schemaName,
            String tableName,
            long causalityToken
    ) {
        final AtomicReference<InternalIgniteTable> tableRef = new AtomicReference<>();

        schemasVv.update(causalityToken,
                    schemas -> {
                        IgniteSchema prevSchema = schemas.computeIfAbsent(schemaName, IgniteSchema::new);
                        IgniteSchema schema = new IgniteSchema(prevSchema.getName(), prevSchema.getTableMap());

                        InternalIgniteTable table = (InternalIgniteTable) schema.getTable(tableName);

                        if (table != null) {
                            schema.removeTable(tableName);
                        }

                        tableRef.set(table);

                        Map<String, IgniteSchema> res = new HashMap<>(schemas);

                        res.put(schemaName, schema);

                        return res;
                    },
                    e -> {
                        throw new IgniteInternalException(e);
                    }
        );

        if (tableRef.get() != null) {
            tablesVv.update(causalityToken,
                    tables -> {
                        Map<UUID, IgniteTable> res = new HashMap<>(tables);

                        res.remove(tableRef.get().id());

                        return res;
                    },
                    e -> {
                        throw new IgniteInternalException(e);
                    }
            );
        }

        rebuild(causalityToken);
    }

    private void rebuild(long causalityToken) {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);

        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));

        // TODO rewrite with VersionedValue#update to get the current (maybe temporary) value for current token
        // TODO https://issues.apache.org/jira/browse/IGNITE-16543
        Map<String, IgniteSchema> schemas = schemasVv.get().join();

        schemas.forEach(newCalciteSchema::add);

        calciteSchemaVv.update(causalityToken, s -> newCalciteSchema, e -> {
            throw new IgniteInternalException(e);
        });

        onSchemaUpdatedCallback.run();
    }

    private static String removeSchema(String schemaName, String canonicalName) {
        return canonicalName.substring(schemaName.length() + 1);
    }
}
