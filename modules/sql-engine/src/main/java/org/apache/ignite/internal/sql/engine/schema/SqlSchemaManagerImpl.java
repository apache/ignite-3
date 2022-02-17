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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.causality.OutdatedTokenException;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.sql.engine.extension.SqlExtension.ExternalCatalog;
import org.apache.ignite.internal.sql.engine.extension.SqlExtension.ExternalSchema;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.PatchedMapView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Holds actual schema and mutates it on schema change, requested by Ignite.
 */
public class SqlSchemaManagerImpl implements SqlSchemaManager {
    private final VersionedValue<Map<String, IgniteSchema>> schemasVv;

    private final VersionedValue<Map<UUID, IgniteTable>> tablesVv;

    private final VersionedValue<Map<String, Schema>> externalCatalogsVv;

    private final Runnable onSchemaUpdatedCallback;

    private final Consumer<Consumer<Long>> storageRevisionUpdater;

    private final VersionedValue<SchemaPlus> calciteSchemaVv;

    private final Supplier<CompletableFuture<Long>> directMsRevision;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public SqlSchemaManagerImpl(
            Consumer<Consumer<Long>> revisionUpdater,
            Runnable onSchemaUpdatedCallback,
            Supplier<CompletableFuture<Long>> directMsRevision
    ) {
        this.onSchemaUpdatedCallback = onSchemaUpdatedCallback;
        this.storageRevisionUpdater = revisionUpdater;

        schemasVv = new VersionedValue<>(null, storageRevisionUpdater, 2, HashMap::new);
        tablesVv = new VersionedValue<>(null, storageRevisionUpdater, 2, HashMap::new);
        externalCatalogsVv = new VersionedValue<>(null, storageRevisionUpdater, 2, HashMap::new);
        this.directMsRevision = directMsRevision;

        calciteSchemaVv = new VersionedValue<>(null, storageRevisionUpdater, 2, () -> {
            SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
            newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));
            return newCalciteSchema;
        });
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus schema(@Nullable String schema) {
        CompletableFuture<SchemaPlus> fut = directMsRevision.get().thenCompose(token -> {
            try {
                return calciteSchemaVv.get(token);
            } catch (OutdatedTokenException e) {
                throw new IgniteInternalException(e);
            }
        });

        try {
            return schema != null ? fut.get().getSubSchema(schema) : fut.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IgniteInternalException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    @NotNull
    public IgniteTable tableById(UUID id) {
        CompletableFuture<Map<UUID, IgniteTable>> fut = directMsRevision.get().thenCompose(token -> {
            try {
                return tablesVv.get(token);
            } catch (OutdatedTokenException e) {
                throw new IgniteInternalException(e);
            }
        });

        IgniteTable table;

        try {
            table = fut.get().get(id);
        } catch (InterruptedException | ExecutionException e) {
            throw new IgniteException(e);
        }

        if (table == null) {
            throw new IgniteInternalException(
                IgniteStringFormatter.format("Table not found [tableId={}]", id));
        }

        return table;
    }

    /**
     * Register an external catalog under given name.
     *
     * @param name Name of the external catalog.
     * @param catalog Catalog to register.
     */
    public void registerExternalCatalog(String name, ExternalCatalog catalog, long causalityToken) {
        CompletableFuture.allOf(
                catalog.schemaNames().stream()
                    .map(schemaName -> registerExternalSchema(name, schemaName, catalog.schema(schemaName), causalityToken))
                    .toArray(a -> new CompletableFuture[catalog.schemaNames().size()])
        ).thenCompose(v -> {
            try {
                return rebuild(causalityToken);
            } catch (OutdatedTokenException e) {
                throw new IgniteInternalException(e);
            }
        });
    }

    private CompletableFuture<?> registerExternalSchema(String catalogName, String schemaName, ExternalSchema schema, long causalityToken) {
        final Map<String, Table> tables = new HashMap<>();

        return tablesVv
                .update(causalityToken,
                    tablesByIds -> {
                        Map<UUID, IgniteTable> tempTables = PatchedMapView.of(tablesByIds, Integer.MAX_VALUE).map();

                        for (String name : schema.tableNames()) {
                            IgniteTable table = schema.table(name);

                            tables.put(name, table);
                            tempTables = PatchedMapView.of(tempTables).put(table.id(), table);
                        }

                        return PatchedMapView.of(tempTables).map();
                    },
                    e -> {
                        throw new IgniteInternalException(e);
                    }
                )
                .thenApply(t -> tables)
                .thenCompose(t ->
                    externalCatalogsVv.update(causalityToken,
                        catalogs -> {
                            Map<String, Schema> res = PatchedMapView.of(catalogs)
                                .computeIfAbsent(catalogName, n -> Frameworks.createRootSchema(false));

                            SchemaPlus schemaPlus = (SchemaPlus) res.get(catalogName);
                            schemaPlus.add(schemaName, new ExternalSchemaHolder(t));

                            return res;
                        },
                        e -> {
                            throw new IgniteInternalException(e);
                        }
                    )
                );
    }

    /**
     * Schema creation handler.
     *
     * @param schemaName Schema name.
     * @param causalityToken Causality token.
     */
    public CompletableFuture<Void> onSchemaCreated(String schemaName, long causalityToken) {
        return schemasVv
                .update(
                    causalityToken,
                    schemas -> PatchedMapView.of(schemas).putIfAbsent(schemaName, new IgniteSchema(schemaName)),
                    e -> {
                        throw new IgniteInternalException(e);
                    }
                )
                .thenCompose(s -> {
                    try {
                        return rebuild(causalityToken);
                    } catch (OutdatedTokenException e) {
                        throw new IgniteInternalException(e);
                    }
                });
    }

    /**
     * Schema drop handler.
     *
     * @param schemaName Schema name.
     * @param causalityToken Causality token.
     */
    public CompletableFuture<Void> onSchemaDropped(String schemaName, long causalityToken) {
        return schemasVv
                .update(
                    causalityToken,
                    schemas -> PatchedMapView.of(schemas).remove(schemaName),
                    e -> {
                        throw new IgniteInternalException(e);
                    }
                )
                .thenCompose(s -> {
                    try {
                        return rebuild(causalityToken);
                    } catch (OutdatedTokenException e) {
                        throw new IgniteInternalException(e);
                    }
                });
    }

    /**
     * OnSqlTypeCreated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public CompletableFuture<Void> onTableCreated(
            String schemaName,
            TableImpl table,
            long causalityToken
    ) {
        final AtomicReference<IgniteTableImpl> tableRef = new AtomicReference<>();

        return schemasVv
                .update(
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

                        return PatchedMapView.of(schemas).put(schemaName, schema);
                    },
                    e -> {
                        throw new IgniteInternalException(e);
                    }
                )
                .thenApply(s -> tableRef.get())
                .thenCompose(igniteTable ->
                    tablesVv
                        .update(
                            causalityToken,
                            tables -> PatchedMapView.of(tables).put(igniteTable.id(), igniteTable),
                            e -> {
                                throw new IgniteInternalException(e);
                            }
                        )
                )
                .thenCompose(t -> {
                    try {
                        return rebuild(causalityToken);
                    } catch (OutdatedTokenException e) {
                        throw new IgniteInternalException(e);
                    }
                });
    }

    /**
     * OnSqlTypeUpdated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public CompletableFuture<Void> onTableUpdated(
            String schemaName,
            TableImpl table,
            long causalityToken
    ) {
        return onTableCreated(schemaName, table, causalityToken);
    }

    /**
     * OnSqlTypeDropped.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public CompletableFuture<Void> onTableDropped(
            String schemaName,
            String tableName,
            long causalityToken
    ) {
        final AtomicReference<InternalIgniteTable> tableRef = new AtomicReference<>();

        return schemasVv
                .update(causalityToken,
                    schemas -> {
                        IgniteSchema prevSchema = schemas.computeIfAbsent(schemaName, IgniteSchema::new);
                        IgniteSchema schema = new IgniteSchema(prevSchema.getName(), prevSchema.getTableMap());

                        InternalIgniteTable table = (InternalIgniteTable) schema.getTable(tableName);

                        if (table != null) {
                            schema.removeTable(tableName);
                        }

                        tableRef.set(table);

                        return PatchedMapView.of(schemas).put(schemaName, schema);
                    },
                    e -> {
                        throw new IgniteInternalException(e);
                    }
                )
                .thenApply(s -> tableRef.get())
                .thenCompose(table ->
                    tablesVv.update(causalityToken,
                        tables -> PatchedMapView.of(tables).remove(table.id()),
                        e -> {
                            throw new IgniteInternalException(e);
                        }
                    )
                )
                .thenCompose(t -> {
                    try {
                        return rebuild(causalityToken);
                    } catch (OutdatedTokenException e) {
                        throw new IgniteInternalException(e);
                    }
                });
    }

    private CompletableFuture<Void> rebuild(long causalityToken) throws OutdatedTokenException {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);

        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));

        return CompletableFuture.allOf(
            schemasVv.get(causalityToken).thenAccept(schemas -> schemas.forEach(newCalciteSchema::add)),
            externalCatalogsVv.get(causalityToken).thenAccept(catalogs -> catalogs.forEach(newCalciteSchema::add))
        ).thenAccept(v -> {
            calciteSchemaVv.set(causalityToken, newCalciteSchema);

            onSchemaUpdatedCallback.run();
        });
    }

    private static String removeSchema(String schemaName, String canonicalName) {
        return canonicalName.substring(schemaName.length() + 1);
    }

    private static class ExternalSchemaHolder extends AbstractSchema {
        private final Map<String, Table> tables;

        public ExternalSchemaHolder(Map<String, Table> tables) {
            this.tables = tables;
        }

        @Override protected Map<String, Table> getTableMap() {
            return tables;
        }
    }
}
