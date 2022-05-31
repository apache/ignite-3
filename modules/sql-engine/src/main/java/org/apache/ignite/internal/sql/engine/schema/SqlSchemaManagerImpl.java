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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
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
    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private final VersionedValue<Map<String, IgniteSchema>> schemasVv;

    private final VersionedValue<Map<UUID, IgniteTable>> tablesVv;

    private final TableManager tableManager;

    private final VersionedValue<SchemaPlus> calciteSchemaVv;

    private final Set<SchemaUpdateListener> listeners = new CopyOnWriteArraySet<>();

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public SqlSchemaManagerImpl(
            TableManager tableManager,
            Consumer<Function<Long, CompletableFuture<?>>> registry
    ) {
        this.tableManager = tableManager;
        schemasVv = new VersionedValue<>(registry, HashMap::new);
        tablesVv = new VersionedValue<>(registry, HashMap::new);

        calciteSchemaVv = new VersionedValue<>(registry, () -> {
            SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
            newCalciteSchema.add(DEFAULT_SCHEMA_NAME, new IgniteSchema(DEFAULT_SCHEMA_NAME));
            return newCalciteSchema;
        });

        calciteSchemaVv.whenComplete((token, schema, e) -> listeners.forEach(SchemaUpdateListener::onSchemaUpdated));
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus schema(@Nullable String schema) {
        SchemaPlus schemaPlus = calciteSchemaVv.latest();

        return schema != null ? schemaPlus.getSubSchema(schema) : schemaPlus.getSubSchema(DEFAULT_SCHEMA_NAME);
    }

    /** {@inheritDoc} */
    @Override
    @NotNull
    public IgniteTable tableById(UUID id, int ver) {
        IgniteTable table = tablesVv.latest().get(id);

        // there is a chance that someone tries to resolve table before
        // the distributed event of that table creation has been processed
        // by TableManager, so we need to get in sync with the TableManager
        if (table == null || ver > table.version()) {
            table = awaitLatestTableSchema(id);
        }

        if (table == null) {
            throw new IgniteInternalException(
                IgniteStringFormatter.format("Table not found [tableId={}]", id));
        }

        if (table.version() < ver) {
            throw new IgniteInternalException(
                    IgniteStringFormatter.format("Table version not found [tableId={}, requiredVer={}, latestKnownVer={}]",
                            id, ver, table.version()));
        }

        return table;
    }

    public void registerListener(SchemaUpdateListener listener) {
        listeners.add(listener);
    }

    private @Nullable IgniteTable awaitLatestTableSchema(UUID tableId) {
        try {
            TableImpl table = tableManager.table(tableId);

            if (table == null) {
                return null;
            }

            table.schemaView().waitLatestSchema();

            return convert(table);
        } catch (NodeStoppingException e) {
            throw new IgniteInternalException(e);
        }
    }

    /**
     * Schema creation handler.
     *
     * @param schemaName Schema name.
     * @param causalityToken Causality token.
     */
    public synchronized void onSchemaCreated(String schemaName, long causalityToken) {
        CompletableFuture<Map<String, IgniteSchema>> schemasMapFut = schemasVv.update(
                causalityToken,
                (schemas, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    Map<String, IgniteSchema> res =  new HashMap<>(schemas);

                    res.putIfAbsent(schemaName, new IgniteSchema(schemaName));

                    return completedFuture(res);
                }
        );

        rebuild(causalityToken, schemasMapFut);
    }

    /**
     * Schema drop handler.
     *
     * @param schemaName Schema name.
     * @param causalityToken Causality token.
     */
    public synchronized void onSchemaDropped(String schemaName, long causalityToken) {
        CompletableFuture<Map<String, IgniteSchema>> schemasMapFut = schemasVv.update(
                    causalityToken,
                    (schemas, e) -> {
                        if (e != null) {
                            return failedFuture(e);
                        }

                        Map<String, IgniteSchema> res = new HashMap<>(schemas);

                        res.remove(schemaName);

                        return completedFuture(res);
                    }
        );

        rebuild(causalityToken, schemasMapFut);
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
        CompletableFuture<Map<String, IgniteSchema>> schemasMapFut = schemasVv.update(
                causalityToken,
                (schemas, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    IgniteSchema schema = res.computeIfAbsent(schemaName, IgniteSchema::new);

                    IgniteTableImpl igniteTable = convert(table);

                    schema.addTable(removeSchema(schemaName, table.name()), igniteTable);

                    return tablesVv
                        .update(
                            causalityToken,
                            (tables, ex) -> {
                                if (ex != null) {
                                    return failedFuture(ex);
                                }

                                Map<UUID, IgniteTable> resTbls = new HashMap<>(tables);

                                resTbls.put(igniteTable.id(), igniteTable);

                                return completedFuture(resTbls);
                            }
                        )
                        .thenCompose(tables -> completedFuture(res));
                }
        );

        rebuild(causalityToken, schemasMapFut);
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
        CompletableFuture<Map<String, IgniteSchema>> schemasMapFut = schemasVv.update(causalityToken,
                (schemas, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    IgniteSchema schema = res.computeIfAbsent(schemaName, IgniteSchema::new);

                    String calciteTableName = removeSchema(schemaName, tableName);

                    InternalIgniteTable table = (InternalIgniteTable) schema.getTable(calciteTableName);

                    if (table != null) {
                        schema.removeTable(calciteTableName);

                        return tablesVv
                            .update(causalityToken,
                                (tables, ex) -> {
                                    if (ex != null) {
                                        return failedFuture(ex);
                                    }

                                    Map<UUID, IgniteTable> resTbls = new HashMap<>(tables);

                                    resTbls.remove(table.id());

                                    return completedFuture(resTbls);
                                }
                            )
                            .thenCompose(tables -> completedFuture(res));
                    }

                    return completedFuture(res);
                }
        );

        rebuild(causalityToken, schemasMapFut);
    }

    private void rebuild(long causalityToken, CompletableFuture<Map<String, IgniteSchema>> schemasFut) {
        schemasFut.thenCompose(schemas -> {
            SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);

            newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));

            schemas.forEach(newCalciteSchema::add);

            return calciteSchemaVv.update(causalityToken, (s, e) -> {
                if (e != null) {
                    return failedFuture(e);
                }

                return completedFuture(newCalciteSchema);
            });
        });
    }

    private IgniteTableImpl convert(TableImpl table) {
        SchemaDescriptor descriptor = table.schemaView().schema();

        List<ColumnDescriptor> colDescriptors = descriptor.columnNames().stream()
                .map(descriptor::column)
                .sorted(Comparator.comparingInt(Column::columnOrder))
                .map(col -> new ColumnDescriptorImpl(
                        col.name(),
                        descriptor.isKeyColumn(col.schemaIndex()),
                        col.columnOrder(),
                        col.schemaIndex(),
                        col.type(),
                        col::defaultValue
                ))
                .collect(Collectors.toList());

        return new IgniteTableImpl(
                new TableDescriptorImpl(colDescriptors),
                table.internalTable(),
                table.schemaView()
        );
    }

    private static String removeSchema(String schemaName, String canonicalName) {
        return canonicalName.substring(schemaName.length() + 1);
    }
}
