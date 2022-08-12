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
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.index.Index;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.index.SortedIndexDescriptor;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.DefaultValueProvider.Type;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
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

    private final VersionedValue<Map<UUID, IgniteIndex>> indicesVv;

    private final TableManager tableManager;

    private final SchemaManager schemaManager;

    private final VersionedValue<SchemaPlus> calciteSchemaVv;

    private final Set<SchemaUpdateListener> listeners = new CopyOnWriteArraySet<>();

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public SqlSchemaManagerImpl(
            TableManager tableManager,
            SchemaManager schemaManager,
            Consumer<Function<Long, CompletableFuture<?>>> registry
    ) {
        this.tableManager = tableManager;
        this.schemaManager = schemaManager;
        schemasVv = new VersionedValue<>(registry, HashMap::new);
        tablesVv = new VersionedValue<>(registry, HashMap::new);
        indicesVv = new VersionedValue<>(registry, HashMap::new);

        calciteSchemaVv = new VersionedValue<>(null, () -> {
            SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
            newCalciteSchema.add(DEFAULT_SCHEMA_NAME, new IgniteSchema(DEFAULT_SCHEMA_NAME));
            return newCalciteSchema;
        });

        schemasVv.whenComplete((token, stringIgniteSchemaMap, throwable) -> {
            if (throwable != null) {
                calciteSchemaVv.completeExceptionally(
                        token,
                        new IgniteInternalException("Couldn't evaluate sql schemas for causality token: " + token, throwable)
                );

                return;
            }

            SchemaPlus newCalciteSchema = rebuild(stringIgniteSchemaMap);

            listeners.forEach(SchemaUpdateListener::onSchemaUpdated);

            calciteSchemaVv.complete(token, newCalciteSchema);
        });
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
        schemasVv.update(
                causalityToken,
                (schemas, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    res.putIfAbsent(schemaName, new IgniteSchema(schemaName));

                    return completedFuture(res);
                }
        );
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
                (schemas, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    res.remove(schemaName);

                    return completedFuture(res);
                }
        );
    }

    /**
     * OnSqlTypeCreated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public synchronized CompletableFuture<?> onTableCreated(
            String schemaName,
            TableImpl table,
            long causalityToken
    ) {
        schemasVv.update(
                causalityToken,
                (schemas, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    IgniteSchema schema = res.computeIfAbsent(schemaName, IgniteSchema::new);

                    CompletableFuture<IgniteTableImpl> igniteTableFuture = convert(causalityToken, table);

                    return tablesVv
                            .update(
                                    causalityToken,
                                    (tables, ex) -> {
                                        if (ex != null) {
                                            return failedFuture(ex);
                                        }

                                        Map<UUID, IgniteTable> resTbls = new HashMap<>(tables);

                                        return igniteTableFuture
                                                .thenApply(igniteTable -> {
                                                    resTbls.put(igniteTable.id(), igniteTable);

                                                    return resTbls;
                                                });
                                    }
                            )
                            .thenCombine(
                                    igniteTableFuture,
                                    (v, igniteTable) -> {
                                        schema.addTable(objectLocalName(schemaName, table.name()), igniteTable);

                                        return null;
                                    }
                            )
                            .thenCompose(v -> completedFuture(res));
                }
        );

        return calciteSchemaVv.get(causalityToken);
    }

    /**
     * OnSqlTypeUpdated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public CompletableFuture<?> onTableUpdated(
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
    public synchronized CompletableFuture<?> onTableDropped(
            String schemaName,
            String tableName,
            long causalityToken
    ) {
        schemasVv.update(causalityToken,
                (schemas, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    IgniteSchema schema = res.computeIfAbsent(schemaName, IgniteSchema::new);

                    String calciteTableName = objectLocalName(schemaName, tableName);

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

        return calciteSchemaVv.get(causalityToken);
    }

    /**
     * Rebuilds Calcite schemas.
     *
     * @param schemas Ignite schemas.
     */
    private SchemaPlus rebuild(Map<String, IgniteSchema> schemas) {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);

        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));

        schemas.forEach(newCalciteSchema::add);

        return newCalciteSchema;
    }

    private CompletableFuture<IgniteTableImpl> convert(long causalityToken, TableImpl table) {
        return schemaManager.schemaRegistry(causalityToken, table.tableId())
                .thenApply(schemaRegistry -> convert(table, schemaRegistry));
    }

    private IgniteTableImpl convert(TableImpl table) {
        SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(table.tableId());

        return convert(table, schemaRegistry);
    }

    private IgniteTableImpl convert(TableImpl table, SchemaRegistry schemaRegistry) {
        SchemaDescriptor descriptor = schemaRegistry.schema();

        List<ColumnDescriptor> colDescriptors = descriptor.columnNames().stream()
                .map(descriptor::column)
                .sorted(Comparator.comparingInt(Column::columnOrder))
                .map(col -> new ColumnDescriptorImpl(
                        col.name(),
                        descriptor.isKeyColumn(col.schemaIndex()),
                        col.nullable(),
                        col.columnOrder(),
                        col.schemaIndex(),
                        col.type(),
                        convertDefaultValueProvider(col.defaultValueProvider()),
                        col::defaultValue
                ))
                .collect(Collectors.toList());

        return new IgniteTableImpl(
                new TableDescriptorImpl(colDescriptors),
                table.internalTable(),
                schemaRegistry
        );
    }

    private DefaultValueStrategy convertDefaultValueProvider(DefaultValueProvider defaultValueProvider) {
        return defaultValueProvider.type() == Type.CONSTANT
                ? DefaultValueStrategy.DEFAULT_CONSTANT
                : DefaultValueStrategy.DEFAULT_COMPUTED;
    }

    /**
     * Cut schema name from object canonical name, if found.
     */
    private static String objectLocalName(String schemaName, String canonicalName) {
        return canonicalName.substring(schemaName.length() + 1);
    }

    public synchronized CompletableFuture<?> onIndexCreated(String schemaName, Index<?> index, long causalityToken) {
        schemasVv.update(
                causalityToken,
                (schemas, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    IgniteSchema schema = res.computeIfAbsent(schemaName, IgniteSchema::new);

                    CompletableFuture<IgniteIndex> igniteIndexFuture = convert(causalityToken, index);

                    //TODO: Should we wait for table creation here?
                    return indicesVv
                            .update(
                                    causalityToken,
                                    (indices, ex) -> {
                                        if (ex != null) {
                                            return failedFuture(ex);
                                        }

                                        Map<UUID, IgniteIndex> resIdxs = new HashMap<>(indices);

                                        return igniteIndexFuture
                                                .thenApply(igniteIndex -> {
                                                    resIdxs.put(index.id(), igniteIndex);

                                                    return resIdxs;
                                                });
                                    }
                            )
                            .thenCombine(
                                    igniteIndexFuture,
                                    (v, igniteIndex) -> {
                                        schema.addIndex(objectLocalName(schemaName, index.name()), igniteIndex);

                                        return null;
                                    }
                            )
                            .thenCompose(v -> completedFuture(res));
                }
        );

        return calciteSchemaVv.get(causalityToken);
    }

    public synchronized CompletableFuture<?> onIndexDropped(String schemaName, UUID indexId, String indexName, long causalityToken) {
        schemasVv.update(causalityToken,
                (schemas, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    IgniteSchema schema = res.computeIfAbsent(schemaName, IgniteSchema::new);

                    IgniteIndex index = (IgniteIndex) schema.getIndex(indexName);

                    if (index != null) {
                        schema.removeIndex(indexName);

                        return indicesVv
                                .update(causalityToken,
                                        (indices, ex) -> {
                                            if (ex != null) {
                                                return failedFuture(ex);
                                            }

                                            Map<UUID, IgniteIndex> resIdxs = new HashMap<>(indices);

                                            resIdxs.remove(indexId);

                                            return completedFuture(resIdxs);
                                        }
                                )
                                .thenCompose(tables -> completedFuture(res));
                    }

                    return completedFuture(res);
                }
        );

        return calciteSchemaVv.get(causalityToken);
    }

    private CompletableFuture<IgniteIndex> convert(long causalityToken, Index<?> index) {
        return tablesVv.get(causalityToken)
                .thenApply(tables -> tables.get(index.tableId()))
                .thenApply(igniteTable -> convert(index, igniteTable));
    }

    private IgniteIndex convert(Index<?> index, IgniteTable table) {
        IndexDescriptor desc = index.descriptor();

        List<String> cols = desc.columns();

        if (desc instanceof SortedIndexDescriptor) {
            List<RelFieldCollation> collations = desc.columns().stream().map(colName ->
                    TraitUtils.createFieldCollation(
                            table.descriptor().columnDescriptor(colName).logicalIndex(),
                            Objects.requireNonNull(((SortedIndexDescriptor) desc).collation(colName))
                    )
            ).collect(Collectors.toList());

            return new IgniteIndex(RelCollations.of(collations), index.name(), (InternalIgniteTable) table);
        }

        List<RelFieldCollation> collations = desc.columns().stream().map(colName ->
                TraitUtils.createFieldCollation(table.descriptor().columnDescriptor(colName).logicalIndex())
        ).collect(Collectors.toList());

        return new IgniteIndex(RelCollations.of(collations), index.name(), (InternalIgniteTable) table);
    }
}
