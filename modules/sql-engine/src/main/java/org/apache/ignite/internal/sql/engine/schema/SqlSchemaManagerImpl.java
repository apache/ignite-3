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

package org.apache.ignite.internal.sql.engine.schema;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.sql.engine.SqlQueryProcessor.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.causality.CompletableVersionedValue;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.causality.OutdatedTokenException;
import org.apache.ignite.internal.index.HashIndex;
import org.apache.ignite.internal.index.Index;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.index.SortedIndexDescriptor;
import org.apache.ignite.internal.index.SortedIndexImpl;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.DefaultValueProvider.Type;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Holds actual schema and mutates it on schema change, requested by Ignite.
 */
public class SqlSchemaManagerImpl implements SqlSchemaManager {
    private final IncrementalVersionedValue<Map<String, IgniteSchema>> schemasVv;

    private final IncrementalVersionedValue<Map<Integer, IgniteTable>> tablesVv;

    private final Map<Integer, CompletableFuture<?>> pkIdxReady = new ConcurrentHashMap<>();

    private final IncrementalVersionedValue<Map<Integer, IgniteIndex>> indicesVv;

    private final TableManager tableManager;
    private final SchemaManager schemaManager;

    private final CompletableVersionedValue<SchemaPlus> calciteSchemaVv;

    private final Set<SchemaUpdateListener> listeners = new CopyOnWriteArraySet<>();

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public SqlSchemaManagerImpl(
            TableManager tableManager,
            SchemaManager schemaManager,
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            IgniteSpinBusyLock busyLock
    ) {
        this.tableManager = tableManager;
        this.schemaManager = schemaManager;

        schemasVv = new IncrementalVersionedValue<>(registry, HashMap::new);
        tablesVv = new IncrementalVersionedValue<>(registry, HashMap::new);
        indicesVv = new IncrementalVersionedValue<>(registry, HashMap::new);
        this.busyLock = busyLock;

        calciteSchemaVv = new CompletableVersionedValue<>(() -> {
            SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
            newCalciteSchema.add(DEFAULT_SCHEMA_NAME, new IgniteSchema(DEFAULT_SCHEMA_NAME));
            return newCalciteSchema;
        });

        schemasVv.whenComplete((token, stringIgniteSchemaMap, throwable) -> {
            if (!busyLock.enterBusy()) {
                calciteSchemaVv.completeExceptionally(token, new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));

                return;
            }
            try {
                if (throwable != null) {
                    calciteSchemaVv.completeExceptionally(
                            token,
                            new IgniteInternalException(
                                    INTERNAL_ERR, "Couldn't evaluate sql schemas for causality token: " + token, throwable)
                    );

                    return;
                }

                SchemaPlus newCalciteSchema = rebuild(stringIgniteSchemaMap);

                listeners.forEach(SchemaUpdateListener::onSchemaUpdated);

                calciteSchemaVv.complete(token, newCalciteSchema);
            } finally {
                busyLock.leaveBusy();
            }
        });
    }

    /** Returns latest schema. */
    @TestOnly
    public SchemaPlus latestSchema(@Nullable String schema) {
        // stub for waiting pk indexes, more clear place is IgniteSchema
        CompletableFuture.allOf(pkIdxReady.values().toArray(CompletableFuture[]::new)).join();

        SchemaPlus schemaPlus = calciteSchemaVv.latest();

        return schema != null ? schemaPlus.getSubSchema(schema) : schemaPlus.getSubSchema(DEFAULT_SCHEMA_NAME);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public SchemaPlus schema(@Nullable String name, int version) {
        return latestSchema(name);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public SchemaPlus schema(@Nullable String name, long timestamp) {
        return latestSchema(name);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> schemaReadyFuture(long version) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }
        try {
            if (version == IgniteSchema.INITIAL_VERSION) {
                return completedFuture(null);
            }

            CompletableFuture<Void> lastSchemaFut;

            try {
                lastSchemaFut = calciteSchemaVv.get(version).thenAccept(ignore -> {});
            } catch (OutdatedTokenException e) {
                return completedFuture(null);
            }

            return lastSchemaFut;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTable tableById(int id) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }
        try {
            IgniteTable table = tablesVv.latest().get(id);

            if (table == null) {
                throw new IgniteInternalException(INTERNAL_ERR,
                        format("Table not found [tableId={}]", id));
            }

            return table;
        } finally {
            busyLock.leaveBusy();
        }
    }

    public void registerListener(SchemaUpdateListener listener) {
        listeners.add(listener);
    }

    /**
     * OnSqlTypeCreated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public CompletableFuture<?> onTableCreated(String schemaName, int tableId, long causalityToken) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            pkIdxReady.computeIfAbsent(tableId, k -> new CompletableFuture<>());

            CompletableFuture<Map<Integer, IgniteTable>> updatedTables = tablesVv.update(causalityToken, (tables, e) ->
                    inBusyLock(busyLock, () -> {
                        if (e != null) {
                            return failedFuture(e);
                        }

                        return tableManager.tableAsync(causalityToken, tableId)
                                .thenCompose(table -> convert(causalityToken, table))
                                .thenApply(igniteTable -> {
                                    Map<Integer, IgniteTable> resTbls = new HashMap<>(tables);

                                    IgniteTable oldTable = resTbls.put(igniteTable.id(), igniteTable);

                                    // looks like this is UPDATE operation
                                    if (oldTable != null) {
                                        for (var index : oldTable.indexes().values()) {
                                            igniteTable.addIndex(index);
                                        }
                                    }

                                    return resTbls;
                                });
                    })
            );

            schemasVv.update(causalityToken, (schemas, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(e);
                }

                return updatedTables.thenApply(tables -> {
                    IgniteTable igniteTable = tables.get(tableId);

                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    IgniteSchema schema = res.compute(schemaName,
                            (k, v) -> v == null ? new IgniteSchema(schemaName, causalityToken) : IgniteSchema.copy(v, causalityToken));

                    schema.addTable(igniteTable);

                    return res;
                });
            }));

            // calciteSchemaVv depends on all other Versioned Values and is completed last.
            return calciteSchemaVv.get(causalityToken);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * OnSqlTypeUpdated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public CompletableFuture<?> onTableUpdated(String schemaName, int tableId, long causalityToken) {
        return onTableCreated(schemaName, tableId, causalityToken);
    }

    /**
     * OnSqlTypeDropped.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public CompletableFuture<?> onTableDropped(String schemaName, int tableId, long causalityToken) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            CompletableFuture<IgniteTable> removedTableFuture = new CompletableFuture<>();

            tablesVv.update(causalityToken, (tables, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    removedTableFuture.completeExceptionally(e);

                    return failedFuture(e);
                }

                Map<Integer, IgniteTable> resTbls = new HashMap<>(tables);

                IgniteTable removedTable = resTbls.remove(tableId);

                removedTableFuture.complete(removedTable);

                return completedFuture(resTbls);
            }));

            schemasVv.update(causalityToken, (schemas, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(e);
                }

                return removedTableFuture.thenApply(table -> {
                    if (table == null) {
                        return schemas;
                    }

                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    IgniteSchema schema = res.compute(schemaName,
                            (k, v) -> v == null ? new IgniteSchema(schemaName, causalityToken) : IgniteSchema.copy(v, causalityToken));

                    schema.removeTable(table.name());

                    pkIdxReady.remove(table.id());

                    return res;
                });
            }));

            // calciteSchemaVv depends on all other Versioned Values and is completed last.
            return calciteSchemaVv.get(causalityToken);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Rebuilds Calcite schemas.
     *
     * @param schemas Ignite schemas.
     */
    private SchemaPlus rebuild(Map<String, IgniteSchema> schemas) {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);

        newCalciteSchema.add(DEFAULT_SCHEMA_NAME, new IgniteSchema(DEFAULT_SCHEMA_NAME));

        schemas.forEach(newCalciteSchema::add);

        return newCalciteSchema;
    }

    private CompletableFuture<IgniteTableImpl> convert(long causalityToken, TableImpl table) {
        return schemaManager.schemaRegistry(causalityToken, table.tableId())
                .thenApply(schemaRegistry -> inBusyLock(busyLock, () -> convert(table, schemaRegistry, causalityToken)));
    }

    private IgniteTableImpl convert(TableImpl table, SchemaRegistry schemaRegistry, long schemaVersion) {
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

        IntList colocationColumns = new IntArrayList();

        for (Column column : descriptor.colocationColumns()) {
            colocationColumns.add(column.columnOrder());
        }

        // TODO Use the actual zone ID after implementing https://issues.apache.org/jira/browse/IGNITE-18426.
        IgniteDistribution distribution = IgniteDistributions.affinity(colocationColumns, table.tableId(), table.tableId());

        InternalTable internalTable = table.internalTable();
        DoubleSupplier rowCount = IgniteTableImpl.rowCountStatistic(internalTable);

        return new IgniteTableImpl(
                new TableDescriptorImpl(colDescriptors, distribution),
                internalTable.tableId(),
                internalTable.name(),
                schemaRegistry.lastSchemaVersion(),
                rowCount
        );
    }

    private DefaultValueStrategy convertDefaultValueProvider(DefaultValueProvider defaultValueProvider) {
        return defaultValueProvider.type() == Type.CONSTANT
                ? DefaultValueStrategy.DEFAULT_CONSTANT
                : DefaultValueStrategy.DEFAULT_COMPUTED;
    }

    /**
     * Index created callback method register index in Calcite schema.
     *
     * @param tableId Table ID.
     * @param indexId Index ID.
     * @param causalityToken Causality token.
     * @return Schema registration future.
     */
    public CompletableFuture<?> onIndexCreated(int tableId, int indexId, IndexDescriptor indexDescriptor, long causalityToken) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            CompletableFuture<Map<Integer, IgniteIndex>> updatedIndices = indicesVv.update(causalityToken, (indices, e) ->
                    inBusyLock(busyLock, () -> {
                        if (e != null) {
                            return failedFuture(e);
                        }

                        return tableManager.tableAsync(causalityToken, tableId).thenApply(table -> {
                            var igniteIndex = new IgniteIndex(newIndex(table, indexId, indexDescriptor));

                            Map<Integer, IgniteIndex> resIdxs = new HashMap<>(indices);

                            resIdxs.put(indexId, igniteIndex);

                            return resIdxs;
                        });
                    }));

            CompletableFuture<Map<Integer, IgniteTable>> updatedTables = tablesVv.update(causalityToken, (tables, e) ->
                    inBusyLock(busyLock, () -> {
                        if (e != null) {
                            return failedFuture(e);
                        }

                        return updatedIndices.thenApply(indices -> {
                            IgniteIndex igniteIndex = indices.get(indexId);

                            Map<Integer, IgniteTable> resTbls = new HashMap<>(tables);

                            IgniteTable igniteTable = resTbls.computeIfPresent(tableId,
                                    (k, v) -> IgniteTableImpl.copyOf((IgniteTableImpl) v));

                            assert igniteTable != null : "Table " + tableId + " was not found";

                            igniteTable.addIndex(igniteIndex);

                            return resTbls;
                        });
                    }));

            schemasVv.update(causalityToken, (schemas, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(e);
                }

                return updatedTables.thenCombine(updatedIndices, (tables, indices) -> inBusyLock(busyLock, () -> {
                    Map<String, IgniteSchema> res = new HashMap<>(schemas);

                    IgniteSchema schema = res.compute(DEFAULT_SCHEMA_NAME,
                            (k, v) -> v == null ? new IgniteSchema(k, causalityToken) : IgniteSchema.copy(v, causalityToken));

                    schema.addTable(tables.get(tableId));

                    schema.addIndex(indexId, indices.get(indexId));

                    return res;
                }));
            }));

            // this stub is necessary for observing pk index creation.
            schemasVv.whenComplete((token, stringIgniteSchemaMap, throwable) -> {
                CompletableFuture<?> pkFut = pkIdxReady.get(tableId);
                // this listener is called repeatedly on node stop.
                if (pkFut != null) {
                    pkFut.complete(null);
                }
            });

            return calciteSchemaVv.get(causalityToken);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static Index<?> newIndex(TableImpl table, int indexId, IndexDescriptor descriptor) {
        if (descriptor instanceof SortedIndexDescriptor) {
            return new SortedIndexImpl(indexId, table.internalTable(), (SortedIndexDescriptor) descriptor);
        } else {
            return new HashIndex(indexId, table.internalTable(), descriptor);
        }
    }

    /**
     * Index dropped callback method deregisters index from Calcite schema.
     *
     * @param schemaName Schema name.
     * @param indexId Index id.
     * @param causalityToken Causality token.
     * @return Schema registration future.
     */
    public CompletableFuture<?> onIndexDropped(String schemaName, int tableId, int indexId, long causalityToken) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            CompletableFuture<IgniteIndex> removedIndexFuture = new CompletableFuture<>();

            indicesVv.update(causalityToken, (indices, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    removedIndexFuture.completeExceptionally(e);

                    return failedFuture(e);
                }

                Map<Integer, IgniteIndex> resIdxs = new HashMap<>(indices);

                IgniteIndex rmvIdx = resIdxs.remove(indexId);

                removedIndexFuture.complete(rmvIdx);

                return completedFuture(resIdxs);
            }));

            CompletableFuture<Map<Integer, IgniteTable>> updatedTables = tablesVv.update(causalityToken, (tables, e) ->
                    inBusyLock(busyLock, () -> {
                        if (e != null) {
                            return failedFuture(e);
                        }

                        Map<Integer, IgniteTable> resTbls = new HashMap<>(tables);

                        IgniteTable table = resTbls.computeIfPresent(tableId, (k, v) -> IgniteTableImpl.copyOf((IgniteTableImpl) v));

                        if (table == null) {
                            return completedFuture(resTbls);
                        } else {
                            return removedIndexFuture.thenApply(rmvIndex -> {
                                table.removeIndex(rmvIndex.name());

                                return resTbls;
                            });
                        }
                    }));

            schemasVv.update(causalityToken, (schemas, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(e);
                }

                Map<String, IgniteSchema> res = new HashMap<>(schemas);

                IgniteSchema schema = res.compute(schemaName,
                        (k, v) -> v == null ? new IgniteSchema(schemaName, causalityToken) : IgniteSchema.copy(v, causalityToken));

                schema.removeIndex(indexId);

                return updatedTables.thenApply(tables -> {
                    IgniteTable table = tables.get(tableId);

                    if (table != null) {
                        schema.addTable(tables.get(tableId));
                    }

                    return res;
                });
            }));

            return calciteSchemaVv.get(causalityToken);
        } finally {
            busyLock.leaveBusy();
        }
    }
}
