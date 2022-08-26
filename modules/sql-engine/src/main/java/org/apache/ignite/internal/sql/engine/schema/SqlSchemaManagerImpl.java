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
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

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
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.DefaultValueProvider.Type;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
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

    private final SchemaManager schemaManager;

    private final VersionedValue<SchemaPlus> calciteSchemaVv;

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
            Consumer<Function<Long, CompletableFuture<?>>> registry,
            IgniteSpinBusyLock busyLock
    ) {
        this.tableManager = tableManager;
        this.schemaManager = schemaManager;
        schemasVv = new VersionedValue<>(registry, HashMap::new);
        tablesVv = new VersionedValue<>(registry, HashMap::new);
        this.busyLock = busyLock;

        calciteSchemaVv = new VersionedValue<>(null, () -> {
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
                            new IgniteInternalException("Couldn't evaluate sql schemas for causality token: " + token, throwable)
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
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }
        try {
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
        } finally {
            busyLock.leaveBusy();
        }
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
            throw new IgniteInternalException(NODE_STOPPING_ERR, e);
        }
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
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }
        try {
            schemasVv.update(causalityToken, (schemas, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(e);
                }

                Map<String, IgniteSchema> res = new HashMap<>(schemas);

                IgniteSchema schema = res.computeIfAbsent(schemaName, IgniteSchema::new);

                CompletableFuture<IgniteTableImpl> igniteTableFuture = convert(causalityToken, table);

                return tablesVv.update(causalityToken, (tables, ex) -> inBusyLock(busyLock, () -> {
                    if (ex != null) {
                        return failedFuture(ex);
                    }

                    Map<UUID, IgniteTable> resTbls = new HashMap<>(tables);

                    return igniteTableFuture.thenApply(igniteTable -> inBusyLock(busyLock, () -> {
                        resTbls.put(igniteTable.id(), igniteTable);

                        return resTbls;
                    }));
                })).thenCombine(igniteTableFuture, (v, igniteTable) -> inBusyLock(busyLock, () -> {
                    schema.addTable(removeSchema(schemaName, table.name()), igniteTable);

                    return null;
                })).thenCompose(v -> inBusyLock(busyLock, () -> completedFuture(res)));

            }));

            return calciteSchemaVv.get(causalityToken);
        } finally {
            busyLock.leaveBusy();
        }
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
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }
        try {
            schemasVv.update(causalityToken, (schemas, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(e);
                }

                Map<String, IgniteSchema> res = new HashMap<>(schemas);

                IgniteSchema schema = res.computeIfAbsent(schemaName, IgniteSchema::new);

                String calciteTableName = removeSchema(schemaName, tableName);

                InternalIgniteTable table = (InternalIgniteTable) schema.getTable(calciteTableName);

                if (table != null) {
                    schema.removeTable(calciteTableName);

                    return tablesVv.update(causalityToken, (tables, ex) -> inBusyLock(busyLock, () -> {
                        if (ex != null) {
                            return failedFuture(ex);
                        }

                        Map<UUID, IgniteTable> resTbls = new HashMap<>(tables);

                        resTbls.remove(table.id());

                        return completedFuture(resTbls);
                    })).thenCompose(tables -> inBusyLock(busyLock, () -> completedFuture(res)));
                }

                return completedFuture(res);
            }));

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

        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));

        schemas.forEach(newCalciteSchema::add);

        return newCalciteSchema;
    }

    private CompletableFuture<IgniteTableImpl> convert(long causalityToken, TableImpl table) {
        return schemaManager.schemaRegistry(causalityToken, table.tableId())
            .thenApply(schemaRegistry -> inBusyLock(busyLock, () -> convert(table, schemaRegistry)));
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

    private static String removeSchema(String schemaName, String canonicalName) {
        return canonicalName.substring(schemaName.length() + 1);
    }
}
