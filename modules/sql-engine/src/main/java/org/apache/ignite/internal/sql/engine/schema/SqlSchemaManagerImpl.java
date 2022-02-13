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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.causality.OutdatedTokenException;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
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

import static java.util.Comparator.comparingInt;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

/**
 * Holds actual schema and mutates it on schema change, requested by Ignite.
 */
public class SqlSchemaManagerImpl implements SqlSchemaManager {
    // This is simple HashMap since it's used only in synchronized methods.
    private final VersionedValue<Map<String, IgniteSchema>> igniteSchemas;

    private final VersionedValue<Map<UUID, IgniteTable>> tablesById;

    private final VersionedValue<Map<String, Schema>> externalCatalogs;

    private final Runnable onSchemaUpdatedCallback;

    private final MetaStorageManager metaStorageManager;

    private final Consumer<Consumer<Long>> storageRevisionUpdater;

    private final VersionedValue<SchemaPlus> calciteSchema;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public SqlSchemaManagerImpl(
        ConfigurationManager configurationManager,
        MetaStorageManager metaStorageManager,
        Runnable onSchemaUpdatedCallback
    ) {
        this.onSchemaUpdatedCallback = onSchemaUpdatedCallback;
        this.storageRevisionUpdater = c -> configurationManager.configurationRegistry().listenUpdateStorageRevision(rev -> {
            c.accept(rev);

            return completedFuture(null);
        });
        this.metaStorageManager = metaStorageManager;

        igniteSchemas = new VersionedValue<>(storageRevisionUpdater);
        tablesById = new VersionedValue<>(storageRevisionUpdater);
        externalCatalogs = new VersionedValue<>(storageRevisionUpdater);

        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));
        calciteSchema = new VersionedValue<>(storageRevisionUpdater);
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus schema(@Nullable String schema) {
        long token = metaStorageManager.appliedRevision();

        try {
            return schema != null ? calciteSchema.get(token).get().getSubSchema(schema) : calciteSchema.get(token).get();
        }
        catch (InterruptedException | ExecutionException | OutdatedTokenException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    @NotNull
    public IgniteTable tableById(UUID id) {
        long token = metaStorageManager.appliedRevision();

        IgniteTable table;

        try {
            table = tablesById.get(token).get().get(id);
        }
        catch (InterruptedException | ExecutionException | OutdatedTokenException e) {
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
    public synchronized void registerExternalCatalog(String name, ExternalCatalog catalog, long causalityToken) {
        catalog.schemaNames().forEach(schemaName -> registerExternalSchema(name, schemaName, catalog.schema(schemaName), causalityToken));

        try {
            rebuild(causalityToken);
        }
        catch (OutdatedTokenException e) {
            // No-op.
        }
    }

    private void registerExternalSchema(String catalogName, String schemaName, ExternalSchema schema, long causalityToken) {
        Map<String, Table> tables = new HashMap<>();

        tablesById.update(causalityToken, HashMap::new, tblsByIds -> {
            Map<UUID, IgniteTable> tempTables = PatchedMapView.of(tblsByIds, Integer.MAX_VALUE).map();

            for (String name : schema.tableNames()) {
                IgniteTable table = schema.table(name);

                tables.put(name, table);
                tempTables = PatchedMapView.of(tempTables).put(table.id(), table);
            }

            return PatchedMapView.of(tempTables).map();
        });

        externalCatalogs.update(causalityToken, HashMap::new, catalogs -> {
            Map<String, Schema> res = PatchedMapView.of(catalogs).computeIfAbsent(catalogName, n -> Frameworks.createRootSchema(false));

            SchemaPlus schemaPlus = (SchemaPlus)res.get(catalogName);
            schemaPlus.add(schemaName, new ExternalSchemaHolder(tables));

            return res;
        });
    }

    public synchronized void onSchemaCreated(String schemaName, long causalityToken) {
        igniteSchemas.update(
            causalityToken,
            HashMap::new,
            schemas -> PatchedMapView.of(schemas).putIfAbsent(schemaName, new IgniteSchema(schemaName))
        );

        try {
            rebuild(causalityToken);
        }
        catch (OutdatedTokenException e) {
            // No-op.
        }
    }

    public synchronized void onSchemaDropped(String schemaName, long causalityToken) {
        igniteSchemas.update(
            causalityToken,
            HashMap::new,
            schemas -> PatchedMapView.of(schemas).remove(schemaName)
        );

        try {
            rebuild(causalityToken);
        }
        catch (OutdatedTokenException e) {
            // No-op.
        }
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
        igniteSchemas.update(causalityToken, HashMap::new, schemas -> {
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

            tablesById.update(causalityToken, HashMap::new, tables -> {
                try {
                    rebuild(causalityToken);
                }
                catch (OutdatedTokenException e) {
                    // No-op.
                }

                return PatchedMapView.of(tables).put(igniteTable.id(), igniteTable);
            });

            return PatchedMapView.of(schemas).put(schemaName, schema);
        });
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
        igniteSchemas.update(causalityToken, HashMap::new, schemas -> {
            IgniteSchema prevSchema = schemas.computeIfAbsent(schemaName, IgniteSchema::new);
            IgniteSchema schema = new IgniteSchema(prevSchema.getName(), prevSchema.getTableMap());

            InternalIgniteTable table = (InternalIgniteTable) schema.getTable(tableName);

            if (table != null) {
                tablesById.update(causalityToken, HashMap::new, tables -> PatchedMapView.of(tables).remove(table.id()));

                schema.removeTable(tableName);
            }

            try {
                rebuild(causalityToken);
            }
            catch (OutdatedTokenException e) {
                // No-op.
            }

            return PatchedMapView.of(schemas).put(schemaName, schema);
        });
    }

    private void rebuild(long causalityToken) throws OutdatedTokenException {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);

        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));

        igniteSchemas.get(causalityToken).thenAccept(schemas -> schemas.forEach(newCalciteSchema::add));
        externalCatalogs.get(causalityToken).thenAccept(catalogs -> catalogs.forEach(newCalciteSchema::add));

        calciteSchema.set(causalityToken, newCalciteSchema);

        onSchemaUpdatedCallback.run();
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
