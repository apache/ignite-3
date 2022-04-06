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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.idx.IndexManager;
import org.apache.ignite.internal.idx.InternalSortedIndex;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.sql.engine.extension.SqlExtension.ExternalCatalog;
import org.apache.ignite.internal.sql.engine.extension.SqlExtension.ExternalSchema;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Holds actual schema and mutates it on schema change, requested by Ignite.
 */
public class SqlSchemaManagerImpl implements SqlSchemaManager {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(SqlSchemaManagerImpl.class);

    private final Map<String, IgniteSchema> igniteSchemas = new HashMap<>();

    private final Map<UUID, IgniteTable> tablesById = new ConcurrentHashMap<>();

    private final Map<UUID, IgniteIndex> indexById = new ConcurrentHashMap<>();

    private final Map<String, Schema> externalCatalogs = new HashMap<>();

    private final Runnable onSchemaUpdatedCallback;

    private final TableManager tableManager;

    private final IndexManager indexManager;

    private volatile SchemaPlus calciteSchema;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public SqlSchemaManagerImpl(TableManager tblManager, IndexManager idxManager, Runnable onSchemaUpdatedCallback) {
        tableManager = tblManager;
        indexManager = idxManager;
        this.onSchemaUpdatedCallback = onSchemaUpdatedCallback;

        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));
        calciteSchema = newCalciteSchema;
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus schema(@Nullable String schema) {
        return schema != null ? calciteSchema.getSubSchema(schema) : calciteSchema;
    }

    /** {@inheritDoc} */
    @Override
    @NotNull
    public IgniteTable tableById(UUID id) {
        IgniteTable table = tablesById.get(id);

        // there is a chance that someone tries to resolve table before
        // the distributed event of that table creation has been processed
        // by TableManager, so we need to get in sync with the TableManager
        if (table == null) {
            ensureTableStructuresCreated(id);

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

    /** {@inheritDoc} */
    @Override
    public IgniteIndex indexById(UUID id) throws IgniteInternalException {
        IgniteIndex idx = indexById.get(id);

        if (idx == null) {
            indexManager.getIndexById(id);

            idx = indexById.get(id);
        }

        if (idx == null) {
            throw new IgniteInternalException(
                    IgniteStringFormatter.format("Index not found [idxId={}]", id));
        }

        return idx;
    }

    private void ensureTableStructuresCreated(UUID id) {
        try {
            tableManager.table(id);
        } catch (NodeStoppingException e) {
            // Discard the exception
        }
    }

    /**
     * Register an external catalog under given name.
     *
     * @param name Name of the external catalog.
     * @param catalog Catalog to register.
     */
    public synchronized void registerExternalCatalog(String name, ExternalCatalog catalog) {
        catalog.schemaNames().forEach(schemaName -> registerExternalSchema(name, schemaName, catalog.schema(schemaName)));

        rebuild();
    }

    private void registerExternalSchema(String catalogName, String schemaName, ExternalSchema schema) {
        Map<String, Table> tables = new HashMap<>();

        for (String name : schema.tableNames()) {
            IgniteTable table = schema.table(name);

            tables.put(name, table);
            tablesById.put(table.id(), table);
        }

        SchemaPlus schemaPlus = (SchemaPlus) externalCatalogs.computeIfAbsent(catalogName, n -> Frameworks.createRootSchema(false));

        schemaPlus.add(schemaName, new ExternalSchemaHolder(tables));
    }

    public synchronized void onSchemaCreated(String schemaName) {
        igniteSchemas.putIfAbsent(schemaName, new IgniteSchema(schemaName));
        rebuild();
    }

    public synchronized void onSchemaDropped(String schemaName) {
        igniteSchemas.remove(schemaName);
        rebuild();
    }

    /**
     * OnSqlTypeCreated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public synchronized void onTableCreated(
            String schemaName,
            TableImpl table
    ) {
        IgniteTableImpl igniteTable = createTable(schemaName, table);

        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        schema.addTable(normalizeSchema(schemaName, table.name()), igniteTable);
        tablesById.put(igniteTable.id(), igniteTable);

        rebuild();
    }

    private IgniteTableImpl createTable(String schemaName, TableImpl table) {
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

    /**
     * OnSqlTypeUpdated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public void onTableUpdated(
            String schemaName,
            TableImpl table
    ) {
        IgniteTableImpl igniteTable = createTable(schemaName, table);

        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        String tblName = normalizeSchema(schemaName, table.name());

        // Rebuild indexes collation.
        igniteTable.addIndexes(schema.internalTable(tblName).indexes().values().stream()
                .map(idx -> createIndex(idx.index(), igniteTable))
                .collect(Collectors.toList())
        );

        schema.addTable(tblName, igniteTable);
        tablesById.put(igniteTable.id(), igniteTable);

        rebuild();
    }

    /**
     * OnSqlTypeDropped.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public synchronized void onTableDropped(
            String schemaName,
            String tableName
    ) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        InternalIgniteTable table = (InternalIgniteTable) schema.getTable(tableName);
        if (table != null) {
            table.indexes().values().forEach(indexById::remove);

            tablesById.remove(table.id());
            schema.removeTable(tableName);
        }

        rebuild();
    }

    /**
     * Build new SQL schema when new index is created.
     */
    public void onIndexCreated(String schema, String tblName, InternalSortedIndex idx) {
        InternalIgniteTable tbl = igniteSchemas.get(schema).internalTable(normalizeSchema(schema, tblName));

        tbl.addIndex(createIndex(idx, tbl));

        rebuild();
    }

    /**
     * TODO: https://issues.apache.org/jira/browse/IGNITE-15480
     * columns mapping should be masted on column ID instead of column name.
     */
    private IgniteIndex createIndex(InternalSortedIndex idx, InternalIgniteTable tbl) {
        List<RelFieldCollation> idxFieldsCollation = idx.descriptor().columns().stream()
                .map(c ->
                        new RelFieldCollation(
                                tbl.descriptor().columnDescriptor(c.column().name()).logicalIndex(),
                                c.asc() ? Direction.ASCENDING : Direction.DESCENDING,
                                NullDirection.FIRST
                        )
                ).collect(Collectors.toList());

        IgniteIndex idx0 = new IgniteIndex(RelCollations.of(idxFieldsCollation), idx, tbl);

        indexById.put(idx0.id(), idx0);

        return idx0;
    }

    /**
     * Build new SQL schema when existed index is dropped.
     */
    public void onIndexDropped(String schema, String tblName, String idxName) {
        InternalIgniteTable tbl = igniteSchemas.get(schema).internalTable(normalizeSchema(schema, tblName));

        IgniteIndex idx0 = tbl.removeIndex(idxName);

        if (idx0 != null) {
            IgniteIndex old = indexById.remove(idx0.id());

            if (old == null) {
                LOG.trace(IgniteStringFormatter.format("Index [name={}] not found in inner store.", idxName));
            }
        }

        rebuild();
    }

    private void rebuild() {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);

        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));

        igniteSchemas.forEach(newCalciteSchema::add);
        externalCatalogs.forEach(newCalciteSchema::add);

        calciteSchema = newCalciteSchema;

        onSchemaUpdatedCallback.run();
    }

    private static String normalizeSchema(String schemaName, String canonicalName) {
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
