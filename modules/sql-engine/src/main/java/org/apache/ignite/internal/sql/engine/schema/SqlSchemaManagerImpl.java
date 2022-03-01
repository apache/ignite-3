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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
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
    private final Map<String, IgniteSchema> igniteSchemas = new HashMap<>();

    private final Map<UUID, IgniteTable> tablesById = new ConcurrentHashMap<>();

    private final Runnable onSchemaUpdatedCallback;

    private final TableManager tableManager;

    private volatile SchemaPlus calciteSchema;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public SqlSchemaManagerImpl(TableManager tableManager, Runnable onSchemaUpdatedCallback) {
        this.onSchemaUpdatedCallback = onSchemaUpdatedCallback;
        this.tableManager = tableManager;

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

    private void ensureTableStructuresCreated(UUID id) {
        try {
            tableManager.table(id);
        } catch (NodeStoppingException e) {
            // Discard the exception
        }
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
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

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

        IgniteTableImpl igniteTable = new IgniteTableImpl(
                new TableDescriptorImpl(colDescriptors),
                table.internalTable(),
                table.schemaView()
        );

        schema.addTable(removeSchema(schemaName, table.name()), igniteTable);
        tablesById.put(igniteTable.id(), igniteTable);

        rebuild();
    }

    /**
     * OnSqlTypeUpdated.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public void onTableUpdated(
            String schemaName,
            TableImpl table
    ) {
        onTableCreated(schemaName, table);
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
            tablesById.remove(table.id());
            schema.removeTable(tableName);
        }

        rebuild();
    }

    private void rebuild() {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);

        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));

        igniteSchemas.forEach(newCalciteSchema::add);

        calciteSchema = newCalciteSchema;

        onSchemaUpdatedCallback.run();
    }

    private static String removeSchema(String schemaName, String canonicalName) {
        return canonicalName.substring(schemaName.length() + 1);
    }
}
