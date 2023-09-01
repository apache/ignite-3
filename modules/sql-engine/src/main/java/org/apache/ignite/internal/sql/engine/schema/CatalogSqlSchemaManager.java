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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.ConstantValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.FunctionCall;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.schema.DefaultValueGenerator;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link SqlSchemaManager} backed by {@link CatalogService}.
 */
public class CatalogSqlSchemaManager implements SqlSchemaManager {

    private final CatalogManager catalogManager;

    private final ConcurrentMap<Map.Entry<String, Integer>, SchemaPlus> cache;

    /** Constructor. */
    public CatalogSqlSchemaManager(CatalogManager catalogManager, int cacheSize) {
        this.catalogManager = catalogManager;
        this.cache = Caffeine.newBuilder().maximumSize(cacheSize).<Map.Entry<String, Integer>, SchemaPlus>build().asMap();
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public SchemaPlus schema(@Nullable String name, int version) {
        String schemaName = name == null ? DEFAULT_SCHEMA_NAME : name;

        Entry<String, Integer> entry = Map.entry(schemaName, version);
        return cache.computeIfAbsent(entry, (e) -> createSqlSchema(e.getValue(), catalogManager.schema(e.getKey(), e.getValue())));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public SchemaPlus schema(@Nullable String name, long timestamp) {
        String schemaName = name == null ? DEFAULT_SCHEMA_NAME : name;

        int schemaVersion = catalogManager.activeCatalogVersion(timestamp);

        Entry<String, Integer> entry = Map.entry(schemaName, schemaVersion);
        return cache.computeIfAbsent(entry, (e) -> createSqlSchema(e.getValue(), catalogManager.schema(e.getKey(), e.getValue())));
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTable tableById(int id) {
        // Should be removed - this method is used to obtain native types from a table.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> schemaReadyFuture(long version) {
        // SqlSchemaManager creates SQL schema lazily on-demand, thus waiting for Catalog version is enough.
        return catalogManager.catalogReadyFuture((int) version);
    }

    @WithSpan
    private static SchemaPlus createSqlSchema(int version, CatalogSchemaDescriptor schemaDescriptor) {
        String schemaName = schemaDescriptor.name();

        int numTables = schemaDescriptor.tables().length;
        Map<String, Table> schemaTables = new HashMap<>(numTables);
        Int2ObjectMap<TableDescriptor> tableDescriptorMap = new Int2ObjectOpenHashMap<>(numTables);

        // Assemble sql-engine.TableDescriptors as they are required by indexes.
        for (CatalogTableDescriptor tableDescriptor : schemaDescriptor.tables()) {
            TableDescriptor descriptor = createTableDescriptor(tableDescriptor);
            tableDescriptorMap.put(tableDescriptor.id(), descriptor);
        }

        Int2ObjectMap<Map<String, IgniteSchemaIndex>> schemaTableIndexes = new Int2ObjectOpenHashMap<>(schemaDescriptor.indexes().length);

        // Assemble indexes as they are required by tables.
        for (CatalogIndexDescriptor indexDescriptor : schemaDescriptor.indexes()) {
            int tableId = indexDescriptor.tableId();
            TableDescriptor tableDescriptor = tableDescriptorMap.get(tableId);
            assert tableDescriptor != null : "Table is not found in schema: " + tableId;

            String indexName = indexDescriptor.name();
            Map<String, IgniteSchemaIndex> tableIndexes = schemaTableIndexes.computeIfAbsent(tableId, id -> new LinkedHashMap<>());

            IgniteSchemaIndex schemaIndex = createSchemaIndex(indexDescriptor, tableDescriptor);
            tableIndexes.put(indexName, schemaIndex);

            schemaTableIndexes.put(tableId, tableIndexes);
        }

        // Assemble tables.
        for (CatalogTableDescriptor tableDescriptor : schemaDescriptor.tables()) {
            int tableId = tableDescriptor.id();
            String tableName = tableDescriptor.name();
            TableDescriptor descriptor = tableDescriptorMap.get(tableId);
            assert descriptor != null;

            IgniteStatistic statistic = new IgniteStatistic(() -> 0.0d, descriptor.distribution());
            Map<String, IgniteSchemaIndex> tableIndexMap = schemaTableIndexes.getOrDefault(tableId, Collections.emptyMap());

            IgniteSchemaTable schemaTable = new IgniteSchemaTable(tableName, tableId, version, descriptor, statistic, tableIndexMap);

            schemaTables.put(tableName, schemaTable);
        }

        // create root schema
        SchemaPlus rootSchema = Frameworks.createRootSchema(false);
        IgniteCatalogSchema igniteSchema = new IgniteCatalogSchema(schemaName, version, schemaTables);
        return rootSchema.add(schemaName, igniteSchema);
    }

    @WithSpan
    private static IgniteSchemaIndex createSchemaIndex(CatalogIndexDescriptor indexDescriptor, TableDescriptor tableDescriptor) {
        Type type;
        if (indexDescriptor instanceof CatalogSortedIndexDescriptor) {
            type = Type.SORTED;
        } else if (indexDescriptor instanceof CatalogHashIndexDescriptor) {
            type = Type.HASH;
        } else {
            throw new IllegalArgumentException("Unexpected index type: " + indexDescriptor);
        }

        RelCollation indexCollation = IgniteSchemaIndex.createIndexCollation(indexDescriptor, tableDescriptor);
        return new IgniteSchemaIndex(indexDescriptor.name(), type, tableDescriptor.distribution(), indexCollation);
    }

    @WithSpan
    private static TableDescriptor createTableDescriptor(CatalogTableDescriptor descriptor) {
        List<ColumnDescriptor> colDescriptors = new ArrayList<>();
        List<Integer> colocationColumns = new ArrayList<>();

        List<CatalogTableColumnDescriptor> columns = descriptor.columns();
        for (int i = 0; i < columns.size(); i++) {
            CatalogTableColumnDescriptor col = columns.get(i);
            boolean nullable = col.nullable();
            boolean key = descriptor.isPrimaryKeyColumn(col.name());

            DefaultValue defaultVal = col.defaultValue();
            DefaultValueStrategy defaultValueStrategy;
            Supplier<Object> defaultValueSupplier;

            if (defaultVal != null) {
                switch (defaultVal.type()) {
                    case CONSTANT:
                        ConstantValue constantValue = (ConstantValue) defaultVal;
                        if (constantValue.value() == null) {
                            defaultValueStrategy = DefaultValueStrategy.DEFAULT_NULL;
                            defaultValueSupplier = () -> null;
                        } else {
                            defaultValueStrategy = DefaultValueStrategy.DEFAULT_CONSTANT;
                            defaultValueSupplier = constantValue::value;
                        }
                        break;
                    case FUNCTION_CALL:
                        FunctionCall functionCall = (FunctionCall) defaultVal;
                        String functionName = functionCall.functionName().toUpperCase(Locale.US);
                        DefaultValueGenerator defaultValueGenerator = DefaultValueGenerator.valueOf(functionName);
                        defaultValueStrategy = DefaultValueStrategy.DEFAULT_COMPUTED;
                        defaultValueSupplier = defaultValueGenerator::next;
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected default value: ");
                }
            } else {
                defaultValueStrategy = null;
                defaultValueSupplier = null;
            }


            CatalogColumnDescriptor columnDescriptor = new CatalogColumnDescriptor(
                    col.name(),
                    key,
                    nullable,
                    i,
                    col.type(),
                    col.precision(),
                    col.scale(),
                    col.length(),
                    defaultValueStrategy,
                    defaultValueSupplier
            );

            if (descriptor.isColocationColumn(col.name())) {
                colocationColumns.add(i);
            }

            colDescriptors.add(columnDescriptor);
        }

        // TODO Use the actual zone ID after implementing https://issues.apache.org/jira/browse/IGNITE-18426.
        int tableId = descriptor.id();
        IgniteDistribution distribution = IgniteDistributions.affinity(colocationColumns, tableId, tableId);

        return new TableDescriptorImpl(colDescriptors, distribution);
    }
}
