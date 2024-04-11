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

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.ConstantValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.FunctionCall;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.schema.DefaultValueGenerator;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.lang.ErrorGroups.Common;

/**
 * Implementation of {@link SqlSchemaManager} backed by {@link CatalogService}.
 */
public class SqlSchemaManagerImpl implements SqlSchemaManager {

    private final CatalogManager catalogManager;

    private final Cache<Integer, SchemaPlus> schemaCache;

    private final Cache<Long, IgniteTable> tableCache;

    /** Constructor. */
    public SqlSchemaManagerImpl(CatalogManager catalogManager, CacheFactory factory, int cacheSize) {
        this.catalogManager = catalogManager;
        this.schemaCache = factory.create(cacheSize);
        this.tableCache = factory.create(cacheSize);
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus schema(int schemaVersion) {
        return schemaCache.get(
                schemaVersion,
                version -> createRootSchema(catalogManager.catalog(version))
        );
    }


    /** {@inheritDoc} */
    @Override
    public SchemaPlus schema(long timestamp) {
        int catalogVersion = catalogManager.activeCatalogVersion(timestamp);

        return schema(catalogVersion);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> schemaReadyFuture(int version) {
        // SqlSchemaManager creates SQL schema lazily on-demand, thus waiting for Catalog version is enough.
        if (catalogManager.latestCatalogVersion() >= version) {
            return nullCompletedFuture();
        }

        return catalogManager.catalogReadyFuture(version);
    }

    @Override
    public IgniteTable table(int schemaVersion, int tableId) {
        return tableCache.get(tableCacheKey(schemaVersion, tableId), key -> {
            SchemaPlus rootSchema = schemaCache.get(schemaVersion);

            if (rootSchema != null) {
                for (String name : rootSchema.getSubSchemaNames()) {
                    SchemaPlus subSchema = rootSchema.getSubSchema(name);

                    assert subSchema != null : name;

                    IgniteSchema schema = subSchema.unwrap(IgniteSchema.class);

                    assert schema != null : "unknown schema " + subSchema;

                    IgniteTable table = schema.tableByIdOpt(tableId);

                    if (table != null) {
                        return table;
                    }
                }
            }

            Catalog catalog = catalogManager.catalog(schemaVersion);

            if (catalog == null) {
                throw new IgniteInternalException(Common.INTERNAL_ERR, "Catalog of given version not found: " + schemaVersion);
            }

            CatalogTableDescriptor tableDescriptor = catalog.table(tableId);

            if (tableDescriptor == null) {
                throw new IgniteInternalException(Common.INTERNAL_ERR, "Table with given id not found: " + tableId);
            }

            return createTable(catalog, tableDescriptor);
        });
    }

    private static long tableCacheKey(int schemaVersion, int tableId) {
        long cacheKey = schemaVersion;
        cacheKey <<= 32;
        return cacheKey | tableId;
    }

    private static SchemaPlus createRootSchema(Catalog catalog) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(false);

        for (CatalogSchemaDescriptor schemaDescriptor : catalog.schemas()) {
            IgniteSchema igniteSchema = createSqlSchema(catalog, schemaDescriptor);
            rootSchema.add(igniteSchema.getName(), igniteSchema);
        }

        return rootSchema;
    }

    private static IgniteSchema createSqlSchema(Catalog catalog, CatalogSchemaDescriptor schemaDescriptor) {
        int catalogVersion = catalog.version();
        String schemaName = schemaDescriptor.name();

        int numTables = schemaDescriptor.tables().length;
        List<IgniteDataSource> schemaDataSources = new ArrayList<>(numTables);

        // Assemble sql-engine.TableDescriptors as they are required by indexes.
        for (CatalogTableDescriptor tableDescriptor : schemaDescriptor.tables()) {
            schemaDataSources.add(
                    createTable(catalog, tableDescriptor)
            );
        }

        for (CatalogSystemViewDescriptor systemViewDescriptor : schemaDescriptor.systemViews()) {
            int viewId = systemViewDescriptor.id();
            String viewName = systemViewDescriptor.name();
            TableDescriptor descriptor = createTableDescriptorForSystemView(systemViewDescriptor);

            IgniteSystemView schemaTable = new IgniteSystemViewImpl(
                    viewName,
                    viewId,
                    descriptor
            );

            schemaDataSources.add(schemaTable);
        }

        return new IgniteSchema(schemaName, catalogVersion, schemaDataSources);
    }

    private static IgniteIndex createSchemaIndex(
            CatalogIndexDescriptor indexDescriptor,
            TableDescriptor tableDescriptor,
            boolean primaryKey
    ) {
        Type type;
        if (indexDescriptor instanceof CatalogSortedIndexDescriptor) {
            type = Type.SORTED;
        } else if (indexDescriptor instanceof CatalogHashIndexDescriptor) {
            type = Type.HASH;
        } else {
            throw new IllegalArgumentException("Unexpected index type: " + indexDescriptor);
        }

        RelCollation outputCollation = IgniteIndex.createIndexCollation(indexDescriptor, tableDescriptor);
        return new IgniteIndex(
                indexDescriptor.id(), indexDescriptor.name(), type, tableDescriptor.distribution(), outputCollation, primaryKey
        );
    }

    private static TableDescriptor createTableDescriptorForTable(CatalogTableDescriptor descriptor) {
        List<ColumnDescriptor> colDescriptors = new ArrayList<>();

        List<CatalogTableColumnDescriptor> columns = descriptor.columns();
        Object2IntMap<String> columnToIndex = new Object2IntOpenHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            CatalogTableColumnDescriptor col = columns.get(i);
            boolean key = descriptor.isPrimaryKeyColumn(col.name());
            CatalogColumnDescriptor columnDescriptor = createColumnDescriptor(col, key, i);

            columnToIndex.put(col.name(), i);

            colDescriptors.add(columnDescriptor);
        }

        List<Integer> colocationColumns = descriptor.colocationColumns().stream()
                .map(columnToIndex::getInt)
                .collect(Collectors.toList());

        // TODO Use the actual zone ID after implementing https://issues.apache.org/jira/browse/IGNITE-18426.
        int tableId = descriptor.id();
        IgniteDistribution distribution = IgniteDistributions.affinity(colocationColumns, tableId, tableId);

        if (Commons.implicitPkEnabled()) {
            colDescriptors = colDescriptors.stream()
                    .map(column -> {
                        if (Commons.IMPLICIT_PK_COL_NAME.equals(column.name())) {
                            return injectDefault(column);
                        }

                        return column;
                    })
                    .collect(Collectors.toList());
        }

        return new TableDescriptorImpl(colDescriptors, distribution);
    }

    private static ColumnDescriptor injectDefault(ColumnDescriptor desc) {
        assert Commons.implicitPkEnabled() && Commons.IMPLICIT_PK_COL_NAME.equals(desc.name()) : desc;

        return new ColumnDescriptorImpl(
                desc.name(),
                desc.key(),
                true,
                desc.nullable(),
                desc.logicalIndex(),
                desc.physicalType(),
                DefaultValueStrategy.DEFAULT_COMPUTED,
                () -> {
                    throw new AssertionError("Implicit primary key is generated by a function");
                }
        );
    }

    private static TableDescriptor createTableDescriptorForSystemView(CatalogSystemViewDescriptor descriptor) {
        List<ColumnDescriptor> colDescriptors = new ArrayList<>();

        List<CatalogTableColumnDescriptor> columns = descriptor.columns();
        for (int i = 0; i < columns.size(); i++) {
            CatalogTableColumnDescriptor col = columns.get(i);
            CatalogColumnDescriptor columnDescriptor = createColumnDescriptor(col, false, i);

            colDescriptors.add(columnDescriptor);
        }

        IgniteDistribution distribution;
        SystemViewType systemViewType = descriptor.systemViewType();

        switch (systemViewType) {
            case NODE:
                // node name is always the first column.
                distribution = IgniteDistributions.identity(0);
                break;
            case CLUSTER:
                distribution = IgniteDistributions.single();
                break;
            default:
                throw new IllegalArgumentException("Unexpected system view type: " + systemViewType);
        }


        return new TableDescriptorImpl(colDescriptors, distribution);
    }

    private static CatalogColumnDescriptor createColumnDescriptor(CatalogTableColumnDescriptor col, boolean key, int i) {
        boolean nullable = col.nullable();

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
        return columnDescriptor;
    }

    private static IgniteTable createTable(
            Catalog catalog,
            CatalogTableDescriptor tableDescriptor
    ) {
        TableDescriptor descriptor = createTableDescriptorForTable(tableDescriptor);

        Map<String, IgniteIndex> tableIndexes = new HashMap<>();
        for (CatalogIndexDescriptor indexDescriptor : catalog.indexes(tableDescriptor.id())) {
            if (indexDescriptor.status() != AVAILABLE) {
                continue;
            }

            String indexName = indexDescriptor.name();

            IgniteIndex schemaIndex = createSchemaIndex(
                    indexDescriptor,
                    descriptor,
                    indexDescriptor.id() == tableDescriptor.primaryKeyIndexId()
            );

            tableIndexes.put(indexName, schemaIndex);
        }

        int zoneId = tableDescriptor.zoneId();
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);
        assert zoneDescriptor != null : "Zone is not found in schema: " + zoneId;

        return createTable(tableDescriptor, descriptor, tableIndexes, zoneDescriptor.partitions());
    }

    private static IgniteTable createTable(
            CatalogTableDescriptor catalogTableDescriptor,
            TableDescriptor tableDescriptor,
            Map<String, IgniteIndex> indexes,
            int parititions
    ) {
        int tableId = catalogTableDescriptor.id();
        String tableName = catalogTableDescriptor.name();

        // TODO IGNITE-19558: The table is not available at planning stage.
        // Let's fix table statistics keeping in mind IGNITE-19558 issue.
        IgniteStatistic statistic = new IgniteStatistic(() -> 0.0d, tableDescriptor.distribution());

        IgniteIndex primaryIndex = indexes.values().stream()
                .filter(IgniteIndex::primaryKey)
                .findFirst()
                .orElseThrow();

        return new IgniteTableImpl(
                tableName,
                tableId,
                catalogTableDescriptor.tableVersion(),
                tableDescriptor,
                primaryIndex.collation().getKeys(),
                statistic,
                indexes,
                parititions
        );
    }
}
