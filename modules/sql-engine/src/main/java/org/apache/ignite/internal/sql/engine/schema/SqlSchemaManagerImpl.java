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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableIntList;
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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.schema.DefaultValueGenerator;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManager;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.ErrorGroups.Common;

/**
 * Implementation of {@link SqlSchemaManager} backed by {@link CatalogService}.
 */
public class SqlSchemaManagerImpl implements SqlSchemaManager {

    private final CatalogManager catalogManager;
    private final SqlStatisticManager sqlStatisticManager;

    private final Cache<Integer, IgniteSchemas> schemaCache;

    /**
     * Table cache by (tableId, tableVersion).
     * Only data that included in a catalog table descriptor itself is up-to-date.
     * Table related information from other object is not reliable.
     */
    private final Cache<CacheKey, IgniteTableImpl> tableCache;

    /** Index cache by (indexId, indexStatus). */
    private final Cache<Long, IgniteIndex> indexCache;

    /** Table cache by (catalogVersion, tableId). Includes all table related information. */
    private final Cache<Long, ActualIgniteTable> fullDataTableCache;

    /** Constructor. */
    public SqlSchemaManagerImpl(
            CatalogManager catalogManager,
            SqlStatisticManager sqlStatisticManager,
            CacheFactory factory,
            int cacheSize
    ) {
        this.catalogManager = catalogManager;
        this.sqlStatisticManager = sqlStatisticManager;
        this.schemaCache = factory.create(cacheSize);
        this.tableCache = factory.create(cacheSize);
        this.indexCache = factory.create(cacheSize);
        this.fullDataTableCache = factory.create(cacheSize);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSchemas schemas(int catalogVersion) {
        return schemaCache.get(
                catalogVersion,
                version -> createRootSchema(catalogManager.catalog(version))
        );
    }


    /** {@inheritDoc} */
    @Override
    public IgniteSchemas schemas(long timestamp) {
        int catalogVersion = catalogManager.activeCatalogVersion(timestamp);

        return schemas(catalogVersion);
    }

    /** {@inheritDoc} */
    @Override
    public int catalogVersion(long timestamp) {
        return catalogManager.activeCatalogVersion(timestamp);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> schemaReadyFuture(int catalogVersion) {
        // SqlSchemaManager creates SQL schema lazily on-demand, thus waiting for Catalog version is enough.
        if (catalogManager.latestCatalogVersion() >= catalogVersion) {
            return nullCompletedFuture();
        }

        return catalogManager.catalogReadyFuture(catalogVersion);
    }

    @Override
    public IgniteTable table(int catalogVersion, int tableId) {
        return fullDataTableCache.get(cacheKey(catalogVersion, tableId), key -> {
            IgniteSchemas rootSchema = schemaCache.get(catalogVersion);

            // Retrieve table from the schema (if it exists).
            if (rootSchema != null) {
                SchemaPlus schemaPlus = rootSchema.root();

                for (String name : schemaPlus.subSchemas().getNames(LikePattern.any())) {
                    SchemaPlus subSchema = schemaPlus.subSchemas().get(name);

                    assert subSchema != null : name;

                    IgniteSchema schema = subSchema.unwrap(IgniteSchema.class);

                    assert schema != null : "unknown schema " + subSchema;

                    // Schema contains a wrapper for IgniteTable that includes actual information for a table (indexes, etc).
                    ActualIgniteTable table = (ActualIgniteTable) schema.tableByIdOpt(tableId);

                    if (table != null) {
                        return table;
                    }
                }
            }

            // Load actual table information from the catalog.

            Catalog catalog = catalogManager.catalog(catalogVersion);

            if (catalog == null) {
                throw new IgniteInternalException(Common.INTERNAL_ERR, "Catalog of given version not found: " + catalogVersion);
            }

            CatalogTableDescriptor tableDescriptor = catalog.table(tableId);

            if (tableDescriptor == null) {
                throw new IgniteInternalException(Common.INTERNAL_ERR, "Table with given id not found: " + tableId);
            }

            CacheKey tableKey = tableCacheKey(tableDescriptor.id(), tableDescriptor.updateTimestamp());

            IgniteTableImpl igniteTable = tableCache.get(tableKey, (x) -> {
                TableDescriptor descriptor = createTableDescriptorForTable(catalog, tableDescriptor);
                return createTableDataOnlyTable(catalog, tableDescriptor, descriptor);
            });

            Map<String, IgniteIndex> tableIndexes = getIndexes(catalog,
                    tableDescriptor.id(),
                    tableDescriptor.primaryKeyIndexId()
            );

            return new ActualIgniteTable(igniteTable, tableIndexes);
        });
    }

    private static long cacheKey(int part1, int part2) {
        long cacheKey = part1;
        cacheKey <<= 32;
        return cacheKey | part2;
    }

    private static CacheKey tableCacheKey(int tableId, HybridTimestamp modificationTimestamp) {
        return new CacheKey(tableId, modificationTimestamp.longValue());
    }

    private IgniteSchemas createRootSchema(Catalog catalog) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(false);

        for (CatalogSchemaDescriptor schemaDescriptor : catalog.schemas()) {
            IgniteSchema igniteSchema = createSqlSchema(catalog, schemaDescriptor);
            rootSchema.add(igniteSchema.getName(), igniteSchema);
        }

        return new IgniteSchemas(rootSchema, catalog.version());
    }

    private IgniteSchema createSqlSchema(Catalog catalog, CatalogSchemaDescriptor schemaDescriptor) {
        int catalogVersion = catalog.version();
        String schemaName = schemaDescriptor.name();

        int numTables = schemaDescriptor.tables().length;
        List<IgniteDataSource> schemaDataSources = new ArrayList<>(numTables);

        // Assemble sql-engine.TableDescriptors as they are required by indexes.
        for (CatalogTableDescriptor tableDescriptor : schemaDescriptor.tables()) {
            CacheKey tableKey = tableCacheKey(tableDescriptor.id(), tableDescriptor.updateTimestamp());

            // Load cached table by (id, version)
            IgniteTableImpl igniteTable = tableCache.get(tableKey, (k) -> {
                TableDescriptor descriptor = createTableDescriptorForTable(catalog, tableDescriptor);
                return createTableDataOnlyTable(catalog, tableDescriptor, descriptor);
            });

            // Get actual indices
            Map<String, IgniteIndex> tableIndexes = getIndexes(catalog,
                    tableDescriptor.id(),
                    tableDescriptor.primaryKeyIndexId()
            );

            // Store a wrapper for the table that includes actual information for a table (indexes, etc),
            // because the cached table entry (id, version) may not include up-to-date information on indexes.
            schemaDataSources.add(new ActualIgniteTable(igniteTable, tableIndexes));
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
            RelCollation outputCollation,
            IgniteDistribution distribution,
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

        return new IgniteIndex(
                indexDescriptor.id(), indexDescriptor.name(), type, distribution, outputCollation, primaryKey
        );
    }

    private TableDescriptor createTableDescriptorForTable(Catalog catalog, CatalogTableDescriptor descriptor) {
        List<CatalogTableColumnDescriptor> columns = descriptor.columns();
        List<ColumnDescriptor> colDescriptors = new ArrayList<>(columns.size() + 2);

        for (int i = 0; i < columns.size(); i++) {
            CatalogTableColumnDescriptor col = columns.get(i);
            boolean key = descriptor.isPrimaryKeyColumn(col.name());
            ColumnDescriptor columnDescriptor = createColumnDescriptor(col, key, i);

            colDescriptors.add(columnDescriptor);
        }

        if (Commons.implicitPkEnabled()) {
            int implicitPkColIdx = descriptor.columnIndex(Commons.IMPLICIT_PK_COL_NAME);

            if (implicitPkColIdx != -1) {
                colDescriptors.set(implicitPkColIdx, injectDefault(colDescriptors.get(implicitPkColIdx)));
            }
        }

        // Add virtual column.
        colDescriptors.add(createPartitionVirtualColumn(columns.size(), Commons.PART_COL_NAME, NativeTypes.INT64));
        colDescriptors.add(createPartitionVirtualColumn(columns.size() + 1, Commons.PART_COL_NAME_LEGACY1, NativeTypes.INT32));
        colDescriptors.add(createPartitionVirtualColumn(columns.size() + 2, Commons.PART_COL_NAME_LEGACY2, NativeTypes.INT32));

        CatalogZoneDescriptor zoneDescriptor = Objects.requireNonNull(catalog.zone(descriptor.zoneId()));
        CatalogSchemaDescriptor schemaDescriptor = Objects.requireNonNull(catalog.schema(descriptor.schemaId()));
        IgniteDistribution distribution = createDistribution(descriptor, schemaDescriptor.name(), zoneDescriptor.name());

        return new TableDescriptorImpl(colDescriptors, distribution);
    }

    private IgniteDistribution createDistribution(CatalogTableDescriptor descriptor, String schemaName, String zoneName) {
        IntList colocationColumns = IntArrayList.toList(descriptor.colocationColumns().intStream()
                .map(descriptor::columnIndexById));

        int tableId = descriptor.id();
        int zoneId = descriptor.zoneId();

        String label = TraitUtils.affinityDistributionLabel(schemaName, descriptor.name(), zoneName);

        return IgniteDistributions.affinity(colocationColumns, tableId, zoneId, label);
    }

    private static ColumnDescriptorImpl createPartitionVirtualColumn(int logicalIndex, String partColName, NativeType type) {
        return new ColumnDescriptorImpl(
                partColName,
                false,
                true,
                true,
                true,
                logicalIndex,
                type,
                DefaultValueStrategy.DEFAULT_COMPUTED,
                () -> {
                    throw new AssertionError("Partition virtual column is generated by a function");
                }
        );
    }

    private static ColumnDescriptor injectDefault(ColumnDescriptor desc) {
        assert Commons.implicitPkEnabled() && Commons.IMPLICIT_PK_COL_NAME.equals(desc.name()) : desc;

        return new ColumnDescriptorImpl(
                desc.name(),
                desc.key(),
                true,
                false,
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

        return new CatalogColumnDescriptor(
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
    }

    private IgniteTableImpl createTableDataOnlyTable(
            Catalog catalog,
            CatalogTableDescriptor table,
            TableDescriptor descriptor
    ) {
        Map<String, IgniteIndex> tableIndexes = getIndexes(catalog,
                table.id(),
                table.primaryKeyIndexId()
        );

        CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(catalog, table.zoneId());

        return createTable(table, descriptor, tableIndexes, zoneDescriptor, sqlStatisticManager);
    }

    private Map<String, IgniteIndex> getIndexes(Catalog catalog, int tableId, int primaryKeyIndexId) {
        Map<String, IgniteIndex> tableIndexes = new HashMap<>();
        CatalogTableDescriptor table = catalog.table(tableId);
        assert table != null;

        for (CatalogIndexDescriptor indexDescriptor : catalog.indexes(tableId)) {
            if (indexDescriptor.status() != AVAILABLE) {
                continue;
            }

            String indexName = indexDescriptor.name();
            long indexKey = cacheKey(indexDescriptor.id(), indexDescriptor.status().id());

            IgniteIndex schemaIndex = indexCache.get(indexKey, (x) -> {
                RelCollation outputCollation = IgniteIndex.createIndexCollation(indexDescriptor, table);

                CatalogZoneDescriptor zoneDescriptor = Objects.requireNonNull(catalog.zone(table.zoneId()));
                CatalogSchemaDescriptor schemaDescriptor = Objects.requireNonNull(catalog.schema(table.schemaId()));
                IgniteDistribution distribution = createDistribution(table, schemaDescriptor.name(), zoneDescriptor.name());

                return createSchemaIndex(
                        indexDescriptor,
                        outputCollation,
                        distribution,
                        indexDescriptor.id() == primaryKeyIndexId
                );
            });

            tableIndexes.put(indexName, schemaIndex);
        }

        return tableIndexes;
    }

    private static CatalogZoneDescriptor getZoneDescriptor(Catalog catalog, int zoneId) {
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);
        assert zoneDescriptor != null : "Zone is not found in schema: " + zoneId;

        return zoneDescriptor;
    }

    private static IgniteTableImpl createTable(
            CatalogTableDescriptor catalogTableDescriptor,
            TableDescriptor tableDescriptor,
            Map<String, IgniteIndex> indexes,
            CatalogZoneDescriptor zoneDescriptor,
            SqlStatisticManager sqlStatisticManager
    ) {
        IgniteIndex primaryIndex = indexes.values().stream()
                .filter(IgniteIndex::primaryKey)
                .findFirst()
                .orElseThrow();

        // We do not need any index other than the primary index,
        // all other indexes are stored in full table data cache.
        Map<String, IgniteIndex> primaryKeyOnlyMap = Map.of(primaryIndex.name(), primaryIndex);

        ImmutableIntList primaryKeyColumns = primaryIndex.collation().getKeys();

        int tableId = catalogTableDescriptor.id();
        String tableName = catalogTableDescriptor.name();

        IgniteStatistic statistic = new IgniteStatistic(() -> sqlStatisticManager.tableSize(tableId), tableDescriptor.distribution());

        return new IgniteTableImpl(
                tableName,
                tableId,
                catalogTableDescriptor.latestSchemaVersion(),
                catalogTableDescriptor.updateTimestamp().longValue(),
                tableDescriptor,
                primaryKeyColumns,
                statistic,
                primaryKeyOnlyMap,
                zoneDescriptor.partitions(),
                zoneDescriptor.id()
        );
    }

    private static class CacheKey {
        final int id;
        final long timestamp;

        private CacheKey(int id, long timestamp) {
            this.id = id;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return id == cacheKey.id && timestamp == cacheKey.timestamp;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, timestamp);
        }
    }

    private static class ActualIgniteTable extends AbstractIgniteDataSource implements IgniteTable {

        /** Cached table by id and version. */
        private final IgniteTableImpl table;

        /** Index map with up-to-date information. */
        private final Map<String, IgniteIndex> indexMap;

        ActualIgniteTable(IgniteTableImpl igniteTable, Map<String, IgniteIndex> indexMap) {
            super(igniteTable.name(), igniteTable.id(), igniteTable.version(), igniteTable.timestamp(), igniteTable.descriptor(),
                    igniteTable.getStatistic());

            this.table = igniteTable;
            this.indexMap = indexMap;
        }

        @Override
        protected TableScan toRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable relOptTbl, List<RelHint> hints) {
            return table.toRel(cluster, traitSet, relOptTbl, hints);
        }

        @Override
        public boolean isUpdateAllowed(int colIdx) {
            return table.isUpdateAllowed(colIdx);
        }

        @Override
        public RelDataType rowTypeForInsert(IgniteTypeFactory factory) {
            return table.rowTypeForInsert(factory);
        }

        @Override
        public RelDataType rowTypeForUpdate(IgniteTypeFactory factory) {
            return table.rowTypeForUpdate(factory);
        }

        @Override
        public RelDataType rowTypeForDelete(IgniteTypeFactory factory) {
            return table.rowTypeForDelete(factory);
        }

        @Override
        public ImmutableIntList keyColumns() {
            return table.keyColumns();
        }

        @Override
        public Supplier<PartitionCalculator> partitionCalculator() {
            return table.partitionCalculator();
        }

        @Override
        public Map<String, IgniteIndex> indexes() {
            return indexMap;
        }

        @Override
        public int partitions() {
            return table.partitions();
        }

        @Override
        public int zoneId() {
            return table.zoneId();
        }
    }
}
