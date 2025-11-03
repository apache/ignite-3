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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.ensureNoTableIndexOrSysViewExistsWithGivenName;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.ensureZoneContainsTablesStorageProfile;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_ZONE_QUORUM_SIZE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.zone;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.STRONG_CONSISTENCY;
import static org.apache.ignite.internal.util.CollectionUtils.copyOrNull;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.TableVersion;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.NewIndexEntry;
import org.apache.ignite.internal.catalog.storage.NewTableEntry;
import org.apache.ignite.internal.catalog.storage.NewZoneEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.SetDefaultZoneEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * A command that adds a new table to the catalog.
 */
public class CreateTableCommand extends AbstractTableCommand {
    /** Returns builder to create a command to create a new table. */
    public static CreateTableCommandBuilder builder() {
        return new Builder();
    }

    private final TablePrimaryKey primaryKey;

    private final @Nullable List<String> colocationColumns;

    private final List<ColumnParams> columns;

    private final String zoneName;

    private final @Nullable String storageProfile;

    private final double staleRowsFraction;
    private final long minStaleRowsCount;

    /**
     * Constructs the object.
     *
     * @param tableName Name of the table to create. Should not be null or blank.
     * @param schemaName Name of the schema to create table in. Should not be null or blank.
     * @param ifNotExists Flag indicating whether the {@code IF NOT EXISTS} was specified.
     * @param primaryKey Primary key.
     * @param colocationColumns Name of the columns participating in distribution calculation. Should be subset of the primary key
     *         columns.
     * @param columns List of the columns containing by the table. There should be at least one column.
     * @param zoneName Name of the zone to create table in or {@code null} to use the default distribution zone.
     * @param validateSystemSchemas Flag indicating whether system schemas should be validated.
     * @param staleRowsFraction A fraction of a partition to be modified before the data is considered to be "stale". Should be in
     *         range [0, 1].
     * @param minStaleRowsCount Minimal number of rows in partition to be modified before the data is considered to be "stale".
     *         Should be non-negative.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private CreateTableCommand(
            String tableName,
            String schemaName,
            boolean ifNotExists,
            TablePrimaryKey primaryKey,
            @Nullable List<String> colocationColumns,
            List<ColumnParams> columns,
            @Nullable String zoneName,
            @Nullable String storageProfile,
            boolean validateSystemSchemas,
            double staleRowsFraction,
            long minStaleRowsCount
    ) throws CatalogValidationException {
        super(schemaName, tableName, ifNotExists, validateSystemSchemas);

        this.primaryKey = primaryKey;
        this.colocationColumns = copyOrNull(colocationColumns);
        this.columns = copyOrNull(columns);
        this.zoneName = zoneName;
        this.storageProfile = storageProfile;
        this.staleRowsFraction = staleRowsFraction;
        this.minStaleRowsCount = minStaleRowsCount;

        validate();
    }

    @Override
    public List<UpdateEntry> get(UpdateContext updateContext) {
        Catalog catalog = updateContext.catalog();
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        if (ifTableExists && schema.table(tableName) != null) {
            return List.of();
        }

        ensureNoTableIndexOrSysViewExistsWithGivenName(schema, tableName);

        int id = catalog.objectIdGenState();
        int tableId = id++;
        int pkIndexId = id++;

        // We will have at max 5 entries if there is lazy default data zone creation action is needed.
        List<UpdateEntry> updateEntries = new ArrayList<>(5);

        CatalogZoneDescriptor zone;
        if (zoneName == null) {
            if (catalog.defaultZone() == null) {
                int zoneId = id++;

                if (catalog.zone(DEFAULT_ZONE_NAME) != null) {
                    throw new CatalogValidationException(
                            "Distribution zone with name '{}' already exists. "
                                    + "Please specify zone name for the new table or set the zone as default",
                            DEFAULT_ZONE_NAME
                    );
                }

                // TODO: https://issues.apache.org/jira/browse/IGNITE-26798
                zone = new CatalogZoneDescriptor(
                        zoneId,
                        DEFAULT_ZONE_NAME,
                        DEFAULT_PARTITION_COUNT,
                        DEFAULT_REPLICA_COUNT,
                        DEFAULT_ZONE_QUORUM_SIZE,
                        IMMEDIATE_TIMER_VALUE,
                        INFINITE_TIMER_VALUE,
                        DEFAULT_FILTER,
                        new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor(DEFAULT_STORAGE_PROFILE))),
                        STRONG_CONSISTENCY
                );

                updateEntries.add(new NewZoneEntry(zone));
                updateEntries.add(new SetDefaultZoneEntry(zone.id()));
            } else {
                zone = catalog.defaultZone();
            }
        } else {
            zone = zone(catalog, zoneName, true);
        }

        assert zone != null;

        String storageProfile = this.storageProfile != null
                ? this.storageProfile
                : zone.storageProfiles().defaultProfile().storageProfile();

        ensureZoneContainsTablesStorageProfile(zone, storageProfile);

        List<CatalogTableColumnDescriptor> columnDescriptors = new ArrayList<>(columns.size());
        for (ColumnParams columnParams : columns) {
            columnDescriptors.add(CatalogUtils.fromParams(columnParams));
        }

        CatalogTableSchemaVersions versions = new CatalogTableSchemaVersions(new TableVersion(columnDescriptors));
        Object2IntMap<String> columnIdByName = new Object2IntOpenHashMap<>();
        // Columns from schemaVersion is used, because apart of original columns former have ids assigned.
        for (CatalogTableColumnDescriptor columnDescriptor : versions.latestVersionColumns()) {
            columnIdByName.put(columnDescriptor.name(), columnDescriptor.id());
        }

        IntList pkColumns = convertNamesToIds(primaryKey.columns(), columnIdByName);
        IntList colocationColumns = convertNamesToIds(this.colocationColumns, columnIdByName);

        CatalogTableDescriptor table = CatalogTableDescriptor.builder()
                .id(tableId)
                .schemaId(schema.id())
                .primaryKeyIndexId(pkIndexId)
                .name(tableName)
                .zoneId(zone.id())
                .schemaVersions(versions)
                .primaryKeyColumns(pkColumns)
                .colocationColumns(colocationColumns)
                .storageProfile(storageProfile)
                .minStaleRowsCount(minStaleRowsCount)
                .staleRowsFraction(staleRowsFraction)
                .build();

        String indexName = primaryKey.name();
        if (indexName == null) {
            indexName = pkIndexName(tableName);
        }

        ensureNoTableIndexOrSysViewExistsWithGivenName(schema, indexName);

        CatalogIndexDescriptor pkIndex = createPkIndexDescriptor(indexName, pkIndexId, table);

        updateEntries.add(new NewTableEntry(table));
        updateEntries.add(new NewIndexEntry(pkIndex));
        updateEntries.add(new ObjectIdGenUpdateEntry(id - catalog.objectIdGenState()));

        return updateEntries;
    }

    @Contract("null, _ -> null; !null, _ -> !null")
    private static @Nullable IntList convertNamesToIds(@Nullable List<String> names, Object2IntMap<String> columnIdByName) {
        if (names == null) {
            return null;
        }

        IntList ids = new IntArrayList(names.size());
        for (String name : names) {
            ids.add(columnIdByName.getInt(name));
        }

        return ids;
    }

    private void validate() {
        if (nullOrEmpty(columns)) {
            throw new CatalogValidationException("Table should have at least one column.");
        }

        Set<String> columnNames = new HashSet<>();
        for (ColumnParams column : columns) {
            if (!columnNames.add(column.name())) {
                throw new CatalogValidationException("Column with name '{}' specified more than once.", column.name());
            }
        }

        if (primaryKey == null || nullOrEmpty(primaryKey.columns())) {
            throw new CatalogValidationException("Table should have primary key.");
        }

        primaryKey.validate(columns);

        for (ColumnParams column : columns) {
            boolean partOfPk = primaryKey.columns().contains(column.name());

            CatalogUtils.ensureTypeCanBeStored(column.name(), column.type());
            if (partOfPk) {
                CatalogUtils.ensureSupportedDefault(column.name(), column.type(), column.defaultValueDefinition());
            } else {
                CatalogUtils.ensureNonFunctionalDefault(column.name(), column.defaultValueDefinition());
            }
        }

        if (nullOrEmpty(colocationColumns)) {
            throw new CatalogValidationException("Colocation columns could not be empty.");
        }

        Set<String> colocationColumnsSet = new HashSet<>();

        for (String name : colocationColumns) {
            if (!primaryKey.columns().contains(name)) {
                throw new CatalogValidationException("Colocation column '{}' is not part of PK.", name);
            }

            if (!colocationColumnsSet.add(name)) {
                throw new CatalogValidationException("Colocation column '{}' specified more that once", name);
            }
        }

        if (!Double.isFinite(staleRowsFraction) || staleRowsFraction > 1 || staleRowsFraction < 0) {
            throw new CatalogValidationException("Stale rows fraction should be in range [0, 1].");
        }

        if (minStaleRowsCount < 0) {
            throw new CatalogValidationException("Minimal stale rows count should be non-negative.");
        }
    }

    private CatalogIndexDescriptor createPkIndexDescriptor(String indexName, int pkIndexId, CatalogTableDescriptor table) {
        CatalogIndexDescriptor pkIndex;

        if (primaryKey instanceof TableSortedPrimaryKey) {
            TableSortedPrimaryKey sortedPrimaryKey = (TableSortedPrimaryKey) primaryKey;
            List<CatalogIndexColumnDescriptor> indexColumns = new ArrayList<>(sortedPrimaryKey.columns().size());

            for (int i = 0; i < sortedPrimaryKey.columns().size(); i++) {
                String columnName = sortedPrimaryKey.columns().get(i);
                CatalogTableColumnDescriptor column = table.column(columnName);

                // Index columns already validated by this point, hence null is not expected.
                assert column != null : columnName;

                CatalogColumnCollation collation = sortedPrimaryKey.collations().get(i);

                indexColumns.add(new CatalogIndexColumnDescriptor(column.id(), collation));
            }

            pkIndex = new CatalogSortedIndexDescriptor(
                    pkIndexId,
                    indexName,
                    table.id(),
                    true,
                    AVAILABLE,
                    indexColumns,
                    true
            );
        } else if (primaryKey instanceof TableHashPrimaryKey) {
            TableHashPrimaryKey hashPrimaryKey = (TableHashPrimaryKey) primaryKey;
            IntList columnIds = new IntArrayList(hashPrimaryKey.columns().size());
            for (String columnName : hashPrimaryKey.columns()) {
                CatalogTableColumnDescriptor column = table.column(columnName);

                // Index columns already validated by this point, hence null is not expected.
                assert column != null : columnName;

                columnIds.add(column.id());
            }
            pkIndex = new CatalogHashIndexDescriptor(
                    pkIndexId,
                    indexName,
                    table.id(),
                    true,
                    AVAILABLE,
                    columnIds,
                    true
            );
        } else {
            throw new IllegalArgumentException("Unexpected primary key type: " + primaryKey);
        }

        return pkIndex;
    }

    /**
     * Implementation of {@link CreateTableCommandBuilder}.
     */
    private static class Builder implements CreateTableCommandBuilder {
        private List<ColumnParams> columns;

        private String schemaName;

        private String tableName;

        private boolean ifNotExists;

        private TablePrimaryKey primaryKey;

        private List<String> colocationColumns;

        private String zoneName;

        private String storageProfile;

        private boolean validateSystemSchemas = true;

        private double staleRowsFraction = CatalogUtils.DEFAULT_STALE_ROWS_FRACTION;
        private long minStaleRowsCount = CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT;

        @Override
        public CreateTableCommandBuilder schemaName(String schemaName) {
            this.schemaName = schemaName;

            return this;
        }

        @Override
        public CreateTableCommandBuilder tableName(String tableName) {
            this.tableName = tableName;

            return this;
        }

        @Override
        public CreateTableCommandBuilder ifTableExists(boolean ifNotExists) {
            this.ifNotExists = ifNotExists;

            return this;
        }

        @Override
        public CreateTableCommandBuilder columns(List<ColumnParams> columns) {
            this.columns = columns;

            return this;
        }

        @Override
        public CreateTableCommandBuilder primaryKey(TablePrimaryKey primaryKey) {
            this.primaryKey = primaryKey;

            return this;
        }

        @Override
        public CreateTableCommandBuilder colocationColumns(List<String> colocationColumns) {
            this.colocationColumns = colocationColumns;

            return this;
        }

        @Override
        public CreateTableCommandBuilder zone(String zoneName) {
            this.zoneName = zoneName;

            return this;
        }

        @Override
        public CreateTableCommandBuilder storageProfile(String storageProfile) {
            this.storageProfile = storageProfile;

            return this;
        }

        @Override
        public CreateTableCommandBuilder validateSystemSchemas(boolean validateSystemSchemas) {
            this.validateSystemSchemas = validateSystemSchemas;

            return this;
        }

        @Override
        public CreateTableCommandBuilder staleRowsFraction(double staleRowsFraction) {
            this.staleRowsFraction = staleRowsFraction;

            return this;
        }

        @Override
        public CreateTableCommandBuilder minStaleRowsCount(long minStaleRowsCount) {
            this.minStaleRowsCount = minStaleRowsCount;

            return this;
        }

        @Override
        public CatalogCommand build() {
            List<String> colocationColumns;

            if (this.colocationColumns != null) {
                colocationColumns = this.colocationColumns;
            } else if (primaryKey != null) {
                colocationColumns = primaryKey.columns();
            } else {
                // All validation is done inside validate method of CreateTableCommand,
                // Pass no colocation columns, because this command is going to be rejected anyway as no primary key is specified.
                colocationColumns = null;
            }

            return new CreateTableCommand(
                    tableName,
                    schemaName,
                    ifNotExists,
                    primaryKey,
                    colocationColumns,
                    columns,
                    zoneName,
                    storageProfile,
                    validateSystemSchemas,
                    staleRowsFraction,
                    minStaleRowsCount
            );
        }
    }
}
