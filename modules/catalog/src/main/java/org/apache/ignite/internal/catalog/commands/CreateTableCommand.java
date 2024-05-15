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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.ensureNoTableIndexOrSysViewExistsWithGivenName;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.ensureZoneContainsTablesStorageProfile;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.zoneOrThrow;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CollectionUtils.copyOrNull;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.MakeIndexAvailableEntry;
import org.apache.ignite.internal.catalog.storage.NewIndexEntry;
import org.apache.ignite.internal.catalog.storage.NewTableEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
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

    private final List<String> colocationColumns;

    private final List<ColumnParams> columns;

    private final String zoneName;

    private String storageProfile;

    /**
     * Constructs the object.
     *
     * @param tableName Name of the table to create. Should not be null or blank.
     * @param schemaName Name of the schema to create table in. Should not be null or blank.
     * @param ifNotExists Flag indicating whether the {@code IF NOT EXISTS} was specified.
     * @param primaryKey Primary key.
     * @param colocationColumns Name of the columns participating in distribution calculation.
     *      Should be subset of the primary key columns.
     * @param columns List of the columns containing by the table. There should be at least one column.
     * @param zoneName Name of the zone to create table in or {@code null} to use the default distribution zone.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private CreateTableCommand(
            String tableName,
            String schemaName,
            boolean ifNotExists,
            TablePrimaryKey primaryKey,
            List<String> colocationColumns,
            List<ColumnParams> columns,
            @Nullable String zoneName,
            String storageProfile
    ) throws CatalogValidationException {
        super(schemaName, tableName, ifNotExists);

        this.primaryKey = primaryKey;
        this.colocationColumns = copyOrNull(colocationColumns);
        this.columns = copyOrNull(columns);
        this.zoneName = zoneName;
        this.storageProfile = storageProfile;

        validate();
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        ensureNoTableIndexOrSysViewExistsWithGivenName(schema, tableName);

        CatalogZoneDescriptor zone;
        if (zoneName == null) {
            if (catalog.defaultZone() == null) {
                throw new CatalogValidationException("The zone is not specified. Please specify zone explicitly or set default one.");
            }

            zone = catalog.defaultZone();
        } else {
            zone = zoneOrThrow(catalog, zoneName);
        }

        if (storageProfile == null) {
            storageProfile = zone.storageProfiles().defaultProfile().storageProfile();
        }

        ensureZoneContainsTablesStorageProfile(zone, storageProfile);

        int id = catalog.objectIdGenState();
        int tableId = id++;
        int pkIndexId = id++;

        CatalogTableDescriptor table = new CatalogTableDescriptor(
                tableId,
                schema.id(),
                pkIndexId,
                tableName,
                zone.id(),
                columns.stream().map(CatalogUtils::fromParams).collect(toList()),
                primaryKey.columns(),
                colocationColumns,
                storageProfile
        );

        String indexName = pkIndexName(tableName);

        ensureNoTableIndexOrSysViewExistsWithGivenName(schema, indexName);
        int txWaitCatalogVersion = catalog.version() + 1;

        CatalogIndexDescriptor pkIndex = createIndexDescriptor(txWaitCatalogVersion, indexName, pkIndexId, tableId);

        return List.of(
                new NewTableEntry(table),
                new NewIndexEntry(pkIndex),
                new MakeIndexAvailableEntry(pkIndexId),
                new ObjectIdGenUpdateEntry(id - catalog.objectIdGenState())
        );
    }

    private void validate() {
        if (nullOrEmpty(columns)) {
            throw new CatalogValidationException("Table should have at least one column.");
        }

        Set<String> columnNames = new HashSet<>();
        for (ColumnParams column : columns) {
            if (!columnNames.add(column.name())) {
                throw new CatalogValidationException(format("Column with name '{}' specified more than once.", column.name()));
            }
        }

        if (primaryKey == null || nullOrEmpty(primaryKey.columns())) {
            throw new CatalogValidationException("Table should have primary key.");
        }

        primaryKey.validate(columns);

        for (ColumnParams column : columns) {
            boolean partOfPk = primaryKey.columns().contains(column.name());
            if (partOfPk) {
                CatalogUtils.ensureSupportedDefault(column.name(), column.defaultValueDefinition());
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
                throw new CatalogValidationException(format("Colocation column '{}' is not part of PK.", name));
            }

            if (!colocationColumnsSet.add(name)) {
                throw new CatalogValidationException(format("Colocation column '{}' specified more that once", name));
            }
        }
    }

    private CatalogIndexDescriptor createIndexDescriptor(int txWaitCatalogVersion, String indexName, int pkIndexId, int tableId) {
        CatalogIndexDescriptor pkIndex;

        if (primaryKey instanceof TableSortedPrimaryKey) {
            TableSortedPrimaryKey sortedPrimaryKey = (TableSortedPrimaryKey) primaryKey;
            List<CatalogIndexColumnDescriptor> indexColumns = new ArrayList<>(sortedPrimaryKey.columns().size());

            for (int i = 0; i < sortedPrimaryKey.columns().size(); i++) {
                String columnName = sortedPrimaryKey.columns().get(i);
                CatalogColumnCollation collation = sortedPrimaryKey.collations().get(i);

                indexColumns.add(new CatalogIndexColumnDescriptor(columnName, collation));
            }

            pkIndex = new CatalogSortedIndexDescriptor(
                    pkIndexId,
                    indexName,
                    tableId,
                    true,
                    AVAILABLE,
                    txWaitCatalogVersion,
                    indexColumns
            );
        } else if (primaryKey instanceof TableHashPrimaryKey) {
            TableHashPrimaryKey hashPrimaryKey = (TableHashPrimaryKey) primaryKey;
            pkIndex = new CatalogHashIndexDescriptor(
                    pkIndexId,
                    indexName,
                    tableId,
                    true,
                    AVAILABLE,
                    txWaitCatalogVersion,
                    hashPrimaryKey.columns()
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
                    storageProfile
            );
        }
    }
}
