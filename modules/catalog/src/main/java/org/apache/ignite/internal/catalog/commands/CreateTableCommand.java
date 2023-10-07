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

import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.ensureNoTableIndexOrSysViewExistsWithGivenName;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.zoneOrThrow;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CollectionUtils.copyOrNull;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.NewIndexEntry;
import org.apache.ignite.internal.catalog.storage.NewTableEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that adds a new table to the catalog.
 */
public class CreateTableCommand extends AbstractTableCommand {
    /** Returns builder to create a command to create a new table. */
    public static CreateTableCommandBuilder builder() {
        return new Builder();
    }

    private final List<String> primaryKeyColumns;

    private final List<String> colocationColumns;

    private final List<ColumnParams> columns;

    private final String zoneName;

    /**
     * Constructs the object.
     *
     * @param tableName Name of the table to create. Should not be null or blank.
     * @param schemaName Name of the schema to create table in. Should not be null or blank.
     * @param primaryKeyColumns Name of columns which represent primary key.
     *      Should be subset of columns in param `columns`.
     * @param colocationColumns Name of the columns participating in distribution calculation.
     *      Should be subset of the primary key columns.
     * @param columns List of the columns containing by the table. There should be at least one column.
     * @param zoneName Name of the zone to create table in. Should not be null or blank.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private CreateTableCommand(
            String tableName,
            String schemaName,
            List<String> primaryKeyColumns,
            List<String> colocationColumns,
            List<ColumnParams> columns,
            String zoneName
    ) throws CatalogValidationException {
        super(schemaName, tableName);

        this.primaryKeyColumns = copyOrNull(primaryKeyColumns);
        this.colocationColumns = copyOrNull(colocationColumns);
        this.columns = copyOrNull(columns);
        this.zoneName = zoneName;

        validate();
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        ensureNoTableIndexOrSysViewExistsWithGivenName(schema, tableName);

        CatalogZoneDescriptor zone = zoneOrThrow(catalog, zoneName);

        int id = catalog.objectIdGenState();
        int tableId = id++;
        int pkIndexId = id++;

        CatalogTableDescriptor table = new CatalogTableDescriptor(
                tableId,
                pkIndexId,
                tableName,
                zone.id(),
                CatalogTableDescriptor.INITIAL_TABLE_VERSION,
                columns.stream().map(CatalogUtils::fromParams).collect(toList()),
                primaryKeyColumns,
                colocationColumns,
                INITIAL_CAUSALITY_TOKEN
        );

        String indexName = tableName + "_PK";

        ensureNoTableIndexOrSysViewExistsWithGivenName(schema, indexName);

        CatalogHashIndexDescriptor pkIndex = new CatalogHashIndexDescriptor(
                pkIndexId,
                indexName,
                tableId,
                true,
                primaryKeyColumns
        );

        return List.of(
                new NewTableEntry(table, schemaName),
                new NewIndexEntry(pkIndex, schemaName),
                new ObjectIdGenUpdateEntry(id - catalog.objectIdGenState())
        );
    }

    private void validate() {
        if (nullOrEmpty(columns)) {
            throw new CatalogValidationException("Table should have at least one column");
        }

        Set<String> columnNames = new HashSet<>();

        for (ColumnParams column : columns) {
            if (!columnNames.add(column.name())) {
                throw new CatalogValidationException(format("Column with name '{}' specified more than once", column.name()));
            }
        }

        if (nullOrEmpty(primaryKeyColumns)) {
            throw new CatalogValidationException("Table should have primary key");
        }

        Set<String> primaryKeyColumnsSet = new HashSet<>();

        for (String name : primaryKeyColumns) {
            if (!columnNames.contains(name)) {
                throw new CatalogValidationException(format("PK column '{}' is not part of table", name));
            }

            if (!primaryKeyColumnsSet.add(name)) {
                throw new CatalogValidationException(format("PK column '{}' specified more that once", name));
            }
        }

        if (nullOrEmpty(colocationColumns)) {
            throw new CatalogValidationException("Colocation columns could not be empty");
        }

        Set<String> colocationColumnsSet = new HashSet<>();

        for (String name : colocationColumns) {
            if (!primaryKeyColumnsSet.contains(name)) {
                throw new CatalogValidationException(format("Colocation column '{}' is not part of PK", name));
            }

            if (!colocationColumnsSet.add(name)) {
                throw new CatalogValidationException(format("Colocation column '{}' specified more that once", name));
            }
        }
    }

    /**
     * Implementation of {@link CreateTableCommandBuilder}.
     */
    private static class Builder implements CreateTableCommandBuilder {
        private List<ColumnParams> columns;

        private String schemaName;

        private String tableName;

        private List<String> primaryKeyColumns;

        private List<String> colocationColumns;

        private String zoneName;

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
        public CreateTableCommandBuilder columns(List<ColumnParams> columns) {
            this.columns = columns;

            return this;
        }

        @Override
        public CreateTableCommandBuilder primaryKeyColumns(List<String> primaryKeyColumns) {
            this.primaryKeyColumns = primaryKeyColumns;

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
        public CatalogCommand build() {
            String zoneName = requireNonNullElse(this.zoneName, CatalogService.DEFAULT_ZONE_NAME);

            List<String> colocationColumns = this.colocationColumns != null
                    ? this.colocationColumns
                    : primaryKeyColumns;

            return new CreateTableCommand(
                    tableName,
                    schemaName,
                    primaryKeyColumns,
                    colocationColumns,
                    columns,
                    zoneName
            );
        }
    }
}
