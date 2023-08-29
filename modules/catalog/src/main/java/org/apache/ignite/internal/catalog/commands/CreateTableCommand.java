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
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.ensureNoTableOrIndexExistsWithGivenName;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateColumnParams;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateIdentifier;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.zoneOrThrow;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
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
public class CreateTableCommand extends AbstractCatalogCommand {
    /** Schema name where this new table will be created. */
    private final String schemaName;

    /** Table name. */
    private final String tableName;

    /** Primary key columns. */
    private final List<String> primaryKeyColumns;

    /** Colocation columns. */
    private final List<String> colocationColumns;

    /** Columns. */
    private final List<ColumnParams> columns;

    /** Distribution zone name. */
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
     * @param columns List of the columns containing by the table. There is should be at least one column.
     * @param zoneName Name of the zone to create table in. Should not be null or blank.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    public CreateTableCommand(
            String tableName,
            String schemaName,
            List<String> primaryKeyColumns,
            List<String> colocationColumns,
            List<ColumnParams> columns,
            String zoneName
    ) throws CatalogValidationException {
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.primaryKeyColumns = primaryKeyColumns;
        this.colocationColumns = colocationColumns;
        this.columns = columns;
        this.zoneName = zoneName;

        validate();
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        ensureNoTableOrIndexExistsWithGivenName(schema, tableName);

        CatalogZoneDescriptor zone = zoneOrThrow(catalog, zoneName);

        int id = catalog.objectIdGenState();
        int tableId = id++;
        int pkIndexId = id++;

        CatalogTableDescriptor table = new CatalogTableDescriptor(
                tableId,
                tableName,
                zone.id(),
                CatalogTableDescriptor.INITIAL_TABLE_VERSION,
                columns.stream().map(CatalogUtils::fromParams).collect(toList()),
                primaryKeyColumns,
                colocationColumns
        );

        String indexName = tableName + "_PK";

        ensureNoTableOrIndexExistsWithGivenName(schema, indexName);

        CatalogHashIndexDescriptor pkIndex = new CatalogHashIndexDescriptor(
                pkIndexId,
                indexName,
                tableId,
                true,
                primaryKeyColumns
        );

        return List.of(
                new NewTableEntry(table),
                new NewIndexEntry(pkIndex),
                new ObjectIdGenUpdateEntry(id - catalog.objectIdGenState())
        );
    }

    private void validate() {
        validateIdentifier(schemaName, "Name of the schema");
        validateIdentifier(tableName, "Name of the table");

        if (nullOrEmpty(columns)) {
            throw new CatalogValidationException("Table should have at least one column");
        }

        Set<String> columnNames = new HashSet<>();

        for (ColumnParams column : columns) {
            validateColumnParams(column);

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
}
