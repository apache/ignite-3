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

import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateIdentifier;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schema;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.table;
import static org.apache.ignite.internal.util.CollectionUtils.copyOrNull;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.DropColumnsEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that deletes columns from the table.
 */
public class AlterTableDropColumnCommand extends AbstractTableCommand {
    /** Returns builder to create a command to delete columns from the table. */
    public static AlterTableDropColumnCommandBuilder builder() {
        return new Builder();
    }

    private final Set<String> columns;

    /**
     * Constructs the object.
     *
     * @param tableName Name of the table to delete columns from. Should not be null or blank.
     * @param schemaName Name of the schema the table of interest belongs to. Should not be null or blank.
     * @param ifTableExists Flag indicating whether the {@code IF EXISTS} was specified.
     * @param columns Set of the columns to delete. There should be at least one column.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private AlterTableDropColumnCommand(
            String tableName,
            String schemaName,
            boolean ifTableExists,
            Set<String> columns
    ) throws CatalogValidationException {
        super(schemaName, tableName, ifTableExists, true);

        // Set.copyOf() will throw NPE if any elements of the given set is null
        validate(columns);

        this.columns = copyOrNull(columns);
    }

    @Override
    public List<UpdateEntry> get(UpdateContext updateContext) {
        Catalog catalog = updateContext.catalog();
        CatalogSchemaDescriptor schema = schema(catalog, schemaName, !ifTableExists);
        if (schema == null) {
            return List.of();
        }

        CatalogTableDescriptor table = table(schema, tableName, !ifTableExists);
        if (table == null) {
            return List.of();
        }

        IntSet indexedColumns = aliveIndexesForTable(catalog, table.id())
                .flatMapToInt(AlterTableDropColumnCommand::indexColumnIds)
                .collect(IntOpenHashSet::new, IntSet::add, IntSet::addAll);

        // To validate always in the same order let's sort given columns
        columns.stream().sorted().forEach(columnName -> {
            CatalogTableColumnDescriptor column = table.column(columnName);
            if (column == null) {
                throw new CatalogValidationException(
                        "Column with name '{}' not found in table '{}.{}'.", columnName, schemaName, tableName);
            }

            if (table.isPrimaryKeyColumn(columnName)) {
                throw new CatalogValidationException("Deleting column `{}` belonging to primary key is not allowed.", columnName);
            }

            if (indexedColumns.contains(column.id())) {
                List<String> indexesNames = aliveIndexesForTable(catalog, table.id())
                        .filter(index -> indexColumnIds(index).anyMatch(id -> id == column.id()))
                        .map(CatalogIndexDescriptor::name)
                        .collect(Collectors.toList());

                throw new CatalogValidationException("Deleting column '{}' used by index(es) {}, it is not allowed.",
                        columnName, indexesNames);
            }
        });

        return List.of(
                new DropColumnsEntry(table.id(), columns)
        );
    }

    private static Stream<CatalogIndexDescriptor> aliveIndexesForTable(Catalog catalog, int tableId) {
        return catalog.indexes(tableId).stream().filter(index -> index.status().isAlive());
    }

    private static IntStream indexColumnIds(CatalogIndexDescriptor index) {
        switch (index.indexType()) {
            case HASH:
                return ((CatalogHashIndexDescriptor) index).columnIds().intStream();

            case SORTED:
                return ((CatalogSortedIndexDescriptor) index).columns().stream().mapToInt(CatalogIndexColumnDescriptor::columnId);

            default:
                throw new AssertionError(index.indexType().toString());
        }
    }

    private static void validate(Set<String> columns) {
        if (nullOrEmpty(columns)) {
            throw new CatalogValidationException("Columns not specified.");
        }

        for (String name : columns) {
            validateIdentifier(name, "Name of the column");
        }
    }

    /**
     * Implementation of {@link AlterTableDropColumnCommandBuilder}.
     */
    private static class Builder implements AlterTableDropColumnCommandBuilder {
        private Set<String> columns;

        private String schemaName;

        private String tableName;

        private boolean ifTableExists;

        @Override
        public AlterTableDropColumnCommandBuilder schemaName(String schemaName) {
            this.schemaName = schemaName;

            return this;
        }

        @Override
        public AlterTableDropColumnCommandBuilder tableName(String tableName) {
            this.tableName = tableName;

            return this;
        }

        @Override
        public AlterTableDropColumnCommandBuilder ifTableExists(boolean ifTableExists) {
            this.ifTableExists = ifTableExists;

            return this;
        }

        @Override
        public AlterTableDropColumnCommandBuilder columns(Set<String> columns) {
            this.columns = columns;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new AlterTableDropColumnCommand(
                    tableName,
                    schemaName,
                    ifTableExists,
                    columns
            );
        }
    }
}
