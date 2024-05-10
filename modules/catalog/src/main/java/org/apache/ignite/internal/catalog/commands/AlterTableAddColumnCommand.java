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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.tableOrThrow;
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
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.NewColumnsEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that adds new columns to the table.
 */
public class AlterTableAddColumnCommand extends AbstractTableCommand {
    /** Returns builder to create a command to add new columns to the table. */
    public static AlterTableAddColumnCommandBuilder builder() {
        return new Builder();
    }

    private final List<ColumnParams> columns;

    /**
     * Constructs the object.
     *
     * @param tableName Name of the table to add new columns to. Should not be null or blank.
     * @param schemaName Name of the schema the table of interest belongs to. Should not be null or blank.
     * @param ifTableExists Flag indicating whether the {@code IF EXISTS} was specified.
     * @param columns List of the columns to add to the table. There should be at least one column.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private AlterTableAddColumnCommand(
            String tableName,
            String schemaName,
            boolean ifTableExists,
            List<ColumnParams> columns
    ) throws CatalogValidationException {
        super(schemaName, tableName, ifTableExists);

        this.columns = copyOrNull(columns);

        validate();
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        CatalogTableDescriptor table = tableOrThrow(schema, tableName);

        List<CatalogTableColumnDescriptor> columnDescriptors = new ArrayList<>();

        for (ColumnParams column : columns) {
            if (table.column(column.name()) != null) {
                throw new CatalogValidationException(
                        format("Column with name '{}' already exists", column.name())
                );
            }

            columnDescriptors.add(fromParams(column));
        }

        return List.of(
                new NewColumnsEntry(table.id(), columnDescriptors)
        );
    }

    private void validate() {
        if (nullOrEmpty(columns)) {
            throw new CatalogValidationException("Columns not specified");
        }

        Set<String> columnNames = new HashSet<>();

        for (ColumnParams column : columns) {
            if (!columnNames.add(column.name())) {
                throw new CatalogValidationException(format("Column with name '{}' specified more than once", column.name()));
            }

            CatalogUtils.ensureNonFunctionalDefault(column.name(), column.defaultValueDefinition());
        }
    }

    /**
     * Implementation of {@link AlterTableAddColumnCommandBuilder}.
     */
    private static class Builder implements AlterTableAddColumnCommandBuilder {
        private List<ColumnParams> columns;

        private String schemaName;

        private String tableName;

        private boolean ifTableExists;

        @Override
        public AlterTableAddColumnCommandBuilder schemaName(String schemaName) {
            this.schemaName = schemaName;

            return this;
        }

        @Override
        public AlterTableAddColumnCommandBuilder tableName(String tableName) {
            this.tableName = tableName;

            return this;
        }

        @Override
        public AlterTableAddColumnCommandBuilder ifTableExists(boolean ifTableExists) {
            this.ifTableExists = ifTableExists;

            return this;
        }

        @Override
        public AlterTableAddColumnCommandBuilder columns(List<ColumnParams> columns) {
            this.columns = columns;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new AlterTableAddColumnCommand(
                    tableName,
                    schemaName,
                    ifTableExists,
                    columns
            );
        }
    }
}
