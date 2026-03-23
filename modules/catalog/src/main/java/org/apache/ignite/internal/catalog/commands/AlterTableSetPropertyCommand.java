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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schema;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.table;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.AlterTablePropertiesEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.jetbrains.annotations.Nullable;

/**
 * A command that changes table's properties.
 */
public class AlterTableSetPropertyCommand extends AbstractTableCommand {
    /** Returns builder to create a command to add new columns to the table. */
    public static AlterTableSetPropertyCommandBuilder builder() {
        return new Builder();
    }

    private final @Nullable Double staleRowsFraction;
    private final @Nullable Long minStaleRowsCount;

    /**
     * Constructs the object.
     *
     * @param tableName Name of the table to change properties. Should not be null or blank.
     * @param schemaName Name of the schema the table of interest belongs to. Should not be null or blank.
     * @param ifTableExists Flag indicating whether the {@code IF EXISTS} was specified.
     * @param staleRowsFraction A fraction of a partition to be modified before the data is considered to be "stale". Should be in
     *         range [0, 1].
     * @param minStaleRowsCount Minimal number of rows in partition to be modified before the data is considered to be "stale".
     *         Should be non-negative.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private AlterTableSetPropertyCommand(
            String tableName,
            String schemaName,
            boolean ifTableExists,
            @Nullable Double staleRowsFraction,
            @Nullable Long minStaleRowsCount
    ) throws CatalogValidationException {
        super(schemaName, tableName, ifTableExists, true);

        this.staleRowsFraction = staleRowsFraction;
        this.minStaleRowsCount = minStaleRowsCount;

        validate();
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

        if (staleRowsFraction == null
                && minStaleRowsCount == null
        ) {
            return List.of();
        }

        return List.of(
                new AlterTablePropertiesEntry(
                        table.id(),
                        staleRowsFraction,
                        minStaleRowsCount
                )
        );
    }

    private void validate() {
        if (staleRowsFraction != null) {
            if (!Double.isFinite(staleRowsFraction) || staleRowsFraction > 1 || staleRowsFraction < 0) {
                throw new CatalogValidationException("Stale rows fraction should be in range [0, 1].");
            }
        }

        if (minStaleRowsCount != null) {
            if (minStaleRowsCount < 0) {
                throw new CatalogValidationException("Minimal stale rows count should be non-negative.");
            }
        }
    }

    private static class Builder implements AlterTableSetPropertyCommandBuilder {
        private @Nullable Double staleRowsFraction;
        private @Nullable Long minStaleRowsCount;

        private String schemaName;
        private String tableName;

        private boolean ifTableExists;

        @Override
        public AlterTableSetPropertyCommandBuilder schemaName(String schemaName) {
            this.schemaName = schemaName;

            return this;
        }

        @Override
        public AlterTableSetPropertyCommandBuilder tableName(String tableName) {
            this.tableName = tableName;

            return this;
        }

        @Override
        public AlterTableSetPropertyCommandBuilder ifTableExists(boolean ifTableExists) {
            this.ifTableExists = ifTableExists;

            return this;
        }

        @Override
        public AlterTableSetPropertyCommandBuilder staleRowsFraction(double staleRowsFraction) {
            this.staleRowsFraction = staleRowsFraction;

            return this;
        }

        @Override
        public AlterTableSetPropertyCommandBuilder minStaleRowsCount(long minStaleRowsCount) {
            this.minStaleRowsCount = minStaleRowsCount;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new AlterTableSetPropertyCommand(
                    tableName,
                    schemaName,
                    ifTableExists,
                    staleRowsFraction,
                    minStaleRowsCount
            );
        }
    }
}
