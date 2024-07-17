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
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateIdentifier;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.indexOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.tableOrThrow;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.RenameIndexEntry;
import org.apache.ignite.internal.catalog.storage.RenameTableEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that changes the name of a table.
 */
public class RenameTableCommand extends AbstractTableCommand {
    /** Returns a builder to create a command to rename a table. */
    public static RenameTableCommandBuilder builder() {
        return new Builder();
    }

    private final String newTableName;

    private RenameTableCommand(String schemaName, String tableName, boolean ifExists, String newTableName)
            throws CatalogValidationException {
        super(schemaName, tableName, ifExists);

        validateIdentifier(newTableName, "New table name");

        this.newTableName = newTableName;
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        ensureNoTableIndexOrSysViewExistsWithGivenName(schema, newTableName);

        CatalogTableDescriptor table = tableOrThrow(schema, tableName);

        String newPkIndexName = pkIndexName(newTableName);

        ensureNoTableIndexOrSysViewExistsWithGivenName(schema, newPkIndexName);

        CatalogIndexDescriptor pkIndex = indexOrThrow(catalog, table.primaryKeyIndexId());

        return List.of(
                new RenameTableEntry(table.id(), newTableName),
                new RenameIndexEntry(pkIndex.id(), newPkIndexName)
        );
    }

    private static class Builder implements RenameTableCommandBuilder {
        private String schemaName;

        private String tableName;

        private boolean ifExists;

        private String newTableName;

        @Override
        public RenameTableCommandBuilder schemaName(String schemaName) {
            this.schemaName = schemaName;

            return this;
        }

        @Override
        public RenameTableCommandBuilder tableName(String tableName) {
            this.tableName = tableName;

            return this;
        }

        @Override
        public RenameTableCommandBuilder ifTableExists(boolean ifExists) {
            this.ifExists = ifExists;

            return this;
        }

        @Override
        public RenameTableCommandBuilder newTableName(String newTableName) {
            this.newTableName = newTableName;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new RenameTableCommand(schemaName, tableName, ifExists, newTableName);
        }
    }
}
