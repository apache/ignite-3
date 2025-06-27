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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.DropTableEntry;
import org.apache.ignite.internal.catalog.storage.RemoveIndexEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that drops table with specified name.
 */
public class DropTableCommand extends AbstractTableCommand {
    /** Returns builder to create a command to drop table with specified name. */
    public static DropTableCommandBuilder builder() {
        return new Builder();
    }

    private DropTableCommand(String schemaName, String tableName, boolean ifExists) throws CatalogValidationException {
        super(schemaName, tableName, ifExists, true);
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

        List<UpdateEntry> updateEntries = new ArrayList<>();

        Arrays.stream(schema.indexes())
                .filter(index -> index.tableId() == table.id())
                .forEach(index -> {
                    // We can remove AVAILABLE/STOPPED index right away as the only reason to have an index in the STOPPING state is to
                    // allow RW transactions started before the index drop to write to it, but as the table is already dropped,
                    // the writes are not possible in any case.
                    updateEntries.add(new RemoveIndexEntry(index.id()));
                });

        updateEntries.add(new DropTableEntry(table.id()));

        return updateEntries;
    }

    /**
     * Implementation of {@link DropTableCommandBuilder}.
     */
    private static class Builder implements DropTableCommandBuilder {
        private String schemaName;

        private String tableName;

        private boolean ifExists;

        @Override
        public DropTableCommandBuilder schemaName(String schemaName) {
            this.schemaName = schemaName;

            return this;
        }

        @Override
        public DropTableCommandBuilder tableName(String tableName) {
            this.tableName = tableName;

            return this;
        }

        @Override
        public DropTableCommandBuilder ifTableExists(boolean ifTableExists) {
            this.ifExists = ifTableExists;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new DropTableCommand(
                    schemaName,
                    tableName,
                    ifExists
            );
        }
    }
}
