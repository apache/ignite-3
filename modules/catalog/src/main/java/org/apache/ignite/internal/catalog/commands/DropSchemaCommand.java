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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.DropSchemaEntry;
import org.apache.ignite.internal.catalog.storage.DropTableEntry;
import org.apache.ignite.internal.catalog.storage.RemoveIndexEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that drops a schema with specified name.
 */
public class DropSchemaCommand implements CatalogCommand {
    /** Returns builder to create a command to drop schema with specified name. */
    public static DropSchemaCommandBuilder builder() {
        return new Builder();
    }

    private final String schemaName;

    private final boolean cascade;

    private final boolean ifExists;

    /**
     * Constructor.
     *
     * @param schemaName Name of the schema.
     * @param cascade Flag indicating forced deletion of a non-empty schema.
     * @param ifExists Flag indicating
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private DropSchemaCommand(String schemaName, boolean cascade, boolean ifExists) throws CatalogValidationException {
        validateIdentifier(schemaName, "Name of the schema");

        this.schemaName = schemaName;
        this.cascade = cascade;
        this.ifExists = ifExists;
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Override
    public List<UpdateEntry> get(UpdateContext updateContext) {
        Catalog catalog = updateContext.catalog();
        if (CatalogUtils.isSystemSchema(schemaName)) {
            throw new CatalogValidationException("System schema can't be dropped [name={}].", schemaName);
        }

        CatalogSchemaDescriptor schema;

        if (ifExists) {
            schema = catalog.schema(schemaName);
            if (schema == null) {
                return List.of();
            }
        } else {
            schema = schemaOrThrow(catalog, schemaName);
        }

        if (!cascade && !schema.isEmpty()) {
            throw new CatalogValidationException("Schema '{}' is not empty. Use CASCADE to drop it anyway.", schemaName);
        }

        List<UpdateEntry> updateEntries = new ArrayList<>();

        Arrays.stream(schema.indexes())
                .forEach(index -> {
                    // We can remove AVAILABLE/STOPPED index right away as the only reason to have an index in the STOPPING state is to
                    // allow RW transactions started before the index drop to write to it, but as the table is already dropped,
                    // the writes are not possible in any case.
                    updateEntries.add(new RemoveIndexEntry(index.id()));
                });

        for (CatalogTableDescriptor tbl : schema.tables()) {
            updateEntries.add(new DropTableEntry(tbl.id()));
        }

        updateEntries.add(new DropSchemaEntry(schema.id()));

        return updateEntries;
    }

    /**
     * Implementation of {@link DropSchemaCommandBuilder}.
     */
    private static class Builder implements DropSchemaCommandBuilder {
        private String schemaName;
        private boolean cascade;
        private boolean ifExists;

        @Override
        public DropSchemaCommandBuilder name(String schemaName) {
            this.schemaName = schemaName;

            return this;
        }

        @Override
        public DropSchemaCommandBuilder cascade(boolean cascade) {
            this.cascade = cascade;

            return this;
        }

        @Override
        public DropSchemaCommandBuilder ifExists(boolean ifExists) {
            this.ifExists = ifExists;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new DropSchemaCommand(schemaName, cascade, ifExists);
        }
    }
}
