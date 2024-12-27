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
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.DropIndexEntry;
import org.apache.ignite.internal.catalog.storage.DropSchemaEntry;
import org.apache.ignite.internal.catalog.storage.DropTableEntry;
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

    /**
     * Constructor.
     *
     * @param schemaName Name of the schema.
     * @param cascade Flag indicating forced deletion of a non-empty schema.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private DropSchemaCommand(String schemaName, boolean cascade) throws CatalogValidationException {
        validateIdentifier(schemaName, "Name of the schema");

        this.schemaName = schemaName;
        this.cascade = cascade;
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        if (CatalogUtils.isSystemSchema(schemaName)) {
            throw new CatalogValidationException("System schema can't be dropped [name={}].", schemaName);
        }

        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        if (!cascade && !schema.isEmpty()) {
            throw new CatalogValidationException("Schema '{}' is not empty. Use CASCADE to drop it anyway.", schemaName);
        }

        assert schema.systemViews().length == 0 : "name=" + schemaName + ", count=" + schema.systemViews().length;

        List<UpdateEntry> updateEntries = new ArrayList<>();

        for (CatalogIndexDescriptor idx : schema.indexes()) {
            updateEntries.add(new DropIndexEntry(idx.id()));
        }

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
        public CatalogCommand build() {
            return new DropSchemaCommand(schemaName, cascade);
        }
    }
}
