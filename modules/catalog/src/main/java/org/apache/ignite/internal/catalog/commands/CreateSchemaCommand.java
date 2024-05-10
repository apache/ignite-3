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

import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateIdentifier;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.NewSchemaEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * Command to create a new schema.
 */
public class CreateSchemaCommand implements CatalogCommand {

    private final String schemaName;

    private CreateSchemaCommand(String schemaName) {
        validateIdentifier(schemaName, "Name of the schema");

        this.schemaName = schemaName;
    }

    /** {@inheritDoc} */
    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        int id = catalog.objectIdGenState();

        if (catalog.schema(schemaName) != null) {
            throw new CatalogValidationException(format("Schema with name '{}' already exists", schemaName));
        }

        CatalogSchemaDescriptor schema = new CatalogSchemaDescriptor(
                id,
                schemaName,
                new CatalogTableDescriptor[0],
                new CatalogIndexDescriptor[0],
                new CatalogSystemViewDescriptor[0],
                INITIAL_CAUSALITY_TOKEN
        );

        return List.of(
                new NewSchemaEntry(schema),
                new ObjectIdGenUpdateEntry(1)
        );
    }

    /** Returns builder to create a command to create a new schema. */
    public static Builder builder() {
        return new Builder();
    }

    /** Implementation of {@link CreateSchemaCommandBuilder}. */
    public static class Builder implements CreateSchemaCommandBuilder {

        private String name;

        /** {@inheritDoc} */
        @Override
        public CreateSchemaCommandBuilder name(String name) {
            this.name = name;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public CatalogCommand build() {
            return new CreateSchemaCommand(name);
        }
    }
}
