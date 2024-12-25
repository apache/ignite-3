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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.storage.DropSchemaEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.sql.SqlCommon;

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
     * @param cascade forces dropping all its objects together with the schema at once.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private DropSchemaCommand(String schemaName, boolean cascade) throws CatalogValidationException {
        this.schemaName = schemaName;
        this.cascade = cascade;
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        if (SqlCommon.DEFAULT_SCHEMA_NAME.equals(schemaName)) {
            throw new CatalogValidationException("Default schema can't be dropped [name={}].", schemaName);
        }

        if (CatalogService.SYSTEM_SCHEMA_NAME.equals(schemaName)) {
            throw new CatalogValidationException("System schema can't be dropped [name={}].", schemaName);
        }

        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        if (schema.indexes().length > 0 || schema.tables().length > 0 || schema.systemViews().length > 0) {
            if (!cascade) {
                throw new CatalogValidationException("Schema '{}' is not empty. Use CASCADE to drop it anyway.", schemaName);
            }
        }

        return List.of(new DropSchemaEntry(schema.id()));
    }

    /**
     * Implementation of {@link DropSchemaCommandBuilder}.
     */
    private static class Builder implements DropSchemaCommandBuilder {
        private String zoneName;
        private boolean cascade;

        @Override
        public DropSchemaCommandBuilder name(String zoneName) {
            this.zoneName = zoneName;

            return this;
        }

        @Override
        public DropSchemaCommandBuilder cascade(boolean cascade) {
            this.cascade = cascade;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new DropSchemaCommand(zoneName, cascade);
        }
    }
}
