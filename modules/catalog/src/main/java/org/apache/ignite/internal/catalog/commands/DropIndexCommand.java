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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.indexOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.DropIndexEntry;
import org.apache.ignite.internal.catalog.storage.RemoveIndexEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that drops index with specified name.
 *
 * <ul>
 *     <li>If the index never was {@link CatalogIndexStatus#AVAILABLE}, it removes it from the Catalog right away.</li>
 *     <li>If it is currently {@link CatalogIndexStatus#AVAILABLE}, moves it to {@link CatalogIndexStatus#STOPPING}</li>
 *     <li>If it is already {@link CatalogIndexStatus#STOPPING}, fails (as the index is already dropped).</li>
 * </ul>
 *
 * <p>Not to be confused with {@link RemoveIndexCommand}.
 */
public class DropIndexCommand extends AbstractIndexCommand {
    /** Returns builder to create a command to drop index with specified name. */
    public static DropIndexCommandBuilder builder() {
        return new Builder();
    }

    /**
     * Constructor.
     *
     * @param schemaName Name of the schema to look up index in. Should not be null or blank.
     * @param indexName Name of the index to drop. Should not be null or blank.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private DropIndexCommand(String schemaName, String indexName) throws CatalogValidationException {
        super(schemaName, indexName);
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        CatalogIndexDescriptor index = indexOrThrow(schema, indexName);

        CatalogTableDescriptor table = catalog.table(index.tableId());

        assert table != null : format("Index refers to non existing table [catalogVersion={}, indexId={}, tableId={}]",
                catalog.version(), index.id(), index.tableId());

        if (table.primaryKeyIndexId() == index.id()) {
            throw new CatalogValidationException("Dropping primary key index is not allowed");
        }

        switch (index.status()) {
            case REGISTERED:
            case BUILDING:
                return List.of(new RemoveIndexEntry(index.id()));
            case AVAILABLE:
                return List.of(new DropIndexEntry(index.id(), index.tableId()));
            default:
                throw new IllegalStateException("Unknown index status: " + index.status());
        }
    }

    private static class Builder implements DropIndexCommandBuilder {
        private String schemaName;

        private String indexName;

        @Override
        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;

            return this;
        }

        @Override
        public Builder indexName(String indexName) {
            this.indexName = indexName;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new DropIndexCommand(
                    schemaName,
                    indexName
            );
        }
    }
}
