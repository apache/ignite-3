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
import org.apache.ignite.internal.catalog.IndexAlreadyAvailableValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.storage.MakeIndexAvailableEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * Makes the index available for read-write, switches from the write-only to the read-write state in catalog.
 *
 * @see CatalogIndexDescriptor#writeOnly()
 * @see IndexNotFoundValidationException
 * @see IndexAlreadyAvailableValidationException
 */
public class MakeIndexAvailableCommand extends AbstractIndexCommand {
    /** Returns builder to make an index available for read-write. */
    public static MakeIndexAvailableCommandBuilder builder() {
        return new Builder();
    }

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param indexName Index name.
     * @throws CatalogValidationException If any of the parameters fails validation.
     */
    private MakeIndexAvailableCommand(String schemaName, String indexName) throws CatalogValidationException {
        super(schemaName, indexName);
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        CatalogIndexDescriptor index = indexOrThrow(schema, indexName);

        if (!index.writeOnly()) {
            throw new IndexAlreadyAvailableValidationException(format("Index already available {}.{}", schemaName, indexName));
        }

        CatalogIndexDescriptor updatedIndex;

        if (index instanceof CatalogHashIndexDescriptor) {
            updatedIndex = createReadWriteIndex((CatalogHashIndexDescriptor) index);
        } else if (index instanceof CatalogSortedIndexDescriptor) {
            updatedIndex = createReadWriteIndex((CatalogSortedIndexDescriptor) index);
        } else {
            throw new CatalogValidationException(format("Unsupported index type {}.{} {}", schemaName, indexName, index));
        }

        return List.of(new MakeIndexAvailableEntry(schemaName, updatedIndex));
    }

    private static CatalogIndexDescriptor createReadWriteIndex(CatalogHashIndexDescriptor index) {
        return new CatalogHashIndexDescriptor(
                index.id(),
                index.name(),
                index.tableId(),
                index.unique(),
                index.columns(),
                false
        );
    }

    private static CatalogIndexDescriptor createReadWriteIndex(CatalogSortedIndexDescriptor index) {
        return new CatalogSortedIndexDescriptor(
                index.id(),
                index.name(),
                index.tableId(),
                index.unique(),
                index.columns(),
                false
        );
    }

    private static class Builder implements MakeIndexAvailableCommandBuilder {
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
            return new MakeIndexAvailableCommand(schemaName, indexName);
        }
    }
}
