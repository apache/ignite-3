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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.storage.RenameIndexEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that changes the name of an index.
 */
public class RenameIndexCommand extends AbstractIndexCommand {
    /** Returns a builder to create a command to rename an index. */
    public static RenameIndexCommandBuilder builder() {
        return new Builder();
    }

    private final String newIndexName;

    private final boolean ifIndexExists;

    private RenameIndexCommand(String schemaName, String indexName, boolean ifIndexExists, String newIndexName)
            throws CatalogValidationException {
        super(schemaName, indexName);

        validateIdentifier(newIndexName, "New index name");

        this.newIndexName = newIndexName;
        this.ifIndexExists = ifIndexExists;
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        CatalogIndexDescriptor index = indexOrThrow(schema, indexName);

        ensureNoTableIndexOrSysViewExistsWithGivenName(schema, newIndexName);

        return List.of(
                new RenameIndexEntry(index.id(), newIndexName)
        );
    }

    public boolean ifIndexExists() {
        return ifIndexExists;
    }

    private static class Builder implements RenameIndexCommandBuilder {
        private String schemaName;

        private String indexName;

        private boolean ifExists;

        private String newIndexName;

        @Override
        public RenameIndexCommandBuilder schemaName(String schemaName) {
            this.schemaName = schemaName;

            return this;
        }

        @Override
        public RenameIndexCommandBuilder indexName(String indexName) {
            this.indexName = indexName;

            return this;
        }

        @Override
        public RenameIndexCommandBuilder ifIndexExists(boolean ifExists) {
            this.ifExists = ifExists;

            return this;
        }

        @Override
        public RenameIndexCommandBuilder newIndexName(String newIndexName) {
            this.newIndexName = newIndexName;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new RenameIndexCommand(schemaName, indexName, ifExists, newIndexName);
        }
    }
}
