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

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;

/**
 * A command that adds a new hash index to the catalog.
 */
public class CreateHashIndexCommand extends AbstractCreateIndexCommand {
    /** Returns builder to create a command to create a new hash index. */
    public static CreateHashIndexCommandBuilder builder() {
        return new Builder();
    }

    /**
     * Constructs the object.
     *
     * @param schemaName Name of the schema to create index in. Should not be null or blank.
     * @param indexName Name of the index to create. Should not be null or blank.
     * @param ifNotExists Flag indicating whether the {@code IF NOT EXISTS} was specified.
     * @param tableName Name of the table the index belong to. Should not be null or blank.
     * @param unique A flag denoting whether index keeps at most one row per every key or not.
     * @param columns List of the indexed columns. There should be at least one column.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private CreateHashIndexCommand(String schemaName, String indexName, boolean ifNotExists, String tableName, boolean unique,
            List<String> columns) throws CatalogValidationException {
        super(schemaName, indexName, ifNotExists, tableName, unique, columns);
    }

    @Override
    protected CatalogIndexDescriptor createDescriptor(int indexId, int tableId, int creationCatalogVersion) {
        return new CatalogHashIndexDescriptor(
                indexId, indexName, tableId, unique, creationCatalogVersion, columns
        );
    }

    private static class Builder implements CreateHashIndexCommandBuilder {
        private String schemaName;
        private String indexName;
        private boolean ifNotExists;
        private String tableName;
        private List<String> columns;
        private boolean unique;

        @Override
        public Builder tableName(String tableName) {
            this.tableName = tableName;

            return this;
        }

        @Override
        public Builder unique(boolean unique) {
            this.unique = unique;

            return this;
        }

        @Override
        public Builder columns(List<String> columns) {
            this.columns = columns;

            return this;
        }

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
        public CreateHashIndexCommandBuilder ifNotExists(boolean ifNotExists) {
            this.ifNotExists = ifNotExists;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new CreateHashIndexCommand(
                    schemaName, indexName, ifNotExists, tableName, unique, columns
            );
        }
    }
}
