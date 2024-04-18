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

import static org.apache.ignite.internal.util.CollectionUtils.copyOrNull;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;

/**
 * A command that adds a new sorted index to the catalog.
 */
public class CreateSortedIndexCommand extends AbstractCreateIndexCommand {
    /** Returns builder to create a command to create a new sorted index. */
    public static CreateSortedIndexCommandBuilder builder() {
        return new Builder();
    }

    private final List<CatalogColumnCollation> collations;

    /**
     * Constructs the object.
     *
     * @param schemaName Name of the schema to create index in. Should not be null or blank.
     * @param indexName Name of the index to create. Should not be null or blank.
     * @param tableName Name of the table the index belong to. Should not be null or blank.
     * @param unique A flag denoting whether index keeps at most one row per every key or not.
     * @param columns List of the indexed columns. There should be at least one column.
     * @param collations List of the columns collations. The size of this list should much size of the columns.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private CreateSortedIndexCommand(String schemaName, String indexName, String tableName, boolean unique, List<String> columns,
            List<CatalogColumnCollation> collations) throws CatalogValidationException {
        super(schemaName, indexName, tableName, unique, columns);

        this.collations = copyOrNull(collations);

        validate();
    }

    @Override
    protected CatalogIndexDescriptor createDescriptor(int indexId, int tableId, int creationCatalogVersion) {
        var indexColumnDescriptors = new ArrayList<CatalogIndexColumnDescriptor>(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            indexColumnDescriptors.add(new CatalogIndexColumnDescriptor(
                    columns.get(i), collations.get(i)
            ));
        }

        return new CatalogSortedIndexDescriptor(
                indexId, indexName, tableId, unique, creationCatalogVersion, indexColumnDescriptors
        );
    }

    private void validate() {
        if (nullOrEmpty(collations)) {
            throw new CatalogValidationException("Collations not specified");
        }

        if (collations.size() != columns.size()) {
            throw new CatalogValidationException("Columns collations doesn't match number of columns");
        }
    }

    private static class Builder implements CreateSortedIndexCommandBuilder {
        private String schemaName;
        private String indexName;
        private String tableName;
        private List<String> columns;
        private List<CatalogColumnCollation> collations;
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
        public CreateSortedIndexCommandBuilder collations(List<CatalogColumnCollation> collations) {
            this.collations = collations;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new CreateSortedIndexCommand(
                    schemaName, indexName, tableName, unique, columns, collations
            );
        }
    }
}
