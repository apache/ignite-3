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
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;

/**
 * CREATE INDEX statement.
 */
public class CreateSortedIndexParams extends AbstractIndexCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Table name. */
    private String tableName;

    /** Indexed columns. */
    private List<String> columns;

    /** Columns collations. */
    private List<CatalogColumnCollation> collations;

    /** Unique index flag. */
    protected boolean unique;

    /**
     * Gets table name.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * Gets indexed columns.
     */
    public List<String> columns() {
        return columns;
    }

    /**
     * Gets columns collations.
     */
    public List<CatalogColumnCollation> collations() {
        return collations;
    }

    /**
     * Returns {@code true} if index is unique, {@code false} otherwise.
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractBuilder<CreateSortedIndexParams, CreateSortedIndexParams.Builder> {
        private Builder() {
            super(new CreateSortedIndexParams());
        }

        /**
         * Set table name.
         *
         * @param tableName Table name.
         * @return {@code this}.
         */
        public Builder tableName(String tableName) {
            params.tableName = tableName;

            return this;
        }

        /**
         * Set columns names.
         *
         * @param columns Columns names.
         * @return {@code this}.
         */
        public Builder columns(List<String> columns) {
            params.columns = columns;

            return this;
        }

        /**
         * Set columns collations.
         *
         * @param collations Columns collations.
         * @return {@code this}.
         */
        public Builder collations(List<CatalogColumnCollation> collations) {
            params.collations = collations;

            return this;
        }

        /**
         * Sets unique flag.
         */
        public Builder unique() {
            params.unique = true;

            return this;
        }
    }
}
