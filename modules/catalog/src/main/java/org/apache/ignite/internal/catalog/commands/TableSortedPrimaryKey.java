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
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Primary key that uses sort index. */
public class TableSortedPrimaryKey extends TablePrimaryKey {

    private final List<CatalogColumnCollation> collations;

    /**
     * Constructor.
     *
     * @param columns List of columns.
     * @param collations List of column collations.
     */
    private TableSortedPrimaryKey(@Nullable String name, List<String> columns, List<CatalogColumnCollation> collations) {
        super(name, columns);
        this.collations = collations != null ? List.copyOf(collations) : List.of();
    }

    /** Column collations. */
    public List<CatalogColumnCollation> collations() {
        return collations;
    }

    /** {@inheritDoc} */
    @Override
    void validate(List<ColumnParams> allColumns) {
        super.validate(allColumns);

        if (columns().size() != collations.size()) {
            throw new CatalogValidationException("Number of collations does not match.");
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(TableSortedPrimaryKey.class, this, "columns", columns(), "collations", collations);
    }

    /** Returns builder to create a primary key that uses a sorted index. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder to create a primary index that uses a sorted index. */
    public static class Builder extends TablePrimaryKeyBuilder<Builder> {
        private @Nullable String name;
        private List<String> columns;

        private List<CatalogColumnCollation> collations;

        Builder() {

        }

        /** Specifies a list of collations for primary key columns. */
        public Builder collations(List<CatalogColumnCollation> collations) {
            this.collations = collations;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        @Override
        public Builder name(@Nullable String name) {
            this.name = name;
            return this;
        }

        /** Crates a primary key that uses a sorted index. */
        @Override
        public TableSortedPrimaryKey build() {
            return new TableSortedPrimaryKey(name, columns, collations);
        }
    }
}
