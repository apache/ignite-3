/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.builder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.SchemaTableImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.PartialIndex;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.TableIndex;
import org.apache.ignite.schema.builder.SchemaObjectBuilder;
import org.apache.ignite.schema.builder.SchemaTableBuilder;

/**
 * Table builder.
 */
public class SchemaTableBuilderImpl implements SchemaTableBuilder {
    /** Schema name. */
    private final String schemaName;

    /** Table name. */
    private final String tableName;

    /** Columns. */
    private final LinkedHashMap<String, Column> columns = new LinkedHashMap<>();

    /** Indices. */
    private final Map<String, TableIndex> indices = new HashMap<>();

    /** Primary key fields. */
    private LinkedHashSet<String> pkCols;

    /** Primary key fields. */
    private LinkedHashSet<String> affCols;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     */
    public SchemaTableBuilderImpl(String schemaName, String tableName) {
        this.schemaName = SchemaBuilders.DEFAULT_SCHEMA_NAME;
        this.tableName = tableName;
    }

    /** {@inheritDoc} */
    @Override public SchemaTableBuilderImpl columns(Column... columns) {
        for (int i = 0; i < columns.length; i++) {
            if (this.columns.put(columns[i].name(), columns[i]) != null)
                throw new IllegalStateException("Column with same name already exists: columnName=" + columns[i].name());
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public SchemaTableBuilder pkColumns(String... colNames) {
        pkCols = new LinkedHashSet<>(Arrays.asList(colNames));

        return this;
    }

    /** {@inheritDoc} */
    @Override public SchemaTableBuilder affinityColumns(String... colNames) {
        affCols = new LinkedHashSet<>(Arrays.asList(colNames));

        return this;
    }

    /** {@inheritDoc} */
    @Override public SchemaTableBuilder withindex(TableIndex index) {
        if (PRIMARY_KEY_INDEX_NAME.equals(index.name()))
            throw new IllegalArgumentException("Not valid index name for secondary index: " + index.name());
        else if (indices.put(index.name(), index) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + index.name());

        return this;
    }

    /** {@inheritDoc} */
    @Override public SchemaTableBuilder withHints(Map<String, String> hints) {
        // No op.
        return this;
    }

    /** {@inheritDoc} */
    @Override public SchemaTable build() {
        assert schemaName != null : "Table name was not specified.";
        assert columns.size() >= 2 : "Key or/and value columns was not defined.";

        validatePrimaryKey();

        validateSecondaryIndices();

        return new SchemaTableImpl(
            schemaName,
            tableName,
            columns,
            pkCols,
            affCols,
            Collections.unmodifiableMap(indices)
        );
    }

    /**
     * Validate key columns.
     */
    private void validatePrimaryKey() {
        assert pkCols != null : "PK index is not configured";

        if (affCols == null)
            affCols = pkCols;

        final Set<String> keyCols = pkCols.stream().collect(Collectors.toSet());

        assert keyCols.stream().allMatch(columns::containsKey) : "Key column must be a valid table column.";
        assert affCols != null && !affCols.isEmpty() : "Primary key must have one affinity column at least";
        assert affCols.stream().allMatch(keyCols::contains) : "Affinity column must be a valid key column.";
    }

    /**
     * Validate secondary indices.
     */
    private void validateSecondaryIndices() {
        assert indices.values().stream()
            .filter(SortedIndexBuilderImpl.class::isInstance)
            .map(SortedIndexBuilderImpl.class::cast)
            .flatMap(idx -> idx.columns().stream())
            .map(SortedIndexBuilderImpl.SortedIndexColumnBuilderImpl::name)
            .allMatch(columns::containsKey) : "Indexed column dosn't exists in schema.";

        assert indices.values().stream()
            .filter(HashIndex.class::isInstance)
            .map(HashIndex.class::cast)
            .flatMap(idx -> idx.columns().stream())
            .allMatch(columns::containsKey) : "Indexed column dosn't exists in schema.";

        assert indices.values().stream()
            .filter(PartialIndex.class::isInstance)
            .map(PartialIndex.class::cast)
            .flatMap(idx -> idx.columns().stream())
            .allMatch(c -> columns.containsKey(c.name())) : "Indexed column dosn't exists in schema.";
    }
}
