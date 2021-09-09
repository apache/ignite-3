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

package org.apache.ignite.internal.schema.definition.builder;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.definition.TableSchemaImpl;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.Column;
import org.apache.ignite.schema.definition.PrimaryKey;
import org.apache.ignite.schema.definition.TableSchema;
import org.apache.ignite.schema.definition.builder.TableSchemaBuilder;
import org.apache.ignite.schema.definition.index.ColumnarIndex;
import org.apache.ignite.schema.definition.index.Index;
import org.apache.ignite.schema.definition.index.IndexColumn;
import org.apache.ignite.schema.definition.index.SortedIndex;

import static org.apache.ignite.schema.definition.PrimaryKey.PRIMARY_KEY_NAME;

/**
 * Table builder.
 */
public class TableSchemaBuilderImpl implements TableSchemaBuilder {
    /** Schema name. */
    private final String schemaName;

    /** Table name. */
    private final String tableName;

    /** Columns. */
    private final LinkedHashMap<String, Column> columns = new LinkedHashMap<>();

    /** Indices. */
    private final Map<String, Index> indices = new HashMap<>();

    /** Table primary key. */
    private PrimaryKey primaryKey;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     */
    public TableSchemaBuilderImpl(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    /** {@inheritDoc} */
    @Override public TableSchemaBuilderImpl columns(Column... columns) {
        for (Column column : columns) {
            if (this.columns.put(column.name(), column) != null)
                throw new IllegalArgumentException("Column with same name already exists: columnName=" + column.name());
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableSchemaBuilder withIndex(Index index) {
        if (indices.put(index.name(), index) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + index.name());

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableSchemaBuilder withPrimaryKey(String colName) {
        primaryKey = SchemaBuilders.primaryKey().withColumns(colName).build();

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableSchemaBuilder withPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableSchemaBuilder withHints(Map<String, String> hints) {
        // No op.
        return this;
    }

    /** {@inheritDoc} */
    @Override public TableSchema build() {
        assert schemaName != null : "Table name was not specified.";

        validateIndices(indices.values(), columns.values());

        assert columns.size() > ((SortedIndex)indices.get(PRIMARY_KEY_NAME)).columns().size() : "Key or/and value columns was not defined.";

        return new TableSchemaImpl(
            schemaName,
            tableName,
            columns,
            primaryKey,
            Collections.unmodifiableMap(indices)
        );
    }

    /**
     * Validate indices.
     *
     * @param indices Table indices.
     * @param columns Table columns.
     */
    public static void validateIndices(Collection<Index> indices, Collection<Column> columns) {
        Set<String> colNames = columns.stream().map(Column::name).collect(Collectors.toSet());

        assert indices.stream()
                   .filter(ColumnarIndex.class::isInstance)
                   .map(ColumnarIndex.class::cast)
                   .flatMap(idx -> idx.columns().stream())
                   .map(IndexColumn::name)
                   .allMatch(colNames::contains) : "Index column doesn't exists in schema.";

        Index pkIdx = indices.stream().filter(idx -> PRIMARY_KEY_NAME.equals(idx.name())).findAny().orElse(null);

        assert pkIdx != null : "Primary key index is not configured.";
        assert !((PrimaryKey)pkIdx).affinityColumns().isEmpty() : "Primary key must have one affinity column at least.";

        // Note: E.g. functional index is not columnar index as it index an expression result only.
        assert indices.stream().allMatch(ColumnarIndex.class::isInstance) : "Columnar indices are supported only.";
    }
}
