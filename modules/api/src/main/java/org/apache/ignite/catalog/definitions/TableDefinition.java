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

package org.apache.ignite.catalog.definitions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.IndexType;
import org.jetbrains.annotations.Nullable;

/**
 * Definition of the {@code CREATE TABLE} statement.
 */
public class TableDefinition {
    private final String tableName;

    private final String schemaName;

    private final boolean ifNotExists;

    private final List<ColumnDefinition> columns;

    private final IndexType pkType;

    private final List<ColumnSorted> pkColumns;

    private final List<String> colocationColumns;

    private final String zoneName;

    private final Class<?> keyClass;

    private final Class<?> valueClass;

    private final List<IndexDefinition> indexes;

    private TableDefinition(
            String tableName,
            String schemaName,
            boolean ifNotExists,
            List<ColumnDefinition> columns,
            IndexType pkType,
            List<ColumnSorted> pkColumns,
            List<String> colocationColumns,
            String zoneName,
            Class<?> keyClass,
            Class<?> valueClass,
            List<IndexDefinition> indexes
    ) {
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.ifNotExists = ifNotExists;
        this.columns = columns;
        this.pkType = pkType;
        this.pkColumns = pkColumns;
        this.colocationColumns = colocationColumns;
        this.zoneName = zoneName;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.indexes = indexes;
    }

    /**
     * Creates a builder for the table with the specified name.
     *
     * @param tableName Table name.
     * @return Builder.
     */
    public static Builder builder(String tableName) {
        return new Builder().tableName(tableName);
    }

    /**
     * Returns table name.
     *
     * @return Table name.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * Returns schema name.
     *
     * @return Schema name or {@code null} if not specified.
     */
    public @Nullable String schemaName() {
        return schemaName;
    }

    /**
     * Returns not exists flag.
     *
     * @return {@code true} if {@code IF NOT EXISTS} clause should be added to the statement.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * Returns definitions of the columns.
     *
     * @return List of column definitions or {@code null} if columns are specified using other methods.
     * @see #keyClass()
     * @see #valueClass()
     */
    public @Nullable List<ColumnDefinition> columns() {
        return columns;
    }

    /**
     * Returns primary key type.
     *
     * @return Primary key type or {@code null} if default should be used.
     */
    public @Nullable IndexType primaryKeyType() {
        return pkType;
    }

    /**
     * Returns a list of columns used in the primary key.
     *
     * @return List of columns used in the primary key or {@code null} if the primary key is not specified.
     */
    public @Nullable List<ColumnSorted> primaryKeyColumns() {
        return pkColumns;
    }

    /**
     * Returns primary zone name.
     *
     * @return Zone name to use in the {@code WITH PRIMARY_ZONE} option or {@code null} if not specified.
     */
    public @Nullable String zoneName() {
        return zoneName;
    }

    /**
     * Returns a list of colocation column names.
     *
     * @return A list of colocation column names, or {@code null} if not specified.
     */
    public List<String> colocationColumns() {
        return colocationColumns;
    }

    /**
     * Returns a class to use to generate columns. If it's a natively supported class, then the column with the name "id" will be created
     * and added to the list of columns for the primary key. If it's a class annotated with the
     * {@link org.apache.ignite.catalog.annotations.Table} annotation, then the annotation will be processed and column definitions will be
     * extracted from it.
     *
     * @return A class to use to generate columns or {@code null} if columns are specified using other methods.
     * @see org.apache.ignite.table.mapper.Mapper#nativelySupported(Class)
     */
    public @Nullable Class<?> keyClass() {
        return keyClass;
    }

    /**
     * Returns a class to use to generate columns. If it's a natively supported class, then the column with the name "val" will be created.
     * If it's a class annotated with the {@link org.apache.ignite.catalog.annotations.Table} annotation, then the annotation will be
     * processed and column definitions will be extracted from it.
     *
     * @return A class to use to generate columns or {@code null} if columns are specified using other methods.
     * @see org.apache.ignite.table.mapper.Mapper#nativelySupported(Class)
     */
    public @Nullable Class<?> valueClass() {
        return valueClass;
    }

    /**
     * Returns a list of indexes to create on this table.
     *
     * @return a list of index definitions to create on this table or {@code null} if none should be created.
     */
    public @Nullable List<IndexDefinition> indexes() {
        return indexes;
    }

    /**
     * Returns new builder using this definition.
     *
     * @return New builder.
     */
    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Builder for the table definition.
     */
    public static class Builder {
        private String tableName;

        private String schemaName;

        private boolean ifNotExists;

        private List<ColumnDefinition> columns;

        private IndexType pkType;

        private List<ColumnSorted> pkColumns;

        private List<String> colocationColumns;

        private String zoneName;

        private Class<?> keyClass;

        private Class<?> valueClass;

        private final List<IndexDefinition> indexes = new ArrayList<>();

        private Builder() {}

        private Builder(TableDefinition definition) {
            tableName = definition.tableName;
            schemaName = definition.schemaName;
            ifNotExists = definition.ifNotExists;
            columns = definition.columns;
            pkType = definition.pkType;
            pkColumns = definition.pkColumns;
            colocationColumns = definition.colocationColumns;
            zoneName = definition.zoneName;
            keyClass = definition.keyClass;
            valueClass = definition.valueClass;
        }

        Builder tableName(String name) {
            Objects.requireNonNull(name, "Table name must not be null.");
            if (name.isBlank()) {
                throw new IllegalArgumentException("Table name must not be blank.");
            }

            this.tableName = name;
            return this;
        }

        /**
         * Sets schema name.
         *
         * @param schemaName Schema name.
         * @return This builder instance.
         */
        public Builder schema(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        /**
         * Sets not exists flag.
         *
         * @return This builder instance.
         */
        public Builder ifNotExists() {
            this.ifNotExists = true;
            return this;
        }

        /**
         * Sets definitions of the columns.
         *
         * @param columns An array of column definitions.
         * @return This builder instance.
         */
        public Builder columns(ColumnDefinition... columns) {
            Objects.requireNonNull(columns, "Columns array must not be null.");
            for (ColumnDefinition column : columns) {
                Objects.requireNonNull(column, "Column must not be null.");
            }

            return columns(Arrays.asList(columns));
        }

        /**
         * Sets definitions of the columns.
         *
         * @param columns A list of column definitions.
         * @return This builder instance.
         */
        public Builder columns(List<ColumnDefinition> columns) {
            Objects.requireNonNull(columns, "Columns list must not be null.");
            for (ColumnDefinition column : columns) {
                Objects.requireNonNull(column, "Column must not be null.");
            }

            this.columns = columns;
            return this;
        }

        /**
         * Sets colocation columns.
         *
         * @param colocationColumns An array of colocation column names.
         * @return This builder instance.
         */
        public Builder colocateBy(String... colocationColumns) {
            Objects.requireNonNull(colocationColumns, "Colocation columns array must not be null.");
            for (String column : colocationColumns) {
                Objects.requireNonNull(column, "Colocation column must not be null.");
            }

            return colocateBy(Arrays.asList(colocationColumns));
        }

        /**
         * Sets colocation columns.
         *
         * @param colocationColumns An array of colocation column names.
         * @return This builder instance.
         */
        public Builder colocateBy(List<String> colocationColumns) {
            Objects.requireNonNull(colocationColumns, "Colocation columns list must not be null.");
            for (String column : colocationColumns) {
                Objects.requireNonNull(column, "Colocation column must not be null.");
            }

            this.colocationColumns = colocationColumns;
            return this;
        }

        /**
         * Sets primary zone name.
         *
         * @param zoneName Primary zone name.
         * @return This builder instance.
         */
        public Builder zone(String zoneName) {
            this.zoneName = zoneName;
            return this;
        }

        /**
         * Sets key class to generate columns. If it's a natively supported class, then the column with the name "id" will be created and
         * added to the list of columns for the primary key. If it's annotated with the {@link org.apache.ignite.catalog.annotations.Table}
         * annotation, then the annotation will be processed and column definitions will be extracted from it.
         *
         * @param keyClass Record class.
         * @return This builder instance.
         */
        public Builder key(Class<?> keyClass) {
            this.keyClass = keyClass;
            return this;
        }

        /**
         * Sets value class to generate columns. If it's a natively supported class,
         * then the column with the name "val" will be created. If it's annotated with the
         * {@link org.apache.ignite.catalog.annotations.Table} annotation, then the annotation will be processed and column definitions will
         * be extracted from it.
         *
         * @param valueClass Value class.
         * @return This builder instance.
         */
        public Builder value(Class<?> valueClass) {
            this.valueClass = valueClass;
            return this;
        }

        /**
         * Sets record class to generate columns. If it's a natively supported class, then the column with the name "id" will be created and
         * added to the list of columns for the primary key. If it's annotated with the {@link org.apache.ignite.catalog.annotations.Table}
         * annotation, then the annotation will be processed and column definitions will be extracted from it.
         *
         * @param recordClass Record class.
         * @return This builder instance.
         */
        public Builder record(Class<?> recordClass) {
            this.keyClass = recordClass;
            return this;
        }

        /**
         * Sets primary key columns using default index type.
         *
         * @param columnNames Column names to use in the primary key.
         * @return This builder instance.
         */
        public Builder primaryKey(String... columnNames) {
            Objects.requireNonNull(columnNames, "Primary key columns array must not be null.");
            for (String column : columnNames) {
                Objects.requireNonNull(column, "Primary key column must not be null.");
            }

            return primaryKey(IndexType.DEFAULT, mapToSortedColumns(columnNames));
        }

        /**
         * Sets primary key columns.
         *
         * @param type Type of the index.
         * @param columns An array of columns to use in the primary key.
         * @return This builder instance.
         */
        public Builder primaryKey(IndexType type, ColumnSorted... columns) {
            Objects.requireNonNull(type, "Primary key index type must not be null.");
            Objects.requireNonNull(columns, "Primary key columns array must not be null.");
            for (ColumnSorted column : columns) {
                Objects.requireNonNull(column, "Primary key column must not be null.");
            }

            return primaryKey(type, Arrays.asList(columns));
        }

        /**
         * Sets primary key columns.
         *
         * @param type Type of the index.
         * @param columns A list of columns to use in the primary key.
         * @return This builder instance.
         */
        public Builder primaryKey(IndexType type, List<ColumnSorted> columns) {
            Objects.requireNonNull(type, "Primary key index type must not be null.");
            Objects.requireNonNull(columns, "Primary key columns list must not be null.");
            for (ColumnSorted column : columns) {
                Objects.requireNonNull(column, "Primary key column must not be null.");
            }

            pkType = type;
            pkColumns = columns;
            return this;
        }

        /**
         * Adds an index on this table using specified columns and default index type and sort order. The name of the index will be
         * autogenerated from the column names.
         *
         * @param columnNames An array of column names to use to create index.
         * @return This builder instance.
         */
        public Builder index(String... columnNames) {
            Objects.requireNonNull(columnNames, "Index columns array must not be null.");
            for (String column : columnNames) {
                Objects.requireNonNull(column, "Index column must not be null.");
            }

            return index(null, IndexType.DEFAULT, mapToSortedColumns(columnNames));
        }

        /**
         * Adds an index on this table using specified columns with sort order and index type.
         *
         * @param indexName Name of the index or {@code null} to autogenerate the name.
         * @param type Index type.
         * @param columns An array of columns to use to create index.
         * @return This builder instance.
         */
        public Builder index(@Nullable String indexName, IndexType type, ColumnSorted... columns) {
            Objects.requireNonNull(type, "Index type must not be null.");
            Objects.requireNonNull(columns, "Index columns array must not be null.");
            for (ColumnSorted column : columns) {
                Objects.requireNonNull(column, "Index column must not be null.");
            }

            return index(indexName, type, Arrays.asList(columns));
        }

        /**
         * Adds an index on this table using specified columns with sort order and index type.
         *
         * @param indexName Name of the index or {@code null} to autogenerate the name.
         * @param type Index type.
         * @param columns A list of columns to use to create index.
         * @return This builder instance.
         */
        public Builder index(@Nullable String indexName, IndexType type, List<ColumnSorted> columns) {
            Objects.requireNonNull(type, "Index type must not be null.");
            Objects.requireNonNull(columns, "Index columns list must not be null.");
            for (ColumnSorted column : columns) {
                Objects.requireNonNull(column, "Index column must not be null.");
            }

            indexes.add(new IndexDefinition(indexName, type, columns));
            return this;
        }

        /**
         * Builds the table definition.
         *
         * @return Table definition.
         */
        public TableDefinition build() {
            return new TableDefinition(
                    tableName,
                    schemaName,
                    ifNotExists,
                    columns,
                    pkType,
                    pkColumns,
                    colocationColumns,
                    zoneName,
                    keyClass,
                    valueClass,
                    indexes
            );
        }

        private static List<ColumnSorted> mapToSortedColumns(String[] columnNames) {
            return Arrays.stream(columnNames).map(ColumnSorted::column).collect(Collectors.toList());
        }
    }
}
