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

package org.apache.ignite.internal.storage.index;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.catalog.descriptors.CatalogDescriptorUtils.getNativeType;

import java.util.List;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.catalog.descriptors.ColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.configuration.ColumnView;
import org.apache.ignite.internal.schema.configuration.ConfigurationToSchemaDescriptorConverter;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesView;
import org.apache.ignite.internal.schema.configuration.index.IndexColumnView;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Descriptor for creating a Sorted Index Storage.
 *
 * @see SortedIndexStorage
 */
// TODO: IGNITE-19646 избавиться от конфигурации
public class SortedIndexDescriptor implements IndexDescriptor {
    /**
     * Descriptor of a Sorted Index column (column name and column sort order).
     */
    public static class SortedIndexColumnDescriptor implements ColumnDescriptor {
        private final String name;

        private final NativeType type;

        private final boolean nullable;

        private final boolean asc;

        /**
         * Creates a Column Descriptor.
         *
         * @param name Name of the column.
         * @param type Type of the column.
         * @param nullable Flag indicating that the column may contain {@code null}s.
         * @param asc Sort order of the column.
         */
        public SortedIndexColumnDescriptor(String name, NativeType type, boolean nullable, boolean asc) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.asc = asc;
        }

        /**
         * Creates a Column Descriptor.
         *
         * @param tableColumnView Table column configuration.
         * @param indexColumnView Index column configuration.
         */
        public SortedIndexColumnDescriptor(ColumnView tableColumnView, IndexColumnView indexColumnView) {
            this.name = tableColumnView.name();
            this.type = ConfigurationToSchemaDescriptorConverter.convert(tableColumnView.type());
            this.nullable = tableColumnView.nullable();
            this.asc = indexColumnView.asc();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public NativeType type() {
            return type;
        }

        @Override
        public boolean nullable() {
            return nullable;
        }

        /**
         * Returns {@code true} if this column is sorted in ascending order or {@code false} otherwise.
         */
        public boolean asc() {
            return asc;
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    private final int id;

    private final List<SortedIndexColumnDescriptor> columns;

    private final BinaryTupleSchema binaryTupleSchema;

    /**
     * Creates an Index Descriptor from a given Table Configuration.
     *
     * @param indexId Index ID.
     * @param tablesConfig Tables configuration.
     */
    public SortedIndexDescriptor(int indexId, TablesView tablesConfig) {
        this(indexId, extractIndexColumnsConfiguration(indexId, tablesConfig));
    }

    /**
     * Constructor.
     *
     * @param table Catalog table descriptor.
     * @param index Catalog index descriptor.
     */
    public SortedIndexDescriptor(
            org.apache.ignite.internal.catalog.descriptors.TableDescriptor table,
            org.apache.ignite.internal.catalog.descriptors.SortedIndexDescriptor index
    ) {
        this(index.id(), extractIndexColumnsConfiguration(table, index));
    }

    /**
     * Creates an Index Descriptor from a given set of column descriptors.
     *
     * @param indexId Index ID.
     * @param columnDescriptors Column descriptors.
     */
    public SortedIndexDescriptor(int indexId, List<SortedIndexColumnDescriptor> columnDescriptors) {
        this.id = indexId;
        this.columns = List.copyOf(columnDescriptors);
        this.binaryTupleSchema = createSchema(columns);
    }

    private static List<SortedIndexColumnDescriptor> extractIndexColumnsConfiguration(int indexId, TablesView tablesConfig) {
        TableIndexView indexConfig = tablesConfig.indexes().stream()
                .filter(tableIndexView -> tableIndexView.id() == indexId)
                .findFirst()
                .orElse(null);

        if (indexConfig == null) {
            throw new StorageException(String.format("Index configuration for \"%s\" could not be found", indexId));
        }

        if (!(indexConfig instanceof SortedIndexView)) {
            throw new StorageException(String.format(
                    "Index \"%s\" is not configured as a Sorted Index. Actual type: %s",
                    indexId, indexConfig.type()
            ));
        }

        TableView tableConfig = findTableById(indexConfig.tableId(), tablesConfig.tables());

        if (tableConfig == null) {
            throw new StorageException(String.format("Table configuration for \"%s\" could not be found", indexConfig.tableId()));
        }

        NamedListView<? extends IndexColumnView> indexColumns = ((SortedIndexView) indexConfig).columns();

        return indexColumns.stream()
                .map(indexColumnView -> {
                    String columnName = indexColumnView.name();

                    ColumnView columnView = tableConfig.columns().get(columnName);

                    assert columnView != null : "Incorrect index column configuration. " + columnName + " column does not exist";

                    return new SortedIndexColumnDescriptor(columnView, indexColumnView);
                })
                .collect(toUnmodifiableList());
    }

    private static BinaryTupleSchema createSchema(List<SortedIndexColumnDescriptor> columns) {
        Element[] elements = columns.stream()
                .map(columnDescriptor -> new Element(columnDescriptor.type(), columnDescriptor.nullable()))
                .toArray(Element[]::new);

        return BinaryTupleSchema.create(elements);
    }

    private @Nullable static TableView findTableById(int tableId, NamedListView<? extends TableView> tablesView) {
        for (TableView table : tablesView) {
            if (table.id() == tableId) {
                return table;
            }
        }

        return null;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public List<SortedIndexColumnDescriptor> columns() {
        return columns;
    }

    /**
     * Returns a {@code BinaryTupleSchema} that corresponds to the index configuration.
     */
    public BinaryTupleSchema binaryTupleSchema() {
        return binaryTupleSchema;
    }

    // TODO: IGNITE-19646 возможно нужно тут избавиться от этого в смысле избавиться от зависимости католога
    private static List<SortedIndexColumnDescriptor> extractIndexColumnsConfiguration(
            org.apache.ignite.internal.catalog.descriptors.TableDescriptor table,
            org.apache.ignite.internal.catalog.descriptors.SortedIndexDescriptor index
    ) {
        assert table.id() == index.id() : "tableId=" + table.id() + ", indexTableId=" + index.tableId();

        return index.columns().stream()
                .map(columnDescriptor -> {
                    String columnName = columnDescriptor.name();

                    TableColumnDescriptor column = table.column(columnName);

                    assert column != null : columnName;

                    ColumnCollation collation = columnDescriptor.collation();

                    return new SortedIndexColumnDescriptor(columnName, getNativeType(column), column.nullable(), collation.asc());
                })
                .collect(toList());
    }
}
