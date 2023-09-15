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
import static org.apache.ignite.internal.storage.index.StorageIndexDescriptor.getNativeType;

import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.tostring.S;

/**
 * Descriptor for creating a Sorted Index Storage.
 *
 * @see SortedIndexStorage
 */
public class StorageSortedIndexDescriptor implements StorageIndexDescriptor {
    /**
     * Descriptor of a Sorted Index column (column name and column sort order).
     */
    public static class StorageSortedIndexColumnDescriptor implements StorageColumnDescriptor {
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
        public StorageSortedIndexColumnDescriptor(String name, NativeType type, boolean nullable, boolean asc) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.asc = asc;
        }

        @Override
        @Deprecated
        //TODO IGNITE-19758 Remove this method and fix the test that uses it.
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

    private final List<StorageSortedIndexColumnDescriptor> columns;

    private final BinaryTupleSchema binaryTupleSchema;

    /**
     * Constructor.
     *
     * @param table Catalog table descriptor.
     * @param index Catalog index descriptor.
     */
    public StorageSortedIndexDescriptor(CatalogTableDescriptor table, CatalogSortedIndexDescriptor index) {
        this(index.id(), extractIndexColumnsConfiguration(table, index));
    }

    /**
     * Creates an Index Descriptor from a given set of column descriptors.
     *
     * @param indexId Index ID.
     * @param columnDescriptors Column descriptors.
     */
    public StorageSortedIndexDescriptor(int indexId, List<StorageSortedIndexColumnDescriptor> columnDescriptors) {
        this.id = indexId;
        this.columns = List.copyOf(columnDescriptors);
        this.binaryTupleSchema = createSchema(columns);
    }

    private static BinaryTupleSchema createSchema(List<StorageSortedIndexColumnDescriptor> columns) {
        Element[] elements = columns.stream()
                .map(columnDescriptor -> new Element(columnDescriptor.type(), columnDescriptor.nullable()))
                .toArray(Element[]::new);

        return BinaryTupleSchema.create(elements);
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public List<StorageSortedIndexColumnDescriptor> columns() {
        return columns;
    }

    /**
     * Returns a {@code BinaryTupleSchema} that corresponds to the index configuration.
     */
    public BinaryTupleSchema binaryTupleSchema() {
        return binaryTupleSchema;
    }

    private static List<StorageSortedIndexColumnDescriptor> extractIndexColumnsConfiguration(
            CatalogTableDescriptor table,
            CatalogSortedIndexDescriptor index
    ) {
        assert table.id() == index.tableId() : "tableId=" + table.id() + ", indexTableId=" + index.tableId();

        return index.columns().stream()
                .map(columnDescriptor -> {
                    String columnName = columnDescriptor.name();

                    CatalogTableColumnDescriptor column = table.column(columnName);

                    assert column != null : columnName;

                    CatalogColumnCollation collation = columnDescriptor.collation();

                    return new StorageSortedIndexColumnDescriptor(columnName, getNativeType(column), column.nullable(), collation.asc());
                })
                .collect(toList());
    }
}
