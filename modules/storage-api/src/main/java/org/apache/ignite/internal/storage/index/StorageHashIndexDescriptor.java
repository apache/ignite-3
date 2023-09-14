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
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.tostring.S;

/**
 * Descriptor for creating a Hash Index Storage.
 *
 * @see HashIndexStorage
 */
public class StorageHashIndexDescriptor implements StorageIndexDescriptor {
    /**
     * Descriptor of a Hash Index column.
     */
    public static class StorageHashIndexColumnDescriptor implements StorageColumnDescriptor {
        private final String name;

        private final NativeType type;

        private final boolean nullable;

        /**
         * Creates a Column Descriptor.
         *
         * @param name Name of the column.
         * @param type Type of the column.
         * @param nullable Flag indicating that the column may contain {@code null}s.
         */
        public StorageHashIndexColumnDescriptor(String name, NativeType type, boolean nullable) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
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

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    private final int id;

    private final List<StorageHashIndexColumnDescriptor> columns;

    /**
     * Constructor.
     *
     * @param table Catalog table descriptor.
     * @param index Catalog index descriptor.
     */
    public StorageHashIndexDescriptor(CatalogTableDescriptor table, CatalogHashIndexDescriptor index) {
        this(index.id(), extractIndexColumnsConfiguration(table, index));
    }

    /**
     * Creates an Index Descriptor from a given set of columns.
     *
     * @param indexId Index id.
     * @param columns Columns descriptors.
     */
    public StorageHashIndexDescriptor(int indexId, List<StorageHashIndexColumnDescriptor> columns) {
        this.id = indexId;
        this.columns = columns;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public List<StorageHashIndexColumnDescriptor> columns() {
        return columns;
    }

    private static List<StorageHashIndexColumnDescriptor> extractIndexColumnsConfiguration(
            CatalogTableDescriptor table,
            CatalogHashIndexDescriptor index
    ) {
        assert table.id() == index.tableId() : "tableId=" + table.id() + ", indexTableId=" + index.tableId();

        return index.columns().stream()
                .map(columnName -> {
                    CatalogTableColumnDescriptor column = table.column(columnName);

                    assert column != null : columnName;

                    return new StorageHashIndexColumnDescriptor(column.name(), getNativeType(column), column.nullable());
                })
                .collect(toList());
    }
}
