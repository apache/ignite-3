/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.index;

import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.List;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.configuration.SchemaDescriptorConverter;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.tostring.S;

/**
 * Descriptor for creating a Sorted Index Storage.
 *
 * @see SortedIndexStorage
 */
public class SortedIndexDescriptor {
    /**
     * Descriptor of a Sorted Index column (column name and column sort order).
     */
    public static class ColumnDescriptor {
        private final String name;

        private final NativeType type;

        private final boolean nullable;

        private final boolean asc;

        ColumnDescriptor(ColumnView tableColumnView, IndexColumnView indexColumnView) {
            this.name = tableColumnView.name();
            this.type = SchemaDescriptorConverter.convert(SchemaConfigurationConverter.convert(tableColumnView.type()));
            this.nullable = tableColumnView.nullable();
            this.asc = indexColumnView.asc();
        }

        /**
         * Returns the name of an index column.
         */
        public String name() {
            return name;
        }

        /**
         * Returns a column descriptor.
         */
        public NativeType type() {
            return type;
        }

        /**
         * Returns {@code true} if this column can contain null values or {@code false} otherwise.
         */
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

    private final String name;

    private final List<ColumnDescriptor> columns;

    /**
     * Creates an Index Descriptor from a given Table Configuration.
     *
     * @param name index name.
     * @param tableConfig table configuration.
     */
    public SortedIndexDescriptor(String name, TableView tableConfig) {
        this.name = name;

        TableIndexView indexConfig = tableConfig.indices().get(name);

        if (indexConfig == null) {
            throw new StorageException(String.format("Index configuration for \"%s\" could not be found", name));
        }

        if (!(indexConfig instanceof SortedIndexView)) {
            throw new StorageException(String.format(
                    "Index \"%s\" is not configured as a Sorted Index. Actual type: %s",
                    name, indexConfig.type()
            ));
        }

        NamedListView<? extends IndexColumnView> indexColumns = ((SortedIndexView) indexConfig).columns();

        columns = indexColumns.namedListKeys().stream()
                .map(columnName -> {
                    ColumnView columnView = tableConfig.columns().get(columnName);

                    assert columnView != null : "Incorrect index column configuration. " + columnName + " column does not exist";

                    IndexColumnView indexColumnView = indexColumns.get(columnName);

                    return new ColumnDescriptor(columnView, indexColumnView);
                })
                .collect(toUnmodifiableList());
    }

    /**
     * Returns this index' name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the Column Descriptors that comprise a row of this index.
     */
    public List<ColumnDescriptor> indexColumns() {
        return columns;
    }
}
