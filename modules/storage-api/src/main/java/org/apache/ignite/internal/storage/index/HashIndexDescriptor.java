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

import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.configuration.ColumnView;
import org.apache.ignite.internal.schema.configuration.ConfigurationToSchemaDescriptorConverter;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesView;
import org.apache.ignite.internal.schema.configuration.index.HashIndexView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.tostring.S;

/**
 * Descriptor for creating a Hash Index Storage.
 *
 * @see HashIndexStorage
 */
public class HashIndexDescriptor implements IndexDescriptor {
    /**
     * Descriptor of a Hash Index column.
     */
    public static class HashIndexColumnDescriptor implements ColumnDescriptor {
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
        public HashIndexColumnDescriptor(String name, NativeType type, boolean nullable) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
        }

        /**
         * Creates a Column Descriptor.
         *
         * @param tableColumnView Table column configuration.
         */
        HashIndexColumnDescriptor(ColumnView tableColumnView) {
            this.name = tableColumnView.name();
            this.type = ConfigurationToSchemaDescriptorConverter.convert(tableColumnView.type());
            this.nullable = tableColumnView.nullable();
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

    private final UUID id;

    private final List<HashIndexColumnDescriptor> columns;

    /**
     * Creates an Index Descriptor from a given Table Configuration.
     *
     * @param indexId Index id.
     * @param tablesConfig Tables and indexes configuration.
     */
    public HashIndexDescriptor(UUID indexId, TablesView tablesConfig) {
        this(indexId, extractIndexColumnsConfiguration(indexId, tablesConfig));
    }

    /**
     * Creates an Index Descriptor from a given set of columns.
     *
     * @param indexId Index id.
     * @param columns Columns descriptors.
     */
    public HashIndexDescriptor(UUID indexId, List<HashIndexColumnDescriptor> columns) {
        this.id = indexId;
        this.columns = columns;
    }

    private static List<HashIndexColumnDescriptor> extractIndexColumnsConfiguration(UUID indexId, TablesView tablesConfig) {
        TableIndexView indexConfig = ConfigurationUtil.getByInternalId(tablesConfig.indexes(), indexId);

        if (indexConfig == null) {
            throw new StorageException(String.format("Index configuration for \"%s\" could not be found", indexId));
        }

        if (!(indexConfig instanceof HashIndexView)) {
            throw new StorageException(String.format(
                    "Index \"%s\" is not configured as a Hash Index. Actual type: %s",
                    indexConfig.id(), indexConfig.type()
            ));
        }

        TableView tableConfig = ConfigurationUtil.getByInternalId(tablesConfig.tables(), indexConfig.tableId());

        if (tableConfig == null) {
            throw new StorageException(String.format("Table configuration for \"%s\" could not be found", indexConfig.tableId()));
        }

        String[] indexColumns = ((HashIndexView) indexConfig).columnNames();

        return Arrays.stream(indexColumns)
                .map(columnName -> {
                    ColumnView columnView = tableConfig.columns().get(columnName);

                    assert columnView != null : "Incorrect index column configuration. " + columnName + " column does not exist";

                    return new HashIndexColumnDescriptor(columnView);
                })
                .collect(toUnmodifiableList());
    }

    @Override
    public UUID id() {
        return id;
    }

    @Override
    public List<HashIndexColumnDescriptor> columns() {
        return columns;
    }
}
