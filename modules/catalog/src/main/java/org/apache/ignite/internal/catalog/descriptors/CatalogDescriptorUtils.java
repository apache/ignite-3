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

package org.apache.ignite.internal.catalog.descriptors;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.descriptors.ColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.ColumnCollation.DESC_NULLS_LAST;

import java.util.List;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.configuration.ColumnTypeView;
import org.apache.ignite.internal.schema.configuration.ColumnView;
import org.apache.ignite.internal.schema.configuration.ConfigurationToSchemaDescriptorConverter;
import org.apache.ignite.internal.schema.configuration.PrimaryKeyView;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.ValueSerializationHelper;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ColumnDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ColumnDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ConstantValueDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.FunctionCallDefaultView;
import org.apache.ignite.internal.schema.configuration.index.HashIndexView;
import org.apache.ignite.internal.schema.configuration.index.IndexColumnView;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;

/**
 * Helper class for working with catalog descriptors.
 */
// TODO: IGNITE-19499 Get rid of the table configuration
// TODO: IGNITE-19646 Get rid of the index configuration
public class CatalogDescriptorUtils {
    /**
     * Converts a table configuration to a catalog table descriptor.
     *
     * @param config Table configuration.
     */
    public static TableDescriptor toTableDescriptor(TableView config) {
        PrimaryKeyView primaryKeyConfig = config.primaryKey();

        return new TableDescriptor(
                config.id(),
                config.name(),
                config.columns().stream().map(CatalogDescriptorUtils::toTableColumnDescriptor).collect(toList()),
                List.of(primaryKeyConfig.columns()),
                List.of(primaryKeyConfig.colocationColumns())
        );
    }

    /**
     * Converts a index configuration to a catalog index descriptor.
     *
     * @param config Index configuration.
     */
    public static IndexDescriptor toIndexDescriptor(TableIndexView config) {
        switch (config.type()) {
            case TableIndexConfigurationSchema.HASH_INDEX_TYPE:
                return toHashIndexDescriptor(((HashIndexView) config));
            case TableIndexConfigurationSchema.SORTED_INDEX_TYPE:
                return toSortedIndexDescriptor(((SortedIndexView) config));
            default:
                throw new IllegalArgumentException("Unknown index type:" + config);
        }
    }

    /**
     * Converts a hash index configuration to a catalog hash index descriptor.
     *
     * @param config Hash index configuration.
     */
    public static HashIndexDescriptor toHashIndexDescriptor(HashIndexView config) {
        return new HashIndexDescriptor(config.id(), config.name(), config.tableId(), List.of(config.columnNames()));
    }

    /**
     * Converts a sorted index configuration to a catalog hash index descriptor.
     *
     * @param config Sorted index configuration.
     */
    public static SortedIndexDescriptor toSortedIndexDescriptor(SortedIndexView config) {
        return new SortedIndexDescriptor(
                config.id(),
                config.name(),
                config.tableId(),
                config.columns().stream().map(CatalogDescriptorUtils::toIndexColumnDescriptor).collect(toList())
        );
    }

    private static TableColumnDescriptor toTableColumnDescriptor(ColumnView config) {
        ColumnTypeView typeConfig = config.type();

        NativeType nativeType = ConfigurationToSchemaDescriptorConverter.convert(typeConfig);

        return new TableColumnDescriptor(
                config.name(),
                nativeType.spec().asColumnType(),
                config.nullable(),
                toDefaultValue(nativeType, config.defaultValueProvider()),
                typeConfig.length(),
                typeConfig.precision(),
                typeConfig.scale()
        );
    }

    private static DefaultValue toDefaultValue(NativeType columnType, ColumnDefaultView config) {
        switch (config.type()) {
            case ColumnDefaultConfigurationSchema.NULL_VALUE_TYPE:
                return DefaultValue.constant(null);
            case ColumnDefaultConfigurationSchema.CONSTANT_VALUE_TYPE:
                String defaultValue = ((ConstantValueDefaultView) config).defaultValue();

                return DefaultValue.constant(ValueSerializationHelper.fromString(defaultValue, columnType));
            case ColumnDefaultConfigurationSchema.FUNCTION_CALL_TYPE:
                String functionName = ((FunctionCallDefaultView) config).functionName();

                return DefaultValue.functionCall(functionName);
            default:
                throw new IllegalArgumentException("Unknown default value:" + config);
        }
    }

    private static IndexColumnDescriptor toIndexColumnDescriptor(IndexColumnView config) {
        // There is no suitable property in the configuration, in IGNITE-19646 the configuration will have to be deleted, so a temporary
        // value has been selected for now.
        ColumnCollation collation = config.asc() ? ASC_NULLS_LAST : DESC_NULLS_LAST;

        return new IndexColumnDescriptor(config.name(), collation);
    }
}
