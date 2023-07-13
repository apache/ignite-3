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

package org.apache.ignite.internal.schema;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.DESC_NULLS_FIRST;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.ConstantValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.FunctionCall;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.schema.configuration.ColumnChange;
import org.apache.ignite.internal.schema.configuration.ColumnTypeChange;
import org.apache.ignite.internal.schema.configuration.ColumnTypeView;
import org.apache.ignite.internal.schema.configuration.ColumnView;
import org.apache.ignite.internal.schema.configuration.ConfigurationToSchemaDescriptorConverter;
import org.apache.ignite.internal.schema.configuration.PrimaryKeyView;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.ValueSerializationHelper;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ColumnDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ColumnDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ConstantValueDefaultChange;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ConstantValueDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.FunctionCallDefaultChange;
import org.apache.ignite.internal.schema.configuration.defaultvalue.FunctionCallDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.NullValueDefaultChange;
import org.apache.ignite.internal.schema.configuration.index.HashIndexView;
import org.apache.ignite.internal.schema.configuration.index.IndexColumnView;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;

/**
 * Helper class for working with catalog descriptors.
 */
// TODO: IGNITE-19499 Get rid of the table configuration
// TODO: IGNITE-19500 Get rid of the index configuration
@Deprecated(forRemoval = true)
public class CatalogDescriptorUtils {
    /**
     * Converts a table configuration to a catalog table descriptor.
     *
     * @param config Table configuration.
     */
    public static CatalogTableDescriptor toTableDescriptor(TableView config) {
        PrimaryKeyView primaryKeyConfig = config.primaryKey();

        return new CatalogTableDescriptor(
                config.id(),
                config.name(),
                config.zoneId(),
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
    public static CatalogIndexDescriptor toIndexDescriptor(TableIndexView config) {
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
    public static CatalogHashIndexDescriptor toHashIndexDescriptor(HashIndexView config) {
        return new CatalogHashIndexDescriptor(config.id(), config.name(), config.tableId(), config.uniq(), List.of(config.columnNames()));
    }

    /**
     * Converts a sorted index configuration to a catalog hash index descriptor.
     *
     * @param config Sorted index configuration.
     */
    public static CatalogSortedIndexDescriptor toSortedIndexDescriptor(SortedIndexView config) {
        return new CatalogSortedIndexDescriptor(
                config.id(),
                config.name(),
                config.tableId(),
                config.uniq(),
                config.columns().stream().map(CatalogDescriptorUtils::toIndexColumnDescriptor).collect(toList())
        );
    }

    /**
     * Gets the column native type from the catalog table column descriptor.
     *
     * @param column Table column descriptor.
     */
    public static NativeType getNativeType(CatalogTableColumnDescriptor column) {
        switch (column.type()) {
            case BOOLEAN:
                return NativeTypes.BOOLEAN;
            case INT8:
                return NativeTypes.INT8;
            case INT16:
                return NativeTypes.INT16;
            case INT32:
                return NativeTypes.INT32;
            case INT64:
                return NativeTypes.INT64;
            case FLOAT:
                return NativeTypes.FLOAT;
            case DOUBLE:
                return NativeTypes.DOUBLE;
            case DECIMAL:
                return NativeTypes.decimalOf(column.precision(), column.scale());
            case NUMBER:
                return NativeTypes.numberOf(column.precision());
            case DATE:
                return NativeTypes.DATE;
            case TIME:
                return NativeTypes.time(column.precision());
            case DATETIME:
                return NativeTypes.datetime(column.precision());
            case TIMESTAMP:
                return NativeTypes.timestamp(column.precision());
            case UUID:
                return NativeTypes.UUID;
            case BITMASK:
                return NativeTypes.bitmaskOf(column.length());
            case STRING:
                return NativeTypes.stringOf(column.length());
            case BYTE_ARRAY:
                return NativeTypes.blobOf(column.length());
            default:
                throw new IllegalArgumentException("Unknown type: " + column.type());
        }
    }

    private static CatalogTableColumnDescriptor toTableColumnDescriptor(ColumnView config) {
        ColumnTypeView typeConfig = config.type();

        NativeType nativeType = ConfigurationToSchemaDescriptorConverter.convert(typeConfig);

        return new CatalogTableColumnDescriptor(
                config.name(),
                nativeType.spec().asColumnType(),
                config.nullable(),
                typeConfig.precision(),
                typeConfig.scale(),
                typeConfig.length(),
                toDefaultValue(nativeType, config.defaultValueProvider())
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

    private static CatalogIndexColumnDescriptor toIndexColumnDescriptor(IndexColumnView config) {
        //TODO IGNITE-15141: Make null-order configurable.
        // NULLS FIRST for DESC, NULLS LAST for ASC by default.
        CatalogColumnCollation collation = config.asc() ? ASC_NULLS_LAST : DESC_NULLS_FIRST;

        return new CatalogIndexColumnDescriptor(config.name(), collation);
    }

    /**
     * Applies changes from params to the configuraiton changer.
     *
     * @param params Column change parameters.
     * @param columnChange Configuration changer.
     */
    public static void convertColumnDefinition(ColumnParams params, ColumnChange columnChange) {
        NativeType nativeType = getNativeType(CatalogUtils.fromParams(params));

        columnChange.changeType(columnTypeChange -> convert(nativeType, columnTypeChange));
        columnChange.changeNullable(params.nullable());
        columnChange.changeDefaultValueProvider(defaultChange -> {
            switch (params.defaultValueDefinition().type()) {
                case CONSTANT:
                    ConstantValue constantValue = params.defaultValueDefinition();

                    var val = constantValue.value();

                    if (val != null) {
                        defaultChange.convert(ConstantValueDefaultChange.class)
                                .changeDefaultValue(ValueSerializationHelper.toString(val, nativeType));
                    } else {
                        defaultChange.convert(NullValueDefaultChange.class);
                    }

                    break;
                case FUNCTION_CALL:
                    FunctionCall functionCall = params.defaultValueDefinition();

                    defaultChange.convert(FunctionCallDefaultChange.class)
                            .changeFunctionName(functionCall.functionName());

                    break;
                default:
                    throw new IllegalStateException("Unknown default value definition type [type="
                            + params.defaultValueDefinition().type() + ']');
            }
        });
    }

    private static void convert(NativeType colType, ColumnTypeChange colTypeChg) {
        NativeTypeSpec spec = colType.spec();
        String typeName = spec.name().toUpperCase();

        colTypeChg.changeType(typeName);

        switch (spec) {
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case UUID:
                // do nothing
                break;

            case BITMASK:
                BitmaskNativeType bitmaskColType = (BitmaskNativeType) colType;

                colTypeChg.changeLength(bitmaskColType.bits());

                break;

            case BYTES:
            case STRING:
                VarlenNativeType varLenColType = (VarlenNativeType) colType;

                colTypeChg.changeLength(varLenColType.length());

                break;

            case DECIMAL:
                DecimalNativeType numColType = (DecimalNativeType) colType;

                colTypeChg.changePrecision(numColType.precision());
                colTypeChg.changeScale(numColType.scale());

                break;

            case NUMBER:
                NumberNativeType numType = (NumberNativeType) colType;

                colTypeChg.changePrecision(numType.precision());

                break;

            case TIME:
            case DATETIME:
            case TIMESTAMP:
                TemporalNativeType temporalColType = (TemporalNativeType) colType;

                colTypeChg.changePrecision(temporalColType.precision());

                break;

            default:
                throw new IllegalArgumentException("Unknown type " + colType.spec().name());
        }
    }

    /**
     * Converts table descriptor to table schema descriptor.
     */
    public static SchemaDescriptor convert(int schemaVer, CatalogTableDescriptor tableDescriptor) {
        List<Column> keyCols = new ArrayList<>(tableDescriptor.primaryKeyColumns().size());
        List<Column> valCols = new ArrayList<>(tableDescriptor.columns().size() - tableDescriptor.primaryKeyColumns().size());

        int idx = 0;

        for (CatalogTableColumnDescriptor column : tableDescriptor.columns()) {
            if (tableDescriptor.isPrimaryKeyColumn(column.name())) {
                keyCols.add(convert(idx, column));
            } else {
                valCols.add(convert(idx, column));
            }

            idx++;
        }

        return new SchemaDescriptor(
                schemaVer,
                keyCols.toArray(Column[]::new),
                tableDescriptor.colocationColumns().toArray(String[]::new),
                valCols.toArray(Column[]::new)
        );
    }

    private static Column convert(int columnOrder, CatalogTableColumnDescriptor columnDescriptor) {
        NativeType type = getNativeType(columnDescriptor);

        DefaultValue defaultValue = columnDescriptor.defaultValue();

        DefaultValueProvider defaultValueProvider = toValueProvider(defaultValue);

        return new Column(columnOrder, columnDescriptor.name(), type, columnDescriptor.nullable(), defaultValueProvider);
    }

    private static DefaultValueProvider toValueProvider(DefaultValue defaultValue) {
        switch (defaultValue.type()) {
            case CONSTANT:
                return DefaultValueProvider.constantProvider(((ConstantValue) defaultValue).value());
            case FUNCTION_CALL:
                return DefaultValueProvider.forValueGenerator(
                        DefaultValueGenerator.valueOf(((FunctionCall) defaultValue).functionName()));
            default:
                throw new IllegalStateException("Unknown default value type: " + defaultValue.type());
        }
    }
}
