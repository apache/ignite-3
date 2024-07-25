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

package org.apache.ignite.internal.schema.catalog;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.ConstantValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.FunctionCall;
import org.apache.ignite.internal.catalog.commands.DefaultValue.Type;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions.TableVersion;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DefaultValueGenerator;
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Utility class to convert table descriptor from the Catalog as well as all related objects to a {@link SchemaDescriptor} domain.
 */
public final class CatalogToSchemaDescriptorConverter {
    private static final Map<String, NativeType> FIX_SIZED_TYPES;

    static {
        List<NativeType> types = IgniteUtils.collectStaticFields(NativeTypes.class, NativeType.class);

        Map<String, NativeType> tmp = new HashMap<>(types.size(), 1.0f);

        for (NativeType type : types) {
            if (!type.spec().fixedLength()) {
                continue;
            }

            tmp.put(type.spec().name(), type);
        }

        FIX_SIZED_TYPES = Map.copyOf(tmp);
    }

    /**
     * Converts type of a column descriptor to a {@link NativeType}.
     *
     * @param columnDescriptor Descriptor to convert.
     * @return A {@link NativeType} object represented by given descriptor.
     */
    public static NativeType convertType(CatalogTableColumnDescriptor columnDescriptor) {
        String typeName = columnDescriptor.type().name();
        NativeType res = FIX_SIZED_TYPES.get(typeName);

        if (res != null) {
            return res;
        }

        switch (typeName) {
            case "STRING":
                int strLen = columnDescriptor.length();

                return NativeTypes.stringOf(strLen);

            case "BYTES":
            case "BYTE_ARRAY":
                int blobLen = columnDescriptor.length();

                return NativeTypes.blobOf(blobLen);

            case "DECIMAL":
                int prec = columnDescriptor.precision();
                int scale = columnDescriptor.scale();

                return NativeTypes.decimalOf(prec, scale);

            case "TIME":
                return NativeTypes.time(columnDescriptor.precision());

            case "DATETIME":
                return NativeTypes.datetime(columnDescriptor.precision());

            case "TIMESTAMP":
                return NativeTypes.timestamp(columnDescriptor.precision());

            default:
                throw new IllegalArgumentException("Unknown type " + typeName);
        }
    }

    /**
     * Converts given column view to a {@link Column}.
     *
     * @param columnDescriptor Descriptor to convert.
     * @return A {@link Column} object representing the table column descriptor.
     */
    public static Column convert(CatalogTableColumnDescriptor columnDescriptor) {
        NativeType type = convertType(columnDescriptor);

        DefaultValue defaultValue = columnDescriptor.defaultValue();

        DefaultValueProvider defaultValueProvider;

        if (defaultValue == null) {
            defaultValueProvider = DefaultValueProvider.NULL_PROVIDER;
        } else if (defaultValue.type() == Type.CONSTANT) {
            ConstantValue constantValue = (ConstantValue) defaultValue;
            defaultValueProvider = DefaultValueProvider.constantProvider(constantValue.value());
        } else if (defaultValue.type() == Type.FUNCTION_CALL) {
            FunctionCall functionCall = (FunctionCall) defaultValue;
            defaultValueProvider = DefaultValueProvider.forValueGenerator(
                    DefaultValueGenerator.valueOf(functionCall.functionName())
            );
        } else {
            throw new IllegalStateException("Unknown value supplier class " + defaultValue.getClass().getName());
        }

        return new Column(columnDescriptor.name(), type, columnDescriptor.nullable(), defaultValueProvider);
    }

    /**
     * Converts given table descriptor to a {@link SchemaDescriptor}.
     *
     * @param tableDescriptor Descriptor to convert.
     * @return A {@link SchemaDescriptor} object representing the table descriptor.
     */
    public static SchemaDescriptor convert(CatalogTableDescriptor tableDescriptor, int tableVersion) {
        List<Column> columns = new ArrayList<>(tableDescriptor.columns().size());

        TableVersion tableVersionInstance = tableDescriptor.schemaVersions().get(tableVersion);

        assert tableVersionInstance != null
                : format("Cannot find table version {} in table descriptor {}", tableVersion, tableDescriptor);

        for (CatalogTableColumnDescriptor column : tableVersionInstance.columns()) {
            columns.add(convert(column));
        }

        return new SchemaDescriptor(
                tableVersion,
                columns,
                tableDescriptor.primaryKeyColumns(),
                tableDescriptor.colocationColumns()
        );
    }

    private CatalogToSchemaDescriptorConverter() { }
}
