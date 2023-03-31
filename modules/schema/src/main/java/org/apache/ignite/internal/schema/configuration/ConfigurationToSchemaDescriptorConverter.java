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

package org.apache.ignite.internal.schema.configuration;

import static org.apache.ignite.internal.schema.configuration.ValueSerializationHelper.fromString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DefaultValueGenerator;
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ColumnDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ConstantValueDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.FunctionCallDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.NullValueDefaultView;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Utility class to convert table configuration view as well as all related objects to a {@link SchemaDescriptor} domain.
 */
public final class ConfigurationToSchemaDescriptorConverter {
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
     * Converts given type view to a {@link NativeType}.
     *
     * @param colTypeView View to convert.
     * @return A {@link NativeType} object represented by given view.
     */
    public static NativeType convert(ColumnTypeView colTypeView) {
        String typeName = colTypeView.type().toUpperCase();
        NativeType res = FIX_SIZED_TYPES.get(typeName);

        if (res != null) {
            return res;
        }

        switch (typeName) {
            case "BITMASK":
                int bitmaskLen = colTypeView.length();

                return NativeTypes.bitmaskOf(bitmaskLen);

            case "STRING":
                int strLen = colTypeView.length();

                return NativeTypes.stringOf(strLen);

            case "BYTES":
                int blobLen = colTypeView.length();

                return NativeTypes.blobOf(blobLen);

            case "DECIMAL":
                int prec = colTypeView.precision();
                int scale = colTypeView.scale();

                return NativeTypes.decimalOf(prec, scale);

            case "NUMBER":
                return NativeTypes.numberOf(colTypeView.precision());

            case "TIME":
                return NativeTypes.time(colTypeView.precision());

            case "DATETIME":
                return NativeTypes.datetime(colTypeView.precision());

            case "TIMESTAMP":
                return NativeTypes.datetime(colTypeView.precision());

            default:
                throw new IllegalArgumentException("Unknown type " + typeName);
        }
    }

    /**
     * Converts given column view to a {@link Column}.
     *
     * @param columnOrder Number of the current column.
     * @param columnView View to convert.
     * @return A {@link Column} object represented by given view.
     */
    public static Column convert(int columnOrder, ColumnView columnView) {
        NativeType type = convert(columnView.type());

        ColumnDefaultView defaultValueView = columnView.defaultValueProvider();

        DefaultValueProvider defaultValueProvider;

        if (defaultValueView instanceof NullValueDefaultView) {
            defaultValueProvider = DefaultValueProvider.constantProvider(null);
        } else if (defaultValueView instanceof FunctionCallDefaultView) {
            defaultValueProvider = DefaultValueProvider.forValueGenerator(
                    DefaultValueGenerator.valueOf(
                            ((FunctionCallDefaultView) defaultValueView).functionName()
                    )
            );
        } else if (defaultValueView instanceof ConstantValueDefaultView) {
            defaultValueProvider = DefaultValueProvider.constantProvider(
                    fromString(((ConstantValueDefaultView) defaultValueView).defaultValue(), type)
            );
        } else {
            throw new IllegalStateException("Unknown value supplier class " + defaultValueView.getClass().getName());
        }

        return new Column(columnOrder, columnView.name(), type, columnView.nullable(), defaultValueProvider);
    }

    /**
     * Converts given table view to a {@link SchemaDescriptor}.
     *
     * @param schemaVer A version of given schema.
     * @param tableView View to convert.
     * @return A {@link SchemaDescriptor} object represented by given view.
     */
    public static SchemaDescriptor convert(int schemaVer, TableView tableView) {
        Set<String> keyColumnsNames = Set.of(tableView.primaryKey().columns());

        List<Column> keyCols = new ArrayList<>(keyColumnsNames.size());
        List<Column> valCols = new ArrayList<>(tableView.columns().size() - keyColumnsNames.size());

        int idx = 0;

        for (String columnName : tableView.columns().namedListKeys()) {
            ColumnView column = tableView.columns().get(columnName);

            if (column == null) {
                // columns was removed, so let's skip it
                continue;
            }

            if (keyColumnsNames.contains(columnName)) {
                keyCols.add(convert(idx, column));
            } else {
                valCols.add(convert(idx, column));
            }

            idx++;
        }

        return new SchemaDescriptor(
                schemaVer,
                keyCols.toArray(Column[]::new),
                tableView.primaryKey().colocationColumns(),
                valCols.toArray(Column[]::new)
        );
    }

    private ConfigurationToSchemaDescriptorConverter() { }
}
