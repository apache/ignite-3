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

package org.apache.ignite.internal.schema.testutils;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.schema.testutils.definition.ColumnDefinition;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.DefaultValueDefinition.ConstantValue;
import org.apache.ignite.internal.schema.testutils.definition.DefaultValueDefinition.FunctionCall;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;

/**
 * Schema to Catalog params converter.
 */
public class SchemaToCatalogParamsConverter {
    /**
     * Types map.
     */
    private static final EnumSet<ColumnType.ColumnTypeSpec> fixSizedTypes = EnumSet.of(
            ColumnType.INT8.typeSpec(),
            ColumnType.INT16.typeSpec(),
            ColumnType.INT32.typeSpec(),
            ColumnType.INT64.typeSpec(),
            ColumnType.FLOAT.typeSpec(),
            ColumnType.DOUBLE.typeSpec(),
            ColumnType.UUID.typeSpec(),
            ColumnType.DATE.typeSpec()
    );

    /**
     * Converts table definition to CreateTableParams.
     */
    public static CreateTableParams toCreateTable(String zoneName, TableDefinition tableDef) {
        Set<String> keyColumns = tableDef.keyColumns();

        List<String> keyColumnsInOrder = tableDef.columns().stream()
                .map(ColumnDefinition::name)
                .filter(keyColumns::contains)
                .collect(Collectors.toList());

        List<ColumnParams> columnParams = tableDef.columns().stream()
                .map(SchemaToCatalogParamsConverter::convert)
                .collect(Collectors.toList());

        return CreateTableParams.builder()
                .schemaName(tableDef.schemaName())
                .tableName(tableDef.name())
                .zone(zoneName)
                .columns(columnParams)
                .primaryKeyColumns(keyColumnsInOrder)
                .colocationColumns(tableDef.colocationColumns())
                .build();
    }

    /**
     * Convert column to column change.
     *
     * @param def Column to convert.
     * @return Column params.
     */
    public static ColumnParams convert(ColumnDefinition def) {
        Builder builder = ColumnParams.builder()
                .name(def.name())
                .nullable(def.nullable());

        setType(builder, def.type());

        assert def.defaultValueDefinition() != null;

        switch (def.defaultValueDefinition().type()) {
            case CONSTANT:
                ConstantValue constantValue = def.defaultValueDefinition();

                builder.defaultValue(DefaultValue.constant(constantValue.value()));

                break;
            case FUNCTION_CALL:
                FunctionCall functionCall = def.defaultValueDefinition();

                builder.defaultValue(DefaultValue.functionCall(functionCall.functionName()));

                break;
            case NULL:

                builder.defaultValue(DefaultValue.constant(null));
                break;
            default:
                throw new IllegalStateException("Unknown default value definition type [type="
                        + def.defaultValueDefinition().type() + ']');
        }

        return builder.build();
    }

    private static org.apache.ignite.sql.ColumnType convert(ColumnType colType) {
        switch (colType.typeSpec()) {
            case INT8:
                return org.apache.ignite.sql.ColumnType.INT8;
            case INT16:
                return org.apache.ignite.sql.ColumnType.INT16;
            case INT32:
                return org.apache.ignite.sql.ColumnType.INT32;
            case INT64:
                return org.apache.ignite.sql.ColumnType.INT64;
            case FLOAT:
                return org.apache.ignite.sql.ColumnType.FLOAT;
            case DOUBLE:
                return org.apache.ignite.sql.ColumnType.DOUBLE;
            case DECIMAL:
                return org.apache.ignite.sql.ColumnType.DECIMAL;
            case DATE:
                return org.apache.ignite.sql.ColumnType.DATE;
            case TIME:
                return org.apache.ignite.sql.ColumnType.TIME;
            case DATETIME:
                return org.apache.ignite.sql.ColumnType.DATETIME;
            case TIMESTAMP:
                return org.apache.ignite.sql.ColumnType.TIMESTAMP;
            case UUID:
                return org.apache.ignite.sql.ColumnType.UUID;
            case BITMASK:
                return org.apache.ignite.sql.ColumnType.BITMASK;
            case STRING:
                return org.apache.ignite.sql.ColumnType.STRING;
            case BYTES:
                return org.apache.ignite.sql.ColumnType.BYTE_ARRAY;
            case NUMBER:
                return org.apache.ignite.sql.ColumnType.NUMBER;

            default:
                throw new IllegalArgumentException("Type is not supported: " + colType.typeSpec());
        }
    }

    /**
     * Convert ColumnType to ColumnTypeChange.
     *
     * @param builder Column builder.
     * @param colType ColumnType.
     */
    private static void setType(Builder builder, ColumnType colType) {
        builder.type(convert(colType));

        if (fixSizedTypes.contains(colType.typeSpec())) {
            return;
        }

        switch (colType.typeSpec()) {
            case BITMASK:
            case BYTES:
            case STRING:
                int length = ((ColumnType.VarLenColumnType) colType).length();

                if (length == 0) {
                    length = Integer.MAX_VALUE;
                }

                builder.length(length);

                return;
            case DECIMAL:
                ColumnType.DecimalColumnType numColType = (ColumnType.DecimalColumnType) colType;

                builder.precision(numColType.precision());
                builder.scale(numColType.scale());

                return;
            case NUMBER:
                ColumnType.NumberColumnType numType = (ColumnType.NumberColumnType) colType;

                builder.precision(numType.precision());

                return;
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                ColumnType.TemporalColumnType temporalColType = (ColumnType.TemporalColumnType) colType;

                builder.precision(temporalColType.precision());

                return;
            default:
                throw new IllegalArgumentException("Unknown type " + colType.typeSpec().name());
        }
    }
}
