/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.internal.schema.configuration;

import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaException;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;

/**
 * Build SchemaDescriptor from Table internal configuration.
 */
public class SchemaDescriptorConverter {
    /**
     * Convert ColumnType to NativeType.
     *
     * @param colType ColumnType.
     * @return NativeType.
     */
    public static NativeType convert(ColumnType colType) {
        assert colType != null;

        ColumnType.ColumnTypeSpec type = colType.typeSpec();

        switch (type) {
            case INT8:
                return INT8;

            case INT16:
                return INT16;

            case INT32:
                return INT32;

            case INT64:
                return INT64;

            case UINT8:
            case UINT16:
            case UINT32:
            case UINT64:
                throw new UnsupportedOperationException("Unsigned types are not supported yet.");

            case FLOAT:
                return FLOAT;

            case DOUBLE:
                return DOUBLE;

            case DECIMAL: {
                ColumnType.DecimalColumnType numType = (ColumnType.DecimalColumnType) colType;

                return NativeTypes.decimalOf(numType.precision(), numType.scale());
            }
            case UUID:
                return UUID;

            case BITMASK:
                return NativeTypes.bitmaskOf(((ColumnType.VarLenColumnType) colType).length());

            case STRING: {
                int strLen = ((ColumnType.VarLenColumnType) colType).length();

                if (strLen == 0) {
                    strLen = Integer.MAX_VALUE;
                }

                return NativeTypes.stringOf(strLen);
            }
            case BLOB: {
                int blobLen = ((ColumnType.VarLenColumnType) colType).length();

                if (blobLen == 0) {
                    blobLen = Integer.MAX_VALUE;
                }

                return NativeTypes.blobOf(blobLen);
            }
            case DATE:
                return NativeTypes.DATE;
            case TIME: {
                ColumnType.TemporalColumnType temporalType = (ColumnType.TemporalColumnType) colType;

                return NativeTypes.time(temporalType.precision());
            }
            case DATETIME: {
                ColumnType.TemporalColumnType temporalType = (ColumnType.TemporalColumnType) colType;

                return NativeTypes.datetime(temporalType.precision());
            }
            case TIMESTAMP: {
                ColumnType.TemporalColumnType temporalType = (ColumnType.TemporalColumnType) colType;

                return NativeTypes.timestamp(temporalType.precision());
            }

            case NUMBER: {
                ColumnType.NumberColumnType numberType = (ColumnType.NumberColumnType) colType;

                return NativeTypes.numberOf(numberType.precision());
            }
            default:
                throw new InvalidTypeException("Unexpected type " + type);
        }
    }

    /**
     * Creates a column for given column definition.
     *
     * @param colCfg Column definition.
     * @return Internal Column.
     */
    public static Column convert(int columnOrder, ColumnDefinition colCfg) {
        NativeType type = convert(colCfg.type());

        return new Column(columnOrder, colCfg.name(), type, colCfg.nullable(),
                new ConstantSupplier(convertDefault(type, (String) colCfg.defaultValue())));
    }

    /**
     * Build schema descriptor by table schema.
     *
     * @param schemaVer Schema version.
     * @param tblCfg    Table schema.
     * @return SchemaDescriptor.
     */
    public static SchemaDescriptor convert(int schemaVer, TableDefinition tblCfg) {
        Set<String> keyColumnsNames = tblCfg.keyColumns();

        List<Column> keyCols = new ArrayList<>(keyColumnsNames.size());
        List<Column> valCols = new ArrayList<>(tblCfg.columns().size() - keyColumnsNames.size());

        int idx = 0;

        for (ColumnDefinition col : tblCfg.columns()) {
            if (keyColumnsNames.contains(col.name())) {
                keyCols.add(convert(idx, col));
            } else {
                valCols.add(convert(idx, col));
            }

            idx++;
        }

        return new SchemaDescriptor(
                schemaVer,
                keyCols.toArray(Column[]::new),
                tblCfg.colocationColumns().toArray(String[]::new),
                valCols.toArray(Column[]::new));
    }

    /**
     * TODO: https://issues.apache.org/jira/browse/IGNITE-14479 Fix default conversion.
     *
     * @param type Column type.
     * @param dflt Column default value.
     * @return Parsed object.
     */
    private static Serializable convertDefault(NativeType type, String dflt) {
        if (dflt == null || dflt.isEmpty()) {
            return null;
        }

        switch (type.spec()) {
            case INT8:
                return Byte.parseByte(dflt);
            case INT16:
                return Short.parseShort(dflt);
            case INT32:
                return Integer.parseInt(dflt);
            case INT64:
                return Long.parseLong(dflt);
            case FLOAT:
                return Float.parseFloat(dflt);
            case DOUBLE:
                return Double.parseDouble(dflt);
            case DECIMAL:
                return new BigDecimal(dflt).setScale(((DecimalNativeType) type).scale(), RoundingMode.HALF_UP);
            case NUMBER:
                return new BigInteger(dflt);
            case STRING:
                return dflt;
            case UUID:
                return java.util.UUID.fromString(dflt);
            case DATE:
                return LocalDate.parse(dflt);
            case TIME:
                return LocalTime.parse(dflt);
            case DATETIME:
                return LocalDateTime.parse(dflt);
            case TIMESTAMP:
                return Instant.parse(dflt);
            default:
                throw new SchemaException("Default value is not supported for type: type=" + type.toString());
        }
    }

    /**
     * Constant value supplier.
     */
    private static class ConstantSupplier implements Supplier<Object>, Serializable {
        /** Value. */
        private final Serializable val;

        /**
         * Constructor.
         *
         * @param val Value.
         */
        ConstantSupplier(Serializable val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override
        public Object get() {
            return val;
        }
    }
}
