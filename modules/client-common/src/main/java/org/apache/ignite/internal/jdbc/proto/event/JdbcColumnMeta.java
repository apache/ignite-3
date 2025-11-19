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

package org.apache.ignite.internal.jdbc.proto.event;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NULL;
import static java.sql.Types.OTHER;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.jdbc.JdbcConverterUtils;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.sql.ColumnType;

/**
 * JDBC column metadata.
 */
public class JdbcColumnMeta extends Response {
    /** Nullable. */
    private boolean nullable;

    /** Column label. */
    private String label;

    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Column name. */
    private String colName;

    /** Data type. */
    private ColumnType columnType;

    /** Precision. */
    private int precision;

    /** Scale. */
    private int scale;

    /**
     * Default constructor is used for serialization.
     */
    public JdbcColumnMeta() {
    }

    /**
     * Constructor.
     *
     * @param label Column label.
     * @param columnType Type.
     */
    public JdbcColumnMeta(String label, ColumnType columnType) {
        this(label, null, null, null, columnType, -1, -1, true);
    }

    /**
     * Constructor with nullable flag.
     *
     * @param label      Column label.
     * @param schemaName Schema.
     * @param tblName    Table.
     * @param colName    Column.
     * @param columnType Type.
     * @param nullable   Nullable flag.
     * @param precision  Column precision.
     * @param scale      Column scale.
     */
    public JdbcColumnMeta(
            String label, String schemaName, String tblName, String colName, ColumnType columnType,
            int precision, int scale, boolean nullable
    ) {
        this.label = label;
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.colName = colName;
        this.nullable = nullable;

        this.columnType = columnType;
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Gets column label.
     *
     * @return Column name.
     */
    public String columnLabel() {
        return label;
    }

    /**
     * Gets schema name.
     *
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Gets table name.
     *
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * Gets column name.
     *
     * @return Column name.
     */
    public String columnName() {
        return colName != null ? colName : label;
    }

    /** Gets column type. */
    public ColumnType columnType() {
        return columnType;
    }

    /**
     * Gets data type id.
     *
     * @return Column's data type.
     */
    public int dataType() {
        return typeId(columnType);
    }

    /**
     * Gets data type name.
     *
     * @return Column's data type name.
     */
    public String dataTypeName() {
        return typeName(columnType);
    }

    /**
     * Gets default value.
     *
     * @return Column's default value.
     */
    public String defaultValue() {
        return null;
    }

    /**
     * Gets column precision.
     *
     * @return Column's precision.
     */
    public int precision() {
        return precision;
    }

    /**
     * Gets column scale.
     *
     * @return Column's scale.
     */
    public int scale() {
        return scale;
    }

    /**
     * Gets nullable flag.
     *
     * @return {@code true} in case the column allows null values. Otherwise returns {@code false}
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Gets data type class.
     *
     * @return Data type class.
     */
    public String dataTypeClass() {
        return JdbcConverterUtils.columnTypeToJdbcClass(columnType).getName();
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!success()) {
            return;
        }

        packer.packString(label);
        ClientMessageUtils.writeStringNullable(packer, schemaName);
        ClientMessageUtils.writeStringNullable(packer, tblName);
        ClientMessageUtils.writeStringNullable(packer, colName);

        packer.packInt(columnType.id());
        packer.packBoolean(nullable);
        packer.packInt(precision);
        packer.packInt(scale);
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!success()) {
            return;
        }

        label = unpacker.unpackString();
        schemaName = ClientMessageUtils.readStringNullable(unpacker);
        tblName = ClientMessageUtils.readStringNullable(unpacker);
        colName = ClientMessageUtils.readStringNullable(unpacker);

        columnType = ColumnType.getById(unpacker.unpackInt());
        nullable = unpacker.unpackBoolean();
        precision = unpacker.unpackInt();
        scale = unpacker.unpackInt();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JdbcColumnMeta meta = (JdbcColumnMeta) o;
        return nullable == meta.nullable
                && columnType == meta.columnType
                && precision == meta.precision
                && scale == meta.scale
                && Objects.equals(schemaName, meta.schemaName)
                && Objects.equals(tblName, meta.tblName)
                && Objects.equals(colName, meta.colName);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = (nullable ? 1 : 0);
        result = 31 * result + (schemaName != null ? schemaName.hashCode() : 0);
        result = 31 * result + (tblName != null ? tblName.hashCode() : 0);
        result = 31 * result + (colName != null ? colName.hashCode() : 0);
        result = 31 * result + columnType.id();
        result = 31 * result + precision;
        result = 31 * result + scale;
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcColumnMeta.class, this);
    }

    /**
     * Converts column type to SQL type name.
     *
     * @param columnType Column type.
     * @return SQL type name.
     */
    public static String typeName(ColumnType columnType) {
        switch (columnType) {
            case BOOLEAN: return "BOOLEAN";
            case INT8: return "TINYINT";
            case INT16: return "SMALLINT";
            case INT32: return "INTEGER";
            case INT64: return "BIGINT";
            case FLOAT: return "REAL";
            case DOUBLE: return "DOUBLE";
            case STRING: return "VARCHAR";
            case BYTE_ARRAY: return "VARBINARY";
            case TIME: return "TIME";
            case DATETIME: return "TIMESTAMP";
            case TIMESTAMP: return "TIMESTAMP WITH LOCAL TIME ZONE";
            case DATE: return "DATE";
            case DECIMAL: return "DECIMAL";
            case NULL: return "NULL";
            case UUID: return "UUID";
            case PERIOD:
            case DURATION:
            case STRUCT:
                // IgniteCustomType: JDBC spec allows database dependent type name. See DatabaseMetadata::getColumns (TYPE_NAME column);
                // So include JDBC TYPE_NAME of your type otherwise its type name is going to be OTHER.
                return "OTHER";
            default:
                throw new IgniteInternalException(Common.INTERNAL_ERR, "Unknown column type: " + columnType);
        }
    }

    /**
     * Converts column type to SQL type id.
     *
     * @param columnType Column type.
     * @return SQL type id.
     */
    public static int typeId(ColumnType columnType) {
        switch (columnType) {
            case BOOLEAN: return BOOLEAN;
            case INT8: return TINYINT;
            case INT16: return SMALLINT;
            case INT32: return INTEGER;
            case INT64: return BIGINT;
            case FLOAT: return REAL;
            case DOUBLE: return DOUBLE;
            case STRING: return VARCHAR;
            case BYTE_ARRAY: return VARBINARY;
            case TIME: return TIME;
            case DATETIME: return TIMESTAMP;
            case DATE: return DATE;
            case DECIMAL: return DECIMAL;
            case NULL: return NULL;
            case UUID:
            case PERIOD:
            case DURATION:
            case TIMESTAMP:
            case STRUCT:
                // IgniteCustomType: JDBC spec allows database dependent type name. See DatabaseMetadata::getColumns (TYPE_NAME column);
                // So include JDBC TYPE_NAME of your type otherwise its type name is going to be OTHER.
                return OTHER;
            default:
                throw new IgniteInternalException(Common.INTERNAL_ERR, "Unknown column type: " + columnType);
        }
    }
}
