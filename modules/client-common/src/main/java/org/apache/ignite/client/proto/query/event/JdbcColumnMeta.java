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

package org.apache.ignite.client.proto.query.event;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.Objects;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;

/**
 * JDBC column metadata.
 */
public class JdbcColumnMeta extends JdbcResponse {
    /** Nullable. */
    private boolean nullable;

    /** Cache name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Column name. */
    private String colName;

    /** Data type. */
    private int dataType;

    /** Data type. */
    private String dataTypeName;

    /** Data type class. */
    private String dataTypeClass;

    /**
     * Default constructor is used for serialization.
     */
    public JdbcColumnMeta() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param schemaName Schema.
     * @param tblName Table.
     * @param colName Column.
     * @param cls Type.
     */
    public JdbcColumnMeta(String schemaName, String tblName, String colName, Class<?> cls) {
        this(schemaName, tblName, colName, cls, true);
    }

    /**
     * Constructor with nullable flag.
     *
     * @param schemaName Schema.
     * @param tblName Table.
     * @param colName Column.
     * @param cls Type.
     * @param nullable Nullable flag.
     */
    public JdbcColumnMeta(String schemaName, String tblName, String colName, Class<?> cls, boolean nullable) {
        this(schemaName, tblName, colName, typeName(cls.getName()), cls.getName(), type(cls.getName()), nullable);
    }

    /**
     * Constructor with nullable flag.
     *
     * @param schemaName Schema.
     * @param tblName Table.
     * @param colName Column.
     * @param sqlTypeName Sql type name.
     * @param dataType Jdbc data type index.
     * @param nullable Nullable flag.
     */
    public JdbcColumnMeta(String schemaName, String tblName, String colName, String sqlTypeName, String typeClassName,
        int dataType, boolean nullable) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.colName = colName;
        this.nullable = nullable;
        this.dataType = dataType;
        this.dataTypeName = sqlTypeName;
        this.dataTypeClass = typeClassName;
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
        return colName;
    }

    /**
     * Gets data type id.
     *
     * @return Column's data type.
     */
    public int dataType() {
        return dataType;
    }

    /**
     * Gets data type name.
     *
     * @return Column's data type name.
     */
    public String dataTypeName() {
        return dataTypeName;
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
        return -1;
    }

    /**
     * Gets column scale.
     *
     * @return Column's scale.
     */
    public int scale() {
        return -1;
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
     * @return Data type class.
     */
    public String dataTypeClass() {
        return dataTypeClass;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (status() != STATUS_SUCCESS)
            return;

        packer.packString(schemaName);
        packer.packString(tblName);
        packer.packString(colName);

        packer.packInt(dataType);
        packer.packString(dataTypeName);
        packer.packBoolean(nullable);
        packer.packString(dataTypeClass);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (status() != STATUS_SUCCESS)
            return;

        schemaName = unpacker.unpackString();
        tblName = unpacker.unpackString();
        colName = unpacker.unpackString();

        dataType = unpacker.unpackInt();
        dataTypeName = unpacker.unpackString();
        nullable = unpacker.unpackBoolean();
        dataTypeClass = unpacker.unpackString();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        JdbcColumnMeta meta = (JdbcColumnMeta)o;

        return Objects.equals(schemaName, meta.schemaName)
            && Objects.equals(tblName, meta.tblName)
            && Objects.equals(colName, meta.colName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = schemaName != null ? schemaName.hashCode() : 0;

        result = 31 * result + (tblName != null ? tblName.hashCode() : 0);
        result = 31 * result + colName.hashCode();

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcColumnMeta.class, this);
    }

    /**
     * Converts Java class name to type from {@link Types}.
     *
     * @param cls Java class name.
     * @return Type from {@link Types}.
     */
    private static int type(String cls) {
        if (Boolean.class.getName().equals(cls) || boolean.class.getName().equals(cls))
            return BOOLEAN;
        else if (Byte.class.getName().equals(cls) || byte.class.getName().equals(cls))
            return TINYINT;
        else if (Short.class.getName().equals(cls) || short.class.getName().equals(cls))
            return SMALLINT;
        else if (Integer.class.getName().equals(cls) || int.class.getName().equals(cls))
            return INTEGER;
        else if (Long.class.getName().equals(cls) || long.class.getName().equals(cls))
            return BIGINT;
        else if (Float.class.getName().equals(cls) || float.class.getName().equals(cls))
            return FLOAT;
        else if (Double.class.getName().equals(cls) || double.class.getName().equals(cls))
            return DOUBLE;
        else if (String.class.getName().equals(cls))
            return VARCHAR;
        else if (byte[].class.getName().equals(cls))
            return BINARY;
        else if (Time.class.getName().equals(cls))
            return TIME;
        else if (Timestamp.class.getName().equals(cls))
            return TIMESTAMP;
        else if (Date.class.getName().equals(cls) || java.sql.Date.class.getName().equals(cls))
            return DATE;
        else if (BigDecimal.class.getName().equals(cls))
            return DECIMAL;
        else
            return OTHER;
    }

    /**
     * Converts Java class name to SQL type name.
     *
     * @param cls Java class name.
     * @return SQL type name.
     */
    private static String typeName(String cls) {
        if (Boolean.class.getName().equals(cls) || boolean.class.getName().equals(cls))
            return "BOOLEAN";
        else if (Byte.class.getName().equals(cls) || byte.class.getName().equals(cls))
            return "TINYINT";
        else if (Short.class.getName().equals(cls) || short.class.getName().equals(cls))
            return "SMALLINT";
        else if (Integer.class.getName().equals(cls) || int.class.getName().equals(cls))
            return "INTEGER";
        else if (Long.class.getName().equals(cls) || long.class.getName().equals(cls))
            return "BIGINT";
        else if (Float.class.getName().equals(cls) || float.class.getName().equals(cls))
            return "FLOAT";
        else if (Double.class.getName().equals(cls) || double.class.getName().equals(cls))
            return "DOUBLE";
        else if (String.class.getName().equals(cls))
            return "VARCHAR";
        else if (byte[].class.getName().equals(cls))
            return "BINARY";
        else if (Time.class.getName().equals(cls))
            return "TIME";
        else if (Timestamp.class.getName().equals(cls))
            return "TIMESTAMP";
        else if (Date.class.getName().equals(cls) || java.sql.Date.class.getName().equals(cls))
            return "DATE";
        else if (BigDecimal.class.getName().equals(cls))
            return "DECIMAL";
        else
            return "OTHER";
    }
}
